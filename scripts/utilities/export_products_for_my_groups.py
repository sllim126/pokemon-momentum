import argparse
import csv
import time
from pathlib import Path

import duckdb
import requests

DATA_DIR = "/app/data/extracted"
OUT_CSV = f"{DATA_DIR}/pokemon_products.csv"
CATEGORY_ID = 3
PROCESSED_DIR = Path("/app/data/processed")
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"
TABLE_NAME = "pokemon_products"
REQUEST_TIMEOUT = (10, 45)
MAX_RETRIES = 3
HEADERS = ["groupId", "productId", "name", "cleanName", "imageUrl", "rarity", "number"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Pokemon products metadata for groups present in price history."
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Refetch every group instead of only groups missing from existing metadata.",
    )
    return parser.parse_args()


def get_unique_group_ids() -> list[int]:
    con = duckdb.connect(str(DB_PATH))
    rows = con.execute(
        """
        SELECT DISTINCT groupId
        FROM pokemon_prices
        WHERE groupId IS NOT NULL
        ORDER BY groupId
        """
    ).fetchall()
    con.close()
    return [row[0] for row in rows]


def get_expected_products_by_group() -> dict[int, set[int]]:
    con = duckdb.connect(str(DB_PATH))
    rows = con.execute(
        """
        SELECT DISTINCT groupId, productId
        FROM pokemon_prices
        WHERE groupId IS NOT NULL
          AND productId IS NOT NULL
        ORDER BY groupId, productId
        """
    ).fetchall()
    con.close()

    expected: dict[int, set[int]] = {}
    for group_id, product_id in rows:
        expected.setdefault(group_id, set()).add(product_id)
    return expected


def load_existing_rows(active_group_ids: set[int]) -> dict[tuple[int, int], tuple]:
    existing: dict[tuple[int, int], tuple] = {}
    path = Path(OUT_CSV)
    if not path.exists():
        return existing

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                group_id = int(row["groupId"])
                product_id = int(row["productId"])
            except Exception:
                continue
            if group_id not in active_group_ids:
                continue
            existing[(group_id, product_id)] = (
                group_id,
                product_id,
                row.get("name", "") or "",
                row.get("cleanName", "") or "",
                row.get("imageUrl", "") or "",
                row.get("rarity", "") or "",
                row.get("number", "") or "",
            )
    return existing


def existing_group_ids(existing_rows: dict[tuple[int, int], tuple]) -> set[int]:
    return {group_id for group_id, _product_id in existing_rows}


def fetch_products_for_group(group_id: int) -> list[dict]:
    url = f"https://tcgcsv.com/tcgplayer/{CATEGORY_ID}/{group_id}/products"
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json().get("results", [])
        except Exception as exc:
            last_error = exc
            print(f"  attempt {attempt}/{MAX_RETRIES} failed for groupId {group_id}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(min(5 * attempt, 15))
    raise RuntimeError(f"Failed to fetch products for groupId {group_id}") from last_error


def normalize_product_row(group_id: int, product: dict) -> tuple:
    rarity = ""
    number = ""
    for ed in product.get("extendedData", []) or []:
        name = (ed.get("name") or "").lower()
        if name == "rarity":
            rarity = ed.get("value", "") or ""
        if name in ["number", "collector number", "collectors number", "collector_number"]:
            number = ed.get("value", "") or ""

    product_id = product.get("productId")
    return (
        group_id,
        product_id,
        product.get("name"),
        product.get("cleanName"),
        product.get("imageUrl"),
        rarity,
        number,
    )


def write_outputs(rows: list[tuple]) -> None:
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        writer.writerows(rows)

    con = duckdb.connect(str(DB_PATH))
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            groupId BIGINT,
            productId BIGINT,
            name VARCHAR,
            cleanName VARCHAR,
            imageUrl VARCHAR,
            rarity VARCHAR,
            number VARCHAR
        )
        """
    )
    con.execute(f"DELETE FROM {TABLE_NAME}")
    con.executemany(
        f"""
        INSERT INTO {TABLE_NAME}
        (groupId, productId, name, cleanName, imageUrl, rarity, number)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    con.close()


def main() -> int:
    args = parse_args()
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    group_ids = get_unique_group_ids()
    active_group_ids = set(group_ids)
    expected_products = get_expected_products_by_group()
    existing_rows = {} if args.full_refresh else load_existing_rows(active_group_ids)

    groups_to_fetch = group_ids if args.full_refresh else [
        gid for gid in group_ids if gid not in existing_group_ids(existing_rows)
    ]

    print("Unique groups in your price history:", len(group_ids))
    print("Existing metadata groups:", len(existing_group_ids(existing_rows)))
    print("Groups queued for fetch:", len(groups_to_fetch))
    print("Mode:", "full-refresh" if args.full_refresh else "incremental")

    rows_by_key = dict(existing_rows)
    placeholder_rows = 0

    for i, gid in enumerate(groups_to_fetch, start=1):
        print(f"[{i}/{len(groups_to_fetch)}] Downloading products for groupId {gid} ...")
        try:
            products = fetch_products_for_group(gid)
        except Exception as exc:
            print(f"  skipping groupId {gid} after repeated failures: {exc}")
            products = []

        for key in [key for key in rows_by_key if key[0] == gid]:
            del rows_by_key[key]

        seen_product_ids: set[int] = set()
        for product in products:
            product_id = product.get("productId")
            if product_id is None:
                continue
            seen_product_ids.add(product_id)
            rows_by_key[(gid, product_id)] = normalize_product_row(gid, product)

    for gid in group_ids:
        expected_ids = expected_products.get(gid, set())
        present_ids = {product_id for group_id, product_id in rows_by_key if group_id == gid}
        missing_ids = sorted(expected_ids - present_ids)
        for product_id in missing_ids:
            placeholder_name = f"Product {product_id}"
            rows_by_key[(gid, product_id)] = (
                gid,
                product_id,
                placeholder_name,
                placeholder_name,
                "",
                "",
                "",
            )
            placeholder_rows += 1

    rows = sorted(rows_by_key.values(), key=lambda row: (row[0], row[1]))
    write_outputs(rows)

    print("Wrote:", OUT_CSV)
    print("DuckDB table:", TABLE_NAME)
    print("Database:", DB_PATH)
    print("Placeholder metadata rows:", placeholder_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
