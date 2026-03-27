import argparse
import csv
import sys
import time
from pathlib import Path

import duckdb
import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common.category_config import get_category_config

DATA_DIR = "/app/data/extracted"
PROCESSED_DIR = Path("/app/data/processed")
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"
REQUEST_TIMEOUT = (10, 45)
MAX_RETRIES = 3
HEADERS = ["groupId", "productId", "name", "cleanName", "imageUrl", "rarity", "number"]


def parse_args() -> argparse.Namespace:
    """Parse metadata export options for either incremental or full refresh mode."""
    parser = argparse.ArgumentParser(
        description="Export Pokemon products metadata for groups present in price history."
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Refetch every group instead of only groups missing from existing metadata.",
    )
    parser.add_argument(
        "--category-id",
        type=int,
        default=3,
        help="TCGplayer categoryId whose product metadata should be exported.",
    )
    return parser.parse_args()


def get_unique_group_ids(category_id: int) -> list[int]:
    """Return only groups that actually appear in historical prices for this category."""
    con = duckdb.connect(str(DB_PATH))
    rows = con.execute(
        """
        SELECT DISTINCT groupId
        FROM pokemon_prices
        WHERE groupId IS NOT NULL
          AND categoryId = ?
        ORDER BY groupId
        """,
        [category_id],
    ).fetchall()
    con.close()
    return [row[0] for row in rows]


def get_expected_products_by_group(category_id: int) -> dict[int, set[int]]:
    """Capture every product seen in price history so missing metadata can be backfilled."""
    con = duckdb.connect(str(DB_PATH))
    rows = con.execute(
        """
        SELECT DISTINCT groupId, productId
        FROM pokemon_prices
        WHERE groupId IS NOT NULL
          AND categoryId = ?
          AND productId IS NOT NULL
        ORDER BY groupId, productId
        """,
        [category_id],
    ).fetchall()
    con.close()

    expected: dict[int, set[int]] = {}
    for group_id, product_id in rows:
        expected.setdefault(group_id, set()).add(product_id)
    return expected


def load_existing_rows(out_csv: str, active_group_ids: set[int]) -> dict[tuple[int, int], tuple]:
    """Reuse existing metadata rows so incremental refreshes only fetch missing groups."""
    existing: dict[tuple[int, int], tuple] = {}
    path = Path(out_csv)
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
    """Return the set of groups already covered by the current metadata cache."""
    return {group_id for group_id, _product_id in existing_rows}


def groups_with_placeholder_rows(existing_rows: dict[tuple[int, int], tuple]) -> set[int]:
    """Return groups whose cached metadata still includes placeholder fallback names.

    Incremental refresh should not treat these groups as complete, because the
    placeholder row means a previous run could not resolve one or more products.
    Re-fetching the full group is the simplest way to replace stale
    `Product <id>` placeholders once tcgcsv starts returning the missing cards.
    """
    groups: set[int] = set()
    for (group_id, product_id), row in existing_rows.items():
        name = str(row[2] or "")
        clean_name = str(row[3] or "")
        if name == f"Product {product_id}" or clean_name == f"Product {product_id}":
            groups.add(group_id)
    return groups


def fetch_products_for_group(category_id: int, group_id: int) -> list[dict]:
    """Fetch one group's product catalog from tcgcsv with retries and timeouts."""
    url = f"https://tcgcsv.com/tcgplayer/{category_id}/{group_id}/products"
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
    """Flatten tcgcsv product JSON into the compact metadata shape used downstream."""
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


def write_outputs(rows: list[tuple], out_csv: str, table_name: str) -> None:
    """Persist the final metadata snapshot to both CSV and DuckDB."""
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        writer.writerows(rows)

    con = duckdb.connect(str(DB_PATH))
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
    con.execute(f"DELETE FROM {table_name}")
    if rows:
        con.executemany(
            f"""
            INSERT INTO {table_name}
            (groupId, productId, name, cleanName, imageUrl, rarity, number)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    con.close()


def main() -> int:
    """Refresh product metadata for active groups and guarantee every priced product has a row."""
    args = parse_args()
    category = get_category_config(args.category_id)
    out_csv = f"{DATA_DIR}/{category.products_csv}"
    table_name = category.products_table
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    group_ids = get_unique_group_ids(category.category_id)
    active_group_ids = set(group_ids)
    expected_products = get_expected_products_by_group(category.category_id)
    existing_rows = {} if args.full_refresh else load_existing_rows(out_csv, active_group_ids)

    # Expected result of the planning stage: we know which groups already have cached
    # metadata and which ones still need to be fetched from tcgcsv.
    placeholder_groups = groups_with_placeholder_rows(existing_rows)
    groups_to_fetch = group_ids if args.full_refresh else [
        gid for gid in group_ids
        if gid not in existing_group_ids(existing_rows) or gid in placeholder_groups
    ]

    print("Unique groups in your price history:", len(group_ids))
    print("Existing metadata groups:", len(existing_group_ids(existing_rows)))
    print("Groups with placeholder rows:", len(placeholder_groups))
    print("Groups queued for fetch:", len(groups_to_fetch))
    print("Mode:", "full-refresh" if args.full_refresh else "incremental")

    rows_by_key = dict(existing_rows)
    placeholder_rows = 0

    # Each fetched group should replace stale rows for that group and contribute a clean
    # metadata snapshot for downstream joins, dashboards, and signal builders.
    for i, gid in enumerate(groups_to_fetch, start=1):
        print(f"[{i}/{len(groups_to_fetch)}] Downloading products for groupId {gid} ...")
        try:
            products = fetch_products_for_group(category.category_id, gid)
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

    # Placeholder rows ensure later joins still resolve every priced product even when
    # tcgcsv is missing a metadata record for a specific productId.
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

    # Final expected result: one complete per-category product catalog in CSV and DuckDB.
    rows = sorted(rows_by_key.values(), key=lambda row: (row[0], row[1]))
    write_outputs(rows, out_csv, table_name)

    print("Category:", category.label, f"({category.category_id})")
    print("Wrote:", out_csv)
    print("DuckDB table:", table_name)
    print("Database:", DB_PATH)
    print("Placeholder metadata rows:", placeholder_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
