import csv
import requests
import duckdb
from pathlib import Path

DATA_DIR = "/app/data/extracted"
OUT_CSV = f"{DATA_DIR}/pokemon_products.csv"
CATEGORY_ID = 3
PROCESSED_DIR = Path("/app/data/processed")
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"
TABLE_NAME = "pokemon_products"


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


def fetch_products_for_group(group_id: int) -> list[dict]:
    url = f"https://tcgcsv.com/tcgplayer/{CATEGORY_ID}/{group_id}/products"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json().get("results", [])

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
group_ids = get_unique_group_ids()
expected_products = get_expected_products_by_group()
print("Unique groups in your price history:", len(group_ids))

rows_to_insert = []
placeholder_rows = 0

with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["groupId", "productId", "name", "cleanName", "imageUrl", "rarity", "number"])

    for i, gid in enumerate(group_ids, start=1):
        print(f"[{i}/{len(group_ids)}] Downloading products for groupId {gid} ...")
        products = fetch_products_for_group(gid)
        seen_product_ids: set[int] = set()

        for p in products:
            product_id = p.get("productId")
            if product_id is not None:
                seen_product_ids.add(product_id)

            # extendedData often includes Number and Rarity but format varies by product
            rarity = ""
            number = ""
            for ed in p.get("extendedData", []) or []:
                n = (ed.get("name") or "").lower()
                if n == "rarity":
                    rarity = ed.get("value", "") or ""
                if n in ["number", "collector number", "collectors number", "collector_number"]:
                    number = ed.get("value", "") or ""

            w.writerow([
                gid,
                p.get("productId"),
                p.get("name"),
                p.get("cleanName"),
                p.get("imageUrl"),
                rarity,
                number
            ])
            rows_to_insert.append(
                (
                    gid,
                    product_id,
                    p.get("name"),
                    p.get("cleanName"),
                    p.get("imageUrl"),
                    rarity,
                    number,
                )
            )

        missing_product_ids = sorted(expected_products.get(gid, set()) - seen_product_ids)
        for product_id in missing_product_ids:
            placeholder_name = f"Product {product_id}"
            w.writerow([
                gid,
                product_id,
                placeholder_name,
                placeholder_name,
                "",
                "",
                "",
            ])
            rows_to_insert.append(
                (
                    gid,
                    product_id,
                    placeholder_name,
                    placeholder_name,
                    "",
                    "",
                    "",
                )
            )
            placeholder_rows += 1

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
    rows_to_insert,
)
con.close()

print("Wrote:", OUT_CSV)
print("DuckDB table:", TABLE_NAME)
print("Database:", DB_PATH)
print("Placeholder metadata rows:", placeholder_rows)
