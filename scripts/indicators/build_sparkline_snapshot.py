import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common.category_config import get_category_config


EXTRACTED_DIR = "/app/data/extracted"
PROCESSED_DIR = "/app/data/processed"
DB_PATH = f"{PROCESSED_DIR}/prices_db.duckdb"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build compact per-product sparkline snapshot rows for a category."
    )
    parser.add_argument(
        "--category-id",
        type=int,
        default=3,
        help="Category ID to snapshot. Default: 3 (Pokemon).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    category = get_category_config(args.category_id)
    out_csv = f"{EXTRACTED_DIR}/{category.sparkline_snapshot_csv}"
    table_name = category.sparkline_snapshot_table

    con = duckdb.connect(DB_PATH)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        WITH latest_date AS (
            SELECT MAX(date) AS latest_date
            FROM pokemon_prices
            WHERE categoryId = {category.category_id}
              AND marketPrice IS NOT NULL
        ),
        base AS (
            SELECT
                categoryId,
                groupId,
                productId,
                subTypeName,
                date,
                marketPrice AS price
            FROM pokemon_prices
            WHERE categoryId = {category.category_id}
              AND marketPrice IS NOT NULL
              AND date >= (SELECT latest_date FROM latest_date) - INTERVAL 364 DAY
        )
        SELECT
            (SELECT latest_date FROM latest_date) AS latest_date,
            categoryId,
            groupId,
            productId,
            subTypeName,
            TO_JSON(LIST(price ORDER BY date)) AS prices_json,
            COUNT(*) AS point_count
        FROM base
        GROUP BY categoryId, groupId, productId, subTypeName
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM {table_name}
            ORDER BY groupId, productId, subTypeName
        ) TO '{out_csv}' WITH (HEADER, DELIMITER ',')
        """
    )

    rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    latest = con.execute(f"SELECT MAX(latest_date) FROM {table_name}").fetchone()[0]
    con.close()

    print(f"Category: {category.label} ({category.category_id})")
    print("Wrote:", out_csv)
    print("DuckDB table:", table_name)
    print("Database:", DB_PATH)
    print("Latest date:", latest)
    print("Rows:", rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
