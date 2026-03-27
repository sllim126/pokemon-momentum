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
        description="Build a lightweight health snapshot for a category."
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
    out_csv = f"{EXTRACTED_DIR}/{category.health_snapshot_csv}"
    table_name = category.health_snapshot_table

    con = duckdb.connect(DB_PATH)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
            {category.category_id} AS category_id,
            '{category.label}' AS category,
            COUNT(*) AS rows,
            MAX(date) AS latest
        FROM pokemon_prices
        WHERE categoryId = {category.category_id}
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM {table_name}
        ) TO '{out_csv}' WITH (HEADER, DELIMITER ',')
        """
    )

    row = con.execute(f"SELECT rows, latest FROM {table_name}").fetchone()
    con.close()

    print(f"Category: {category.label} ({category.category_id})")
    print("Wrote:", out_csv)
    print("DuckDB table:", table_name)
    print("Database:", DB_PATH)
    print("Rows:", row[0] if row else 0)
    print("Latest:", row[1] if row else None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
