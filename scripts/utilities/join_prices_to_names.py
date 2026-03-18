import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common.category_config import get_category_config


PROCESSED_DIR = Path("/app/data/processed")
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"


def parse_args() -> argparse.Namespace:
    """Parse which category's named price export should be rebuilt."""
    parser = argparse.ArgumentParser(
        description="Join price history to product and group names for a specific category."
    )
    parser.add_argument(
        "--category-id",
        type=int,
        default=3,
        help="Category ID to export. Default: 3 (Pokemon).",
    )
    return parser.parse_args()


def main() -> int:
    """Materialize a human-readable price history table for exports and spot checks."""
    args = parse_args()
    category = get_category_config(args.category_id)
    out_csv = f"/app/data/extracted/{category.prices_named_csv}"
    table_name = category.prices_named_table

    # Expected input state:
    # - pokemon_prices contains historical rows for this category
    # - category-specific group and product metadata tables already exist
    #
    # Expected result:
    # - a joined table/CSV where raw IDs are accompanied by set and product names,
    #   making downstream inspection and ad hoc exports much easier to read.
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT
                p.date,
                p.categoryId,
                p.groupId,
                p.productId,
                p.subTypeName,
                p.lowPrice,
                p.midPrice,
                p.highPrice,
                p.marketPrice,
                p.directLowPrice,
                g.name AS groupName,
                pr.name AS productName
            FROM pokemon_prices p
            LEFT JOIN {category.groups_table} g USING (groupId)
            LEFT JOIN {category.products_table} pr USING (groupId, productId)
            WHERE p.categoryId = {category.category_id}
            """
        )
        # Persist the joined export so users can inspect named history outside DuckDB.
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM {table_name}
                ORDER BY date, groupId, productId, subTypeName
            ) TO '{out_csv}' WITH (HEADER, DELIMITER ',')
            """
        )
    finally:
        con.close()

    print(f"Category: {category.label} ({category.category_id})")
    print("Wrote:", out_csv)
    print("DuckDB table:", table_name)
    print("Database:", DB_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
