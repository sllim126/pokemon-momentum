"""Series snapshots: every printing's price history, prepared nightly so charts and the
6-Month Hold read one or two rows instead of scanning the raw price history.

- <slug>_series_snapshot: the last 730 days, one price per day, with 7- and 30-day
  moving averages, as JSON lists. Serves the 30D/90D/1Y chart windows.
- <slug>_long_series_snapshot: everything older than that, one price per week (the
  week's last price, with the moving averages as they stood that day). Added to the
  730-day row for the "All" chart window and the 6-Month Hold's track record.
"""

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
        description="Build compact recent chart-series snapshot rows for a category."
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
    out_csv = f"{EXTRACTED_DIR}/{category.series_snapshot_csv}"
    table_name = category.series_snapshot_table

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
              AND date >= (SELECT latest_date FROM latest_date) - INTERVAL 729 DAY
        ),
        with_ma AS (
            SELECT
                categoryId,
                groupId,
                productId,
                subTypeName,
                date,
                price,
                AVG(price) OVER (
                    PARTITION BY groupId, productId, subTypeName
                    ORDER BY date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS sma7,
                AVG(price) OVER (
                    PARTITION BY groupId, productId, subTypeName
                    ORDER BY date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS sma30
            FROM base
        )
        SELECT
            (SELECT latest_date FROM latest_date) AS latest_date,
            categoryId,
            groupId,
            productId,
            subTypeName,
            TO_JSON(LIST(CAST(date AS VARCHAR) ORDER BY date)) AS dates_json,
            TO_JSON(LIST(price ORDER BY date)) AS prices_json,
            TO_JSON(LIST(sma7 ORDER BY date)) AS sma7_json,
            TO_JSON(LIST(sma30 ORDER BY date)) AS sma30_json,
            COUNT(*) AS point_count
        FROM with_ma
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

    # Older history, one point per week. The moving averages are worked out on the daily
    # prices first, then sampled, so they mean the same thing as in the 730-day table.
    long_table = category.long_series_snapshot_table
    long_csv = f"{EXTRACTED_DIR}/{category.long_series_snapshot_csv}"
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {long_table} AS
        WITH latest_date AS (
            SELECT MAX(date) AS latest_date
            FROM pokemon_prices
            WHERE categoryId = {category.category_id}
              AND marketPrice IS NOT NULL
        ),
        daily AS (
            SELECT
                categoryId,
                groupId,
                productId,
                subTypeName,
                date,
                marketPrice AS price,
                AVG(marketPrice) OVER (
                    PARTITION BY groupId, productId, subTypeName
                    ORDER BY date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS sma7,
                AVG(marketPrice) OVER (
                    PARTITION BY groupId, productId, subTypeName
                    ORDER BY date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS sma30
            FROM pokemon_prices
            WHERE categoryId = {category.category_id}
              AND marketPrice IS NOT NULL
        ),
        weekly AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY groupId, productId, subTypeName, DATE_TRUNC('week', date)
                    ORDER BY date DESC
                ) AS week_rank
            FROM daily
            WHERE date < (SELECT latest_date FROM latest_date) - INTERVAL 729 DAY
        )
        SELECT
            (SELECT latest_date FROM latest_date) AS latest_date,
            categoryId,
            groupId,
            productId,
            subTypeName,
            TO_JSON(LIST(CAST(date AS VARCHAR) ORDER BY date)) AS dates_json,
            TO_JSON(LIST(price ORDER BY date)) AS prices_json,
            TO_JSON(LIST(sma7 ORDER BY date)) AS sma7_json,
            TO_JSON(LIST(sma30 ORDER BY date)) AS sma30_json,
            COUNT(*) AS point_count
        FROM weekly
        WHERE week_rank = 1
        GROUP BY categoryId, groupId, productId, subTypeName
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM {long_table}
            ORDER BY groupId, productId, subTypeName
        ) TO '{long_csv}' WITH (HEADER, DELIMITER ',')
        """
    )
    long_rows = con.execute(f"SELECT COUNT(*) FROM {long_table}").fetchone()[0]

    rows = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    latest = con.execute(f"SELECT MAX(latest_date) FROM {table_name}").fetchone()[0]
    con.close()

    print(f"Category: {category.label} ({category.category_id})")
    print("Wrote:", out_csv)
    print("DuckDB table:", table_name)
    print("Database:", DB_PATH)
    print("Latest date:", latest)
    print("Rows:", rows)
    print("Long-history table:", long_table, "rows:", long_rows, "->", long_csv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
