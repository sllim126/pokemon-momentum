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
    """Parse the category whose set-level breadth snapshot should be rebuilt."""
    parser = argparse.ArgumentParser(
        description="Build the per-group momentum signal snapshot for a category."
    )
    parser.add_argument(
        "--category-id",
        type=int,
        default=3,
        help="Category ID to snapshot. Default: 3 (Pokemon).",
    )
    return parser.parse_args()


def main() -> int:
    """Roll the latest product snapshot up into one row per set/group."""
    args = parse_args()
    category = get_category_config(args.category_id)
    out_csv = f"{EXTRACTED_DIR}/{category.group_signal_csv}"
    table_name = category.group_signal_table
    product_signal_table = category.product_signal_table

    con = duckdb.connect(DB_PATH)

    # This snapshot is intentionally downstream of the product snapshot: it summarizes
    # the latest product-level signals into a set-level breadth and momentum view.
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    if product_signal_table not in tables:
        raise RuntimeError(
            f"{product_signal_table} does not exist. Run scripts/indicators/build_product_signal_snapshot.py first."
        )

    con.execute(
        f"""
    CREATE OR REPLACE TABLE {table_name} AS
    WITH latest_snapshot AS (
        -- Use only the most recent product snapshot rows for this category.
        SELECT *
        FROM {product_signal_table}
        WHERE latest_date = (SELECT MAX(latest_date) FROM {product_signal_table})
          AND categoryId = {category.category_id}
    ),
    grouped AS (
        -- Aggregate product-level strength into set-level participation, trend, and
        -- card-vs-sealed comparisons.
        SELECT
            latest_date,
            groupId,
            groupName,
            COUNT(*) AS item_count,
            COUNT(*) FILTER (WHERE productKind = 'card') AS card_count,
            COUNT(*) FILTER (WHERE productKind = 'sealed') AS sealed_count,
            AVG(latest_price) AS avg_latest_price,
            MEDIAN(latest_price) AS median_latest_price,
            AVG(roc_30d_pct) AS avg_30d_pct,
            MEDIAN(roc_30d_pct) AS median_30d_pct,
            AVG(roc_90d_pct) AS avg_90d_pct,
            MEDIAN(roc_90d_pct) AS median_90d_pct,
            AVG(CASE WHEN latest_price > sma_30 AND sma_30 IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 AS pct_above_sma30,
            AVG(CASE WHEN latest_price > sma_90 AND sma_90 IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 AS pct_above_sma90,
            AVG(CASE WHEN breakout_90d_flag = 1 THEN 1.0 ELSE 0.0 END) * 100 AS pct_at_90d_high,
            AVG(price_vs_sma30_pct) AS avg_price_vs_sma30_pct,
            AVG(price_vs_sma90_pct) AS avg_price_vs_sma90_pct,
            AVG(acceleration_7d_vs_30d) AS avg_acceleration_7d_vs_30d,
            AVG(trend_score) AS avg_trend_score,
            AVG(CASE WHEN productKind = 'card' THEN roc_30d_pct END) AS card_avg_30d_pct,
            AVG(CASE WHEN productKind = 'sealed' THEN roc_30d_pct END) AS sealed_avg_30d_pct,
            AVG(CASE WHEN productKind = 'card' THEN roc_90d_pct END) AS card_avg_90d_pct,
            AVG(CASE WHEN productKind = 'sealed' THEN roc_90d_pct END) AS sealed_avg_90d_pct
        FROM latest_snapshot
        GROUP BY latest_date, groupId, groupName
    )
    SELECT
        -- Final result: one set-level summary row suitable for rankings and dashboard tabs.
        latest_date,
        groupId,
        groupName,
        item_count,
        card_count,
        sealed_count,
        avg_latest_price,
        median_latest_price,
        avg_30d_pct,
        median_30d_pct,
        avg_90d_pct,
        median_90d_pct,
        pct_above_sma30,
        pct_above_sma90,
        pct_at_90d_high,
        avg_price_vs_sma30_pct,
        avg_price_vs_sma90_pct,
        avg_acceleration_7d_vs_30d,
        avg_trend_score,
        card_avg_30d_pct,
        sealed_avg_30d_pct,
        CASE
            WHEN card_avg_30d_pct IS NULL OR sealed_avg_30d_pct IS NULL THEN NULL
            ELSE sealed_avg_30d_pct - card_avg_30d_pct
        END AS sealed_vs_cards_30d_divergence,
        card_avg_90d_pct,
        sealed_avg_90d_pct,
        CASE
            WHEN card_avg_90d_pct IS NULL OR sealed_avg_90d_pct IS NULL THEN NULL
            ELSE sealed_avg_90d_pct - card_avg_90d_pct
        END AS sealed_vs_cards_90d_divergence,
        (
            0.35 * COALESCE(LEAST(GREATEST(avg_30d_pct, -100), 200), 0) +
            0.2 * COALESCE(LEAST(GREATEST(avg_90d_pct, -100), 200), 0) +
            0.25 * COALESCE(pct_above_sma30, 0) +
            0.2 * COALESCE(pct_at_90d_high, 0)
        ) AS breadth_score
    FROM grouped
    ORDER BY breadth_score DESC, avg_30d_pct DESC, pct_above_sma30 DESC, groupName
    """
    )

    # Export the ranked set snapshot to CSV so it can be inspected without querying DuckDB.
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM {table_name}
            ORDER BY breadth_score DESC, avg_30d_pct DESC, pct_above_sma30 DESC, groupName
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
