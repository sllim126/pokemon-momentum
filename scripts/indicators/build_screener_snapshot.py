import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common.category_config import get_category_config
from scripts.dashboards.query_support import build_premium_rarity_filter


EXTRACTED_DIR = "/app/data/extracted"
PROCESSED_DIR = "/app/data/processed"
DB_PATH = f"{PROCESSED_DIR}/prices_db.duckdb"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the per-product screener helper snapshot for a category."
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
    out_csv = f"{EXTRACTED_DIR}/{category.screener_snapshot_csv}"
    table_name = category.screener_snapshot_table
    product_signal_table = category.product_signal_table
    premium_rarity_filter = build_premium_rarity_filter("s.rarity")

    con = duckdb.connect(DB_PATH)
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    if product_signal_table not in tables:
        raise RuntimeError(
            f"{product_signal_table} does not exist. Run scripts/indicators/build_product_signal_snapshot.py first."
        )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        WITH latest_snapshot AS (
            SELECT *
            FROM {product_signal_table}
            WHERE categoryId = {category.category_id}
              AND latest_date = (
                SELECT MAX(latest_date)
                FROM {product_signal_table}
                WHERE categoryId = {category.category_id}
              )
        ),
        recent_prices AS (
            SELECT
                productId,
                groupId,
                subTypeName,
                date,
                marketPrice,
                ROW_NUMBER() OVER (
                    PARTITION BY productId, groupId, subTypeName
                    ORDER BY date DESC
                ) AS rn
            FROM pokemon_prices
            WHERE categoryId = {category.category_id}
              AND marketPrice IS NOT NULL
              AND date >= (
                SELECT MAX(latest_date)
                FROM latest_snapshot
              ) - INTERVAL 10 DAY
        ),
        recent_lift AS (
            SELECT
                productId,
                groupId,
                subTypeName,
                MAX(CASE WHEN rn = 1 THEN marketPrice END) AS latest_price_1d,
                MAX(CASE WHEN rn = 2 THEN marketPrice END) AS latest_price_2d,
                MAX(CASE WHEN rn = 3 THEN marketPrice END) AS latest_price_3d,
                COUNT(*) FILTER (WHERE rn <= 3) AS recent_price_points
            FROM recent_prices
            WHERE rn <= 3
            GROUP BY productId, groupId, subTypeName
        ),
        floor_window AS (
            SELECT
                rp.productId,
                rp.groupId,
                rp.subTypeName,
                COUNT(*) AS floor_observations_7d,
                MIN(rp.marketPrice) AS floor_low_7d,
                MAX(rp.marketPrice) AS floor_high_7d,
                AVG(rp.marketPrice) AS floor_avg_7d
            FROM recent_prices rp
            JOIN latest_snapshot s
              ON s.productId = rp.productId
             AND s.groupId = rp.groupId
             AND s.subTypeName = rp.subTypeName
            WHERE rp.date >= s.latest_date - INTERVAL 7 DAY
            GROUP BY rp.productId, rp.groupId, rp.subTypeName
        ),
        enriched AS (
            SELECT
                s.*,
                rl.latest_price_1d,
                rl.latest_price_2d,
                rl.latest_price_3d,
                COALESCE(rl.recent_price_points, 0) AS recent_price_points,
                fw.floor_observations_7d,
                fw.floor_low_7d,
                fw.floor_high_7d,
                fw.floor_avg_7d,
                CASE
                    WHEN s.latest_price IS NULL OR s.latest_price = 0
                      OR fw.floor_high_7d IS NULL OR fw.floor_low_7d IS NULL
                    THEN NULL
                    ELSE GREATEST(
                        ABS(((fw.floor_high_7d / NULLIF(s.latest_price, 0)) - 1) * 100.0),
                        ABS(((fw.floor_low_7d / NULLIF(s.latest_price, 0)) - 1) * 100.0)
                    )
                END AS floor_variance_to_current_pct_7d
            FROM latest_snapshot s
            LEFT JOIN recent_lift rl
              ON rl.productId = s.productId
             AND rl.groupId = s.groupId
             AND rl.subTypeName = s.subTypeName
            LEFT JOIN floor_window fw
              ON fw.productId = s.productId
             AND fw.groupId = s.groupId
             AND fw.subTypeName = s.subTypeName
        ),
        good_buys_ranked AS (
            SELECT
                s.productId,
                s.groupId,
                s.subTypeName,
                ROW_NUMBER() OVER (
                    ORDER BY
                        CASE
                            WHEN s.floor_observations_7d >= 4
                             AND COALESCE(s.floor_variance_to_current_pct_7d, 999999.0) <= 12.0
                            THEN 0 ELSE 1
                        END ASC,
                        COALESCE(s.floor_variance_to_current_pct_7d, 999999.0) ASC,
                        s.roc_30d_pct ASC,
                        s.price_vs_sma30_pct ASC,
                        s.latest_price DESC
                ) AS good_buys_default_rank
            FROM enriched s
            WHERE s.latest_price >= 5.0
              AND s.productKind = 'card'
              AND {premium_rarity_filter}
              AND COALESCE(s.recent_distinct_prices_30d, 0) >= 10
              AND (
                (
                  COALESCE(s.roc_30d_pct, 0) <= 0.0
                  AND COALESCE(s.roc_90d_pct, 0) <= 20.0
                  AND COALESCE(s.roc_90d_pct, 0) < 0
                  AND COALESCE(s.roc_7d_pct, 0) <= 6.0
                  AND (
                    (
                      s.recent_price_points >= 3
                      AND s.latest_price_1d < s.latest_price_2d
                      AND s.latest_price_2d < s.latest_price_3d
                    )
                    OR (
                      s.floor_observations_7d >= 4
                      AND COALESCE(s.floor_variance_to_current_pct_7d, 999999.0) <= 12.0
                    )
                  )
                )
                OR (
                  s.latest_price >= 80.0
                  AND s.floor_observations_7d >= 4
                  AND COALESCE(s.floor_variance_to_current_pct_7d, 999999.0) <= 12.0
                  AND COALESCE(s.roc_30d_pct, 0) <= 12.0
                  AND COALESCE(s.roc_90d_pct, 0) <= 35.0
                  AND COALESCE(s.roc_7d_pct, 0) <= 2.5
                  AND COALESCE(s.price_vs_sma30_pct, 0) >= -12.0
                  AND COALESCE(s.price_vs_sma30_pct, 0) <= 4.0
                  AND (
                    COALESCE(s.roc_7d_pct, 0) < 0
                    OR COALESCE(s.price_vs_sma30_pct, 0) <= 0
                  )
                )
              )
        ),
        early_uptrends_ranked AS (
            SELECT
                s.productId,
                s.groupId,
                s.subTypeName,
                ROW_NUMBER() OVER (
                    ORDER BY
                        s.acceleration_7d_vs_30d DESC,
                        s.price_vs_sma30_pct ASC,
                        s.roc_30d_pct ASC,
                        s.latest_price DESC
                ) AS early_uptrends_default_rank
            FROM enriched s
            WHERE s.latest_price >= 5.0
              AND s.early_streak >= 3
              AND s.latest_sma30 IS NOT NULL
              AND COALESCE(s.recent_observations_7d, 0) >= 3
              AND COALESCE(s.recent_distinct_prices_7d, 0) >= 2
              AND COALESCE(s.recent_distinct_prices_30d, 0) >= 10
              AND s.last_change_date IS NOT NULL
              AND s.last_change_date >= s.latest_date - INTERVAL 5 DAY
              AND COALESCE(s.roc_7d_pct, 0) >= 1.0
              AND COALESCE(s.roc_7d_pct, 0) <= 10.0
              AND COALESCE(s.roc_30d_pct, 0) <= 12.0
              AND COALESCE(s.roc_90d_pct, 0) <= 25.0
              AND COALESCE(s.acceleration_7d_vs_30d, 0) >= 0.5
              AND s.recent_price_points >= 3
              AND s.latest_price_1d > s.latest_price_2d
              AND s.latest_price_2d > s.latest_price_3d
              AND COALESCE(s.price_vs_sma30_pct, 0) <= 8.0
        )
        SELECT
            e.*,
            CASE WHEN gb.good_buys_default_rank IS NULL THEN 0 ELSE 1 END AS good_buys_default_flag,
            gb.good_buys_default_rank,
            CASE WHEN eu.early_uptrends_default_rank IS NULL THEN 0 ELSE 1 END AS early_uptrends_default_flag,
            eu.early_uptrends_default_rank
        FROM enriched e
        LEFT JOIN good_buys_ranked gb
          ON gb.productId = e.productId
         AND gb.groupId = e.groupId
         AND gb.subTypeName = e.subTypeName
        LEFT JOIN early_uptrends_ranked eu
          ON eu.productId = e.productId
         AND eu.groupId = e.groupId
         AND eu.subTypeName = e.subTypeName
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM {table_name}
            ORDER BY COALESCE(good_buys_default_rank, 999999), COALESCE(early_uptrends_default_rank, 999999), trend_score DESC
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
