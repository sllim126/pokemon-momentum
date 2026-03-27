import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common.category_config import get_category_config
from scripts.common.product_classification import get_product_class_sql, get_product_kind_sql


EXTRACTED_DIR = "/app/data/extracted"
PROCESSED_DIR = "/app/data/processed"
DB_PATH = f"{PROCESSED_DIR}/prices_db.duckdb"
PRODUCT_CLASS_SQL = get_product_class_sql("p")
PRODUCT_KIND_SQL = get_product_kind_sql("p")


def parse_args() -> argparse.Namespace:
    """Parse the category to snapshot into the per-product signal table."""
    parser = argparse.ArgumentParser(
        description="Build the per-product momentum signal snapshot for a category."
    )
    parser.add_argument(
        "--category-id",
        type=int,
        default=3,
        help="Category ID to snapshot. Default: 3 (Pokemon).",
    )
    return parser.parse_args()


def main() -> int:
    """Build the latest per-product momentum snapshot used by API screens and dashboards."""
    args = parse_args()
    category = get_category_config(args.category_id)
    out_csv = f"{EXTRACTED_DIR}/{category.product_signal_csv}"
    table_name = category.product_signal_table

    con = duckdb.connect(DB_PATH)

    # Expected input state:
    # - pokemon_prices contains historical fact rows for this category
    # - category-specific group and product metadata tables already exist
    #
    # Expected output state:
    # - one latest-row snapshot per product/subtype with momentum, trend, and
    #   product classification fields ready for API consumption.
    con.execute(f"""
CREATE OR REPLACE TABLE {table_name} AS
WITH latest_date AS (
    -- Establish the as-of date for the entire snapshot.
    SELECT MAX(date) AS latest_date
    FROM pokemon_prices
    WHERE categoryId = {category.category_id}
      AND marketPrice IS NOT NULL
),
base AS (
    -- Historical price rows for just this category. Every later CTE builds on this slice.
    SELECT
        date,
        categoryId,
        groupId,
        productId,
        subTypeName,
        marketPrice AS price
    FROM pokemon_prices
    WHERE categoryId = {category.category_id}
      AND marketPrice IS NOT NULL
),
with_ma AS (
    -- Add moving averages and rolling highs so the final snapshot can score trend strength.
    SELECT
        date,
        categoryId,
        groupId,
        productId,
        subTypeName,
        price,
        AVG(price) OVER (
            PARTITION BY groupId, productId, subTypeName
            ORDER BY date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS sma_3,
        AVG(price) OVER (
            PARTITION BY groupId, productId, subTypeName
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS sma_7,
        AVG(price) OVER (
            PARTITION BY groupId, productId, subTypeName
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS sma_30,
        AVG(price) OVER (
            PARTITION BY groupId, productId, subTypeName
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS sma_90,
        MAX(price) OVER (
            PARTITION BY groupId, productId, subTypeName
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS high_90d
    FROM base
),
recent_changes AS (
    -- Precompute recent day-to-day price changes once so the API does not need
    -- to rescan history every time the Top Movers screen loads.
    SELECT
        groupId,
        productId,
        subTypeName,
        date,
        price,
        LAG(price) OVER (
            PARTITION BY groupId, productId, subTypeName
            ORDER BY date
        ) AS prev_price
    FROM base
    WHERE date >= (SELECT latest_date FROM latest_date) - INTERVAL 37 DAY
),
top_mover_activity AS (
    -- Defaults mirror the dashboard Top Movers screen:
    -- - 30d lookback
    -- - 1% minimum daily move
    -- - activity counts used as quality filters
    SELECT
        groupId,
        productId,
        subTypeName,
        COUNT(*) FILTER (
            WHERE prev_price IS NOT NULL
              AND prev_price > 0
              AND ((price / prev_price) - 1) * 100 >= 1.0
              AND date >= (SELECT latest_date FROM latest_date) - INTERVAL 30 DAY
        ) AS top_mover_signal_days,
        COUNT(*) FILTER (
            WHERE prev_price IS NOT NULL
              AND date >= (SELECT latest_date FROM latest_date) - INTERVAL 30 DAY
        ) AS top_mover_observed_changes
    FROM recent_changes
    GROUP BY groupId, productId, subTypeName
),
top_mover_recent_variation AS (
    SELECT
        groupId,
        productId,
        subTypeName,
        COUNT(*) FILTER (
            WHERE date >= (SELECT latest_date FROM latest_date) - INTERVAL 14 DAY
        ) AS top_mover_recent_observations,
        COUNT(DISTINCT price) FILTER (
            WHERE date >= (SELECT latest_date FROM latest_date) - INTERVAL 14 DAY
        ) AS top_mover_recent_distinct_prices
    FROM recent_changes
    GROUP BY groupId, productId, subTypeName
),
top_mover_latest_window AS (
    SELECT
        groupId,
        productId,
        subTypeName,
        COUNT(*) FILTER (WHERE rn <= 3) AS top_mover_recent_points
    FROM (
        SELECT
            groupId,
            productId,
            subTypeName,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY groupId, productId, subTypeName
                ORDER BY date DESC
            ) AS rn
        FROM base
    )
    GROUP BY groupId, productId, subTypeName
),
top_mover_recent_activity AS (
    SELECT
        groupId,
        productId,
        subTypeName,
        MAX(date) FILTER (
            WHERE prev_price IS NOT NULL
              AND price <> prev_price
        ) AS top_mover_last_change_date
    FROM recent_changes
    GROUP BY groupId, productId, subTypeName
),
screen_recent_activity AS (
    -- Shared activity windows for multiple screener endpoints.
    SELECT
        groupId,
        productId,
        subTypeName,
        COUNT(*) FILTER (
            WHERE date >= (SELECT latest_date FROM latest_date) - INTERVAL 7 DAY
        ) AS recent_observations_7d,
        COUNT(DISTINCT price) FILTER (
            WHERE date >= (SELECT latest_date FROM latest_date) - INTERVAL 7 DAY
        ) AS recent_distinct_prices_7d,
        COUNT(DISTINCT price) FILTER (
            WHERE date >= (SELECT latest_date FROM latest_date) - INTERVAL 30 DAY
        ) AS recent_distinct_prices_30d,
        MAX(date) FILTER (
            WHERE prev_price IS NOT NULL
              AND price <> prev_price
        ) AS last_change_date
    FROM recent_changes
    GROUP BY groupId, productId, subTypeName
),
flagged AS (
    -- Boolean state flags for the product-level screener families.
    SELECT
        date,
        categoryId,
        groupId,
        productId,
        subTypeName,
        price,
        sma_3,
        sma_7,
        sma_30,
        sma_90,
        high_90d,
        CASE
            WHEN sma_30 IS NOT NULL AND price > sma_30 THEN 1
            ELSE 0
        END AS above30,
        CASE
            WHEN sma_30 IS NOT NULL
             AND sma_7 IS NOT NULL
             AND price > sma_30
             AND sma_7 > sma_30 THEN 1
            ELSE 0
        END AS bullish_day,
        CASE
            WHEN sma_30 IS NOT NULL
             AND sma_7 IS NOT NULL
             AND sma_3 IS NOT NULL
             AND price > sma_30
             AND sma_3 > sma_7
             AND sma_7 > sma_30 THEN 1
            ELSE 0
        END AS early_bullish_day
    FROM with_ma
),
flagged_runs AS (
    -- Running false counters let us measure consecutive streaks from the latest row.
    SELECT
        *,
        SUM(CASE WHEN above30 = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY groupId, productId, subTypeName
            ORDER BY date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS above30_false_run,
        SUM(CASE WHEN bullish_day = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY groupId, productId, subTypeName
            ORDER BY date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS bullish_false_run,
        SUM(CASE WHEN early_bullish_day = 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY groupId, productId, subTypeName
            ORDER BY date DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS early_false_run,
        LAG(above30) OVER (
            PARTITION BY groupId, productId, subTypeName
            ORDER BY date
        ) AS prev_above30
    FROM flagged
),
streak_summary AS (
    SELECT
        groupId,
        productId,
        subTypeName,
        MAX(CASE WHEN date = (SELECT latest_date FROM latest_date) THEN sma_3 END) AS latest_sma3,
        MAX(CASE WHEN date = (SELECT latest_date FROM latest_date) THEN sma_7 END) AS latest_sma7,
        MAX(CASE WHEN date = (SELECT latest_date FROM latest_date) THEN sma_30 END) AS latest_sma30,
        COUNT(*) FILTER (
            WHERE above30 = 1
              AND above30_false_run = 0
        ) AS hold_days,
        COUNT(*) FILTER (
            WHERE bullish_day = 1
              AND bullish_false_run = 0
        ) AS bullish_streak,
        COUNT(*) FILTER (
            WHERE early_bullish_day = 1
              AND early_false_run = 0
        ) AS early_streak,
        MAX(CASE
            WHEN above30 = 1
             AND COALESCE(prev_above30, 0) = 0
            THEN date
        END) AS cross_date
    FROM flagged_runs
    GROUP BY groupId, productId, subTypeName
),
latest_rows AS (
    -- Reduce to the latest available observation for each product/subtype pair.
    SELECT *
    FROM with_ma
    WHERE date = (SELECT latest_date FROM latest_date)
),
prior_7 AS (
    -- Most recent observation at least 7 days old, used to compute 7d ROC.
    SELECT groupId, productId, subTypeName, price AS price_7d
    FROM (
        SELECT
            groupId,
            productId,
            subTypeName,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY groupId, productId, subTypeName
                ORDER BY date DESC
            ) AS rn
        FROM base
        WHERE date <= (SELECT latest_date FROM latest_date) - INTERVAL 7 DAY
    )
    WHERE rn = 1
),
prior_30 AS (
    -- Most recent observation at least 30 days old, used to compute 30d ROC.
    SELECT groupId, productId, subTypeName, price AS price_30d
    FROM (
        SELECT
            groupId,
            productId,
            subTypeName,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY groupId, productId, subTypeName
                ORDER BY date DESC
            ) AS rn
        FROM base
        WHERE date <= (SELECT latest_date FROM latest_date) - INTERVAL 30 DAY
    )
    WHERE rn = 1
),
prior_90 AS (
    -- Most recent observation at least 90 days old, used to compute 90d ROC.
    SELECT groupId, productId, subTypeName, price AS price_90d
    FROM (
        SELECT
            groupId,
            productId,
            subTypeName,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY groupId, productId, subTypeName
                ORDER BY date DESC
            ) AS rn
        FROM base
        WHERE date <= (SELECT latest_date FROM latest_date) - INTERVAL 90 DAY
    )
    WHERE rn = 1
),
prior_365 AS (
    -- Most recent observation at least 365 days old, used to compute 1y ROC.
    SELECT groupId, productId, subTypeName, price AS price_365d
    FROM (
        SELECT
            groupId,
            productId,
            subTypeName,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY groupId, productId, subTypeName
                ORDER BY date DESC
            ) AS rn
        FROM base
        WHERE date <= (SELECT latest_date FROM latest_date) - INTERVAL 365 DAY
    )
    WHERE rn = 1
),
enriched AS (
    -- Join in metadata and classify each product so downstream screens can split
    -- cards vs sealed vs MCAP without repeating this logic in the API.
    SELECT
        l.date AS latest_date,
        l.categoryId,
        l.groupId,
        COALESCE(g.name, 'Unknown Group') AS groupName,
        l.productId,
        COALESCE(p.name, p.cleanName, 'Product ' || CAST(l.productId AS VARCHAR)) AS productName,
        p.cleanName,
        p.imageUrl,
        p.rarity,
        p.number,
        {PRODUCT_CLASS_SQL} AS productClass,
        {PRODUCT_KIND_SQL} AS productKind,
        l.subTypeName,
        l.price AS latest_price,
        p7.price_7d,
        p30.price_30d,
        p90.price_90d,
        p365.price_365d,
        ss.latest_sma3,
        ss.latest_sma7,
        ss.latest_sma30,
        ss.hold_days,
        ss.bullish_streak,
        ss.early_streak,
        ss.cross_date,
        sra.recent_observations_7d,
        sra.recent_distinct_prices_7d,
        sra.recent_distinct_prices_30d,
        sra.last_change_date,
        l.sma_30,
        l.sma_90,
        l.high_90d,
        tma.top_mover_signal_days,
        tma.top_mover_observed_changes,
        tmrv.top_mover_recent_observations,
        tmrv.top_mover_recent_distinct_prices,
        tmlw.top_mover_recent_points,
        tmra.top_mover_last_change_date
    FROM latest_rows l
    LEFT JOIN prior_7 p7
      ON p7.groupId = l.groupId
     AND p7.productId = l.productId
     AND p7.subTypeName = l.subTypeName
    LEFT JOIN prior_30 p30
      ON p30.groupId = l.groupId
     AND p30.productId = l.productId
     AND p30.subTypeName = l.subTypeName
    LEFT JOIN prior_90 p90
      ON p90.groupId = l.groupId
     AND p90.productId = l.productId
     AND p90.subTypeName = l.subTypeName
    LEFT JOIN prior_365 p365
      ON p365.groupId = l.groupId
     AND p365.productId = l.productId
     AND p365.subTypeName = l.subTypeName
    LEFT JOIN top_mover_activity tma
      ON tma.groupId = l.groupId
     AND tma.productId = l.productId
     AND tma.subTypeName = l.subTypeName
    LEFT JOIN top_mover_recent_variation tmrv
      ON tmrv.groupId = l.groupId
     AND tmrv.productId = l.productId
     AND tmrv.subTypeName = l.subTypeName
    LEFT JOIN top_mover_latest_window tmlw
      ON tmlw.groupId = l.groupId
     AND tmlw.productId = l.productId
     AND tmlw.subTypeName = l.subTypeName
    LEFT JOIN top_mover_recent_activity tmra
      ON tmra.groupId = l.groupId
     AND tmra.productId = l.productId
     AND tmra.subTypeName = l.subTypeName
    LEFT JOIN streak_summary ss
      ON ss.groupId = l.groupId
     AND ss.productId = l.productId
     AND ss.subTypeName = l.subTypeName
    LEFT JOIN screen_recent_activity sra
      ON sra.groupId = l.groupId
     AND sra.productId = l.productId
     AND sra.subTypeName = l.subTypeName
    LEFT JOIN {category.products_table} p
      ON p.groupId = l.groupId
     AND p.productId = l.productId
    LEFT JOIN {category.groups_table} g
      ON g.groupId = l.groupId
)
SELECT
    -- Final snapshot result: one row per product/subtype with price, ROC, moving
    -- average context, breakout state, and a composite trend score.
    latest_date,
    categoryId,
    groupId,
    groupName,
    productId,
    productName,
    cleanName,
    imageUrl,
    rarity,
    number,
    productClass,
    productKind,
    subTypeName,
    latest_price,
    price_7d,
    CASE WHEN price_7d IS NULL OR price_7d = 0 THEN NULL
         ELSE ((latest_price / price_7d) - 1) * 100 END AS roc_7d_pct,
    price_30d,
    CASE WHEN price_30d IS NULL OR price_30d = 0 THEN NULL
         ELSE ((latest_price / price_30d) - 1) * 100 END AS roc_30d_pct,
    price_90d,
    CASE WHEN price_90d IS NULL OR price_90d = 0 THEN NULL
         ELSE ((latest_price / price_90d) - 1) * 100 END AS roc_90d_pct,
    price_365d,
    CASE WHEN price_365d IS NULL OR price_365d = 0 THEN NULL
         ELSE ((latest_price / price_365d) - 1) * 100 END AS roc_365d_pct,
    top_mover_signal_days,
    top_mover_observed_changes,
    top_mover_recent_observations,
    top_mover_recent_distinct_prices,
    top_mover_recent_points,
    top_mover_last_change_date,
    recent_observations_7d,
    recent_distinct_prices_7d,
    recent_distinct_prices_30d,
    last_change_date,
    latest_sma3,
    latest_sma7,
    latest_sma30,
    hold_days,
    bullish_streak,
    early_streak,
    cross_date,
    sma_30,
    CASE WHEN sma_30 IS NULL OR sma_30 = 0 THEN NULL
         ELSE ((latest_price / sma_30) - 1) * 100 END AS price_vs_sma30_pct,
    sma_90,
    CASE WHEN sma_90 IS NULL OR sma_90 = 0 THEN NULL
         ELSE ((latest_price / sma_90) - 1) * 100 END AS price_vs_sma90_pct,
    high_90d,
    CASE WHEN latest_price >= high_90d AND high_90d IS NOT NULL THEN 1 ELSE 0 END AS breakout_90d_flag,
    (
        COALESCE(
            CASE WHEN price_7d IS NULL OR price_7d = 0 THEN NULL
                 ELSE ((latest_price / price_7d) - 1) * 100 END,
            0
        ) -
        COALESCE(
            CASE WHEN price_30d IS NULL OR price_30d = 0 THEN NULL
                 ELSE ((latest_price / price_30d) - 1) * 100 END,
            0
        )
    ) AS acceleration_7d_vs_30d,
    (
        0.4 * COALESCE(LEAST(GREATEST(
            CASE WHEN price_30d IS NULL OR price_30d = 0 THEN NULL
                 ELSE ((latest_price / price_30d) - 1) * 100 END,
            -100
        ), 300), 0) +
        0.2 * COALESCE(LEAST(GREATEST(
            CASE WHEN price_7d IS NULL OR price_7d = 0 THEN NULL
                 ELSE ((latest_price / price_7d) - 1) * 100 END,
            -100
        ), 300), 0) +
        15 * CASE WHEN latest_price >= high_90d AND high_90d IS NOT NULL THEN 1 ELSE 0 END +
        10 * CASE WHEN latest_price > sma_30 AND sma_30 IS NOT NULL THEN 1 ELSE 0 END +
        10 * CASE WHEN latest_price > sma_90 AND sma_90 IS NOT NULL THEN 1 ELSE 0 END
    ) AS trend_score
FROM enriched
ORDER BY trend_score DESC, roc_30d_pct DESC, latest_price DESC
""")

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM {table_name}
            ORDER BY trend_score DESC, roc_30d_pct DESC, latest_price DESC
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
