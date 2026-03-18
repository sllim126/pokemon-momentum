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
        CASE
            WHEN COALESCE(NULLIF(p.number, ''), '') <> ''
              OR COALESCE(NULLIF(p.rarity, ''), '') <> ''
              THEN 'card'
            WHEN lower(COALESCE(p.name, '')) LIKE '%ultra-premium collection%'
              OR lower(COALESCE(p.name, '')) LIKE '%ultra premium collection%'
              THEN 'mcap'
            WHEN lower(COALESCE(p.name, '')) LIKE '%booster box%'
              OR lower(COALESCE(p.name, '')) LIKE '%elite trainer box%'
              OR lower(COALESCE(p.name, '')) LIKE '% etb%'
              OR lower(COALESCE(p.name, '')) LIKE 'etb%'
              THEN 'sealed_booster_box'
            WHEN lower(COALESCE(p.name, '')) LIKE '%booster pack%'
              OR lower(COALESCE(p.name, '')) LIKE '%booster bundle%'
              OR lower(COALESCE(p.name, '')) LIKE '%bundle%'
              OR lower(COALESCE(p.name, '')) LIKE '%mini tin%'
              OR lower(COALESCE(p.name, '')) LIKE '% tin%'
              OR lower(COALESCE(p.name, '')) LIKE 'tin%'
              OR lower(COALESCE(p.name, '')) LIKE '%blister case%'
              OR lower(COALESCE(p.name, '')) LIKE '%blister%'
              OR lower(COALESCE(p.name, '')) LIKE '%premium figure collection%'
              OR lower(COALESCE(p.name, '')) LIKE '%figure collection%'
              OR lower(COALESCE(p.name, '')) LIKE '%premium figure set%'
              OR lower(COALESCE(p.name, '')) LIKE '%sleeved%'
              THEN 'sealed_booster_pack'
            ELSE 'other'
        END AS productClass,
        CASE
            WHEN COALESCE(NULLIF(p.number, ''), '') <> ''
              OR COALESCE(NULLIF(p.rarity, ''), '') <> ''
              THEN 'card'
            WHEN lower(COALESCE(p.name, '')) LIKE '%booster box%'
              OR lower(COALESCE(p.name, '')) LIKE '%elite trainer box%'
              OR lower(COALESCE(p.name, '')) LIKE '% etb%'
              OR lower(COALESCE(p.name, '')) LIKE 'etb%'
              OR lower(COALESCE(p.name, '')) LIKE '%booster pack%'
              OR lower(COALESCE(p.name, '')) LIKE '%booster bundle%'
              OR lower(COALESCE(p.name, '')) LIKE '%bundle%'
              OR lower(COALESCE(p.name, '')) LIKE '%mini tin%'
              OR lower(COALESCE(p.name, '')) LIKE '% tin%'
              OR lower(COALESCE(p.name, '')) LIKE 'tin%'
              OR lower(COALESCE(p.name, '')) LIKE '%blister case%'
              OR lower(COALESCE(p.name, '')) LIKE '%blister%'
              OR lower(COALESCE(p.name, '')) LIKE '%premium figure collection%'
              OR lower(COALESCE(p.name, '')) LIKE '%figure collection%'
              OR lower(COALESCE(p.name, '')) LIKE '%premium figure set%'
              OR lower(COALESCE(p.name, '')) LIKE '%sleeved%'
              THEN 'sealed'
            WHEN lower(COALESCE(p.name, '')) LIKE '%ultra-premium collection%'
              OR lower(COALESCE(p.name, '')) LIKE '%ultra premium collection%'
              THEN 'mcap'
            ELSE 'other'
        END AS productKind,
        l.subTypeName,
        l.price AS latest_price,
        p7.price_7d,
        p30.price_30d,
        p90.price_90d,
        p365.price_365d,
        l.sma_30,
        l.sma_90,
        l.high_90d
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
