import duckdb

EXTRACTED_DIR = "/app/data/extracted"
PROCESSED_DIR = "/app/data/processed"
DB_PATH = f"{PROCESSED_DIR}/prices_db.duckdb"
OUT_CSV = f"{EXTRACTED_DIR}/product_signal_snapshot.csv"
TABLE_NAME = "product_signal_snapshot"

con = duckdb.connect(DB_PATH)

con.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} AS
WITH latest_date AS (
    SELECT MAX(date) AS latest_date
    FROM pokemon_prices
    WHERE categoryId = 3
      AND marketPrice IS NOT NULL
),
base AS (
    SELECT
        date,
        categoryId,
        groupId,
        productId,
        subTypeName,
        marketPrice AS price
    FROM pokemon_prices
    WHERE categoryId = 3
      AND marketPrice IS NOT NULL
),
with_ma AS (
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
    SELECT *
    FROM with_ma
    WHERE date = (SELECT latest_date FROM latest_date)
),
prior_7 AS (
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
    LEFT JOIN pokemon_products p
      ON p.groupId = l.groupId
     AND p.productId = l.productId
    LEFT JOIN pokemon_groups g
      ON g.groupId = l.groupId
)
SELECT
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
        FROM {TABLE_NAME}
        ORDER BY trend_score DESC, roc_30d_pct DESC, latest_price DESC
    ) TO '{OUT_CSV}' WITH (HEADER, DELIMITER ',')
    """
)

rows = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
latest = con.execute(f"SELECT MAX(latest_date) FROM {TABLE_NAME}").fetchone()[0]
con.close()

print("Wrote:", OUT_CSV)
print("DuckDB table:", TABLE_NAME)
print("Database:", DB_PATH)
print("Latest date:", latest)
print("Rows:", rows)
