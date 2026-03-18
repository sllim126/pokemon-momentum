import duckdb

EXTRACTED_DIR = "/app/data/extracted"
PROCESSED_DIR = "/app/data/processed"
DB_PATH = f"{PROCESSED_DIR}/prices_db.duckdb"
OUT_CSV = f"{EXTRACTED_DIR}/group_signal_snapshot.csv"
TABLE_NAME = "group_signal_snapshot"

con = duckdb.connect(DB_PATH)

tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
if "product_signal_snapshot" not in tables:
    raise RuntimeError(
        "product_signal_snapshot does not exist. Run scripts/indicators/build_product_signal_snapshot.py first."
    )

con.execute(
    f"""
    CREATE OR REPLACE TABLE {TABLE_NAME} AS
    WITH latest_snapshot AS (
        SELECT *
        FROM product_signal_snapshot
        WHERE latest_date = (SELECT MAX(latest_date) FROM product_signal_snapshot)
          AND categoryId = 3
    ),
    grouped AS (
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

con.execute(
    f"""
    COPY (
        SELECT *
        FROM {TABLE_NAME}
        ORDER BY breadth_score DESC, avg_30d_pct DESC, pct_above_sma30 DESC, groupName
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
