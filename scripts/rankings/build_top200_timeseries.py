import duckdb

EXTRACTED_DIR = "/app/data/extracted"
PROCESSED_DIR = "/app/data/processed"
DB_PATH = f"{PROCESSED_DIR}/prices_db.duckdb"
OUT_FILE = f"{EXTRACTED_DIR}/top200_timeseries.csv"
TABLE_NAME = "top200_timeseries"

con = duckdb.connect(DB_PATH)
con.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} AS
WITH filtered AS (
    SELECT
        p.date,
        p.productId,
        p.subTypeName,
        p.marketPrice AS price
    FROM pokemon_prices p
    JOIN top200_universe u USING (productId, subTypeName)
    WHERE p.marketPrice IS NOT NULL
)
SELECT
    productId,
    subTypeName,
    date,
    price,
    AVG(price) OVER (
        PARTITION BY productId, subTypeName
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS sma_7,
    AVG(price) OVER (
        PARTITION BY productId, subTypeName
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS sma_30
FROM filtered
ORDER BY productId, subTypeName, date
""")
con.execute(
    f"""
    COPY (
      SELECT *
      FROM {TABLE_NAME}
      ORDER BY productId, subTypeName, date
    ) TO '{OUT_FILE}' WITH (HEADER, DELIMITER ',')
    """
)
rows = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
con.close()

print("Saving:", OUT_FILE)
print("DuckDB table:", TABLE_NAME)
print("Database:", DB_PATH)
print("Done. Rows:", rows)
