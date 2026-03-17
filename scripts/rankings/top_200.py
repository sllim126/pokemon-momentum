import duckdb

EXTRACTED_DIR = "/app/data/extracted"
PROCESSED_DIR = "/app/data/processed"
DB_PATH = f"{PROCESSED_DIR}/prices_db.duckdb"
OUT_CSV = f"{EXTRACTED_DIR}/top200_universe.csv"
TABLE_NAME = "top200_universe"

con = duckdb.connect(DB_PATH)

con.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} AS
WITH latest_date AS (
  SELECT MAX(date) AS d FROM pokemon_prices
),
latest AS (
  SELECT productId, subTypeName, marketPrice AS latest_price
  FROM pokemon_prices
  WHERE date = (SELECT d FROM latest_date)
    AND marketPrice IS NOT NULL
),
prior AS (
  SELECT productId, subTypeName, marketPrice AS prior_price
  FROM pokemon_prices
  WHERE date = (SELECT d FROM latest_date) - INTERVAL 30 DAY
    AND marketPrice IS NOT NULL
)
SELECT
  l.productId,
  l.subTypeName,
  l.latest_price,
  p.prior_price,
  ROUND(((l.latest_price - p.prior_price) / p.prior_price) * 100, 2) AS pct_change_30d
FROM latest l
JOIN prior p USING(productId, subTypeName)
WHERE p.prior_price >= 5
ORDER BY pct_change_30d DESC
LIMIT 200
""")
con.execute(
    f"""
    COPY (
      SELECT *
      FROM {TABLE_NAME}
      ORDER BY pct_change_30d DESC
    ) TO '{OUT_CSV}' WITH (HEADER, DELIMITER ',')
    """
)
df = con.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY pct_change_30d DESC").fetchdf()
con.close()

print("Wrote:", OUT_CSV)
print("DuckDB table:", TABLE_NAME)
print("Database:", DB_PATH)
print("Rows:", len(df))
print(df.head(10))
