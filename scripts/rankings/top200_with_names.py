import duckdb

EXTRACTED_DIR = "/app/data/extracted"
PROCESSED_DIR = "/app/data/processed"
DB_PATH = f"{PROCESSED_DIR}/prices_db.duckdb"
OUT = f"{EXTRACTED_DIR}/top200_30d_min5_named.csv"
TABLE_NAME = "top200_30d_min5_named"

con = duckdb.connect(DB_PATH)

con.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} AS
WITH latest_date AS (SELECT MAX(date) AS d FROM pokemon_prices),
latest AS (
  SELECT groupId, productId, subTypeName, marketPrice AS p_latest
  FROM pokemon_prices
  WHERE date = (SELECT d FROM latest_date)
    AND marketPrice IS NOT NULL
),
prior AS (
  SELECT productId, subTypeName, marketPrice AS p_prior
  FROM pokemon_prices
  WHERE date = (SELECT d FROM latest_date) - INTERVAL 30 DAY
    AND marketPrice IS NOT NULL
),
movers AS (
  SELECT
    l.groupId,
    l.productId,
    l.subTypeName,
    l.p_latest,
    p.p_prior,
    ROUND(((l.p_latest - p.p_prior) / p.p_prior) * 100, 2) AS pct_change_30d
  FROM latest l
  JOIN prior p USING(productId, subTypeName)
  WHERE p.p_prior >= 5
  ORDER BY pct_change_30d DESC
  LIMIT 200
),
SELECT
  m.*,
  g.name AS groupName,
  pr.name AS productName
FROM movers m
LEFT JOIN pokemon_groups g USING(groupId)
LEFT JOIN pokemon_products pr USING(groupId, productId)
ORDER BY pct_change_30d DESC
""")
con.execute(
    f"""
    COPY (
      SELECT *
      FROM {TABLE_NAME}
      ORDER BY pct_change_30d DESC
    ) TO '{OUT}' WITH (HEADER, DELIMITER ',')
    """
)
df = con.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY pct_change_30d DESC").fetchdf()
con.close()

print("Wrote:", OUT)
print("DuckDB table:", TABLE_NAME)
print("Database:", DB_PATH)
print(df.head(10))
