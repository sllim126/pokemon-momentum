import duckdb
import pandas as pd

DATA_DIR = "/app/data/extracted"
PRICES = f"{DATA_DIR}/pokemon_prices_all_days.csv"
PRODUCTS = f"{DATA_DIR}/pokemon_products.csv"
GROUPS = f"{DATA_DIR}/pokemon_groups.csv"
OUT = f"{DATA_DIR}/top200_30d_min5_named.csv"

con = duckdb.connect()

df = con.execute(f"""
WITH prices AS (
  SELECT
    CAST(date AS DATE) AS d,
    CAST(groupId AS BIGINT) AS groupId,
    CAST(productId AS BIGINT) AS productId,
    subTypeName,
    CAST(marketPrice AS DOUBLE) AS marketPrice
  FROM read_csv_auto('{PRICES}', ignore_errors=true)
  WHERE marketPrice IS NOT NULL
),
latest_date AS (SELECT MAX(d) AS d FROM prices),
latest AS (
  SELECT groupId, productId, subTypeName, marketPrice AS p_latest
  FROM prices
  WHERE d = (SELECT d FROM latest_date)
),
prior AS (
  SELECT productId, subTypeName, marketPrice AS p_prior
  FROM prices
  WHERE d = (SELECT d FROM latest_date) - INTERVAL 30 DAY
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
products AS (
  SELECT CAST(productId AS BIGINT) AS productId, name AS productName
  FROM read_csv_auto('{PRODUCTS}', ignore_errors=true)
),
groups AS (
  SELECT CAST(groupId AS BIGINT) AS groupId, name AS groupName
  FROM read_csv_auto('{GROUPS}', ignore_errors=true)
)
SELECT
  m.*,
  g.groupName,
  pr.productName
FROM movers m
LEFT JOIN groups g USING(groupId)
LEFT JOIN products pr USING(productId)
ORDER BY pct_change_30d DESC
""").fetchdf()

df.to_csv(OUT, index=False)
print("Wrote:", OUT)
print(df.head(10))