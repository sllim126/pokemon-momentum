import duckdb
import pandas as pd

DATA_DIR = "/app/data/extracted"
CSV_PATH = f"{DATA_DIR}/pokemon_prices_all_days.csv"
OUT_CSV = f"{DATA_DIR}/top200_universe.csv"

con = duckdb.connect()

df = con.execute(f"""
WITH prices AS (
  SELECT
    CAST(date AS DATE) AS d,
    CAST(productId AS BIGINT) AS productId,
    subTypeName,
    CAST(marketPrice AS DOUBLE) AS marketPrice
  FROM read_csv_auto('{CSV_PATH}', ignore_errors=true)
  WHERE marketPrice IS NOT NULL
),
latest_date AS (
  SELECT MAX(d) AS d FROM prices
),
latest AS (
  SELECT productId, subTypeName, marketPrice AS latest_price
  FROM prices
  WHERE d = (SELECT d FROM latest_date)
),
prior AS (
  SELECT productId, subTypeName, marketPrice AS prior_price
  FROM prices
  WHERE d = (SELECT d FROM latest_date) - INTERVAL 30 DAY
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
""").fetchdf()

df.to_csv(OUT_CSV, index=False)
print("Wrote:", OUT_CSV)
print("Rows:", len(df))
print(df.head(10))