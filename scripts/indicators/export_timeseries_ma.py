import duckdb
import pandas as pd

DATA_DIR = "/app/data/extracted"
CSV_PATH = f"{DATA_DIR}/pokemon_prices_all_days.csv"

PRODUCT_ID = 662184
SUBTYPE = "Holofoil"  # change this to the subtype you saw

OUT_CSV = "/app/output/charizard_timeseries.csv"

con = duckdb.connect()

df = con.execute(f"""
SELECT
  CAST(date AS DATE) AS d,
  CAST(marketPrice AS DOUBLE) AS price
FROM read_csv_auto('{CSV_PATH}', ignore_errors=true)
WHERE
  productId = {PRODUCT_ID}
  AND subTypeName = '{SUBTYPE}'
  AND marketPrice IS NOT NULL
ORDER BY d
""").fetchdf()

df["sma_7"] = df["price"].rolling(7).mean()
df["sma_30"] = df["price"].rolling(30).mean()

df.to_csv(OUT_CSV, index=False)
print("Rows:", len(df))
print("Wrote:", OUT_CSV)
print(df.tail(10))