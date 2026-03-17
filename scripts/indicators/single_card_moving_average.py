import duckdb

DATA_DIR = "/app/data/extracted"
CSV_PATH = f"{DATA_DIR}/pokemon_prices_all_days.csv"
OUT_CSV = "/app/output/single_card_timeseries.csv"

PRODUCT_ID = 662184
SUBTYPE = "Holofoil"

con = duckdb.connect()

con.execute(f"""
WITH base AS (
  SELECT
    CAST(date AS DATE) AS d,
    CAST(marketPrice AS DOUBLE) AS price
  FROM read_csv_auto('{CSV_PATH}', ignore_errors=true)
  WHERE
    productId = {PRODUCT_ID}
    AND subTypeName = '{SUBTYPE}'
    AND marketPrice IS NOT NULL
),
ma AS (
  SELECT
    d,
    price,
    AVG(price) OVER (
      ORDER BY d
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS sma_7,
    AVG(price) OVER (
      ORDER BY d
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS sma_30
  FROM base
)
SELECT *
FROM ma
ORDER BY d;
""")

df = con.fetch_df()
print(df.tail(10))

df.to_csv(OUT_CSV, index=False)
print("Wrote:", OUT_CSV)
