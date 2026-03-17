import duckdb

DATA_DIR = "/app/data/extracted"
CSV_PATH = f"{DATA_DIR}/pokemon_prices_all_days.csv"
OUT_CSV = f"{DATA_DIR}/movers_30d.csv"

con = duckdb.connect()

# Create a view over the large CSV (no full load into memory)
con.execute(f"""
CREATE OR REPLACE VIEW prices AS
SELECT
  CAST(date AS DATE) AS d,
  CAST(productId AS BIGINT) AS productId,
  CAST(groupId AS BIGINT) AS groupId,
  subTypeName,
  CAST(marketPrice AS DOUBLE) AS marketPrice
FROM read_csv_auto('{CSV_PATH}', ignore_errors=true)
WHERE marketPrice IS NOT NULL;
""")

# Find the most recent date in the dataset
latest_date = con.execute("""
SELECT MAX(d) FROM prices
""").fetchone()[0]

print("Latest date found:", latest_date)

# Build the 30-day movers report
con.execute(f"""
COPY (
  WITH last AS (
    SELECT productId, subTypeName, groupId, marketPrice AS price_last
    FROM prices
    WHERE d = DATE '{latest_date}'
  ),
  prev AS (
    SELECT productId, subTypeName, marketPrice AS price_prev
    FROM prices
    WHERE d = DATE '{latest_date}' - INTERVAL 30 DAY
  )
  SELECT
    l.groupId,
    l.productId,
    l.subTypeName,
    l.price_last,
    p.price_prev,
    ROUND(((l.price_last - p.price_prev) / p.price_prev) * 100, 2) AS pct_change_30d
  FROM last l
  JOIN prev p
    ON l.productId = p.productId
   AND l.subTypeName = p.subTypeName
  WHERE p.price_prev > 0
  ORDER BY pct_change_30d DESC
  LIMIT 200
)
TO '{OUT_CSV}' WITH (HEADER, DELIMITER ',');
""")

print("Wrote:", OUT_CSV)

