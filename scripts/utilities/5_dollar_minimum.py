import duckdb

DATA_DIR = "/app/data/extracted"
CSV_PATH = f"{DATA_DIR}/pokemon_prices_all_days.csv"
OUT_CSV = f"{DATA_DIR}/movers_30d_min5.csv"

con = duckdb.connect()

con.execute(f"""
CREATE OR REPLACE VIEW prices AS
SELECT
  CAST(date AS DATE) AS d,
  CAST(productId AS BIGINT) AS productId,
  CAST(groupId AS BIGINT) AS groupId,
  subTypeName,
  CAST(marketPrice AS DOUBLE) AS marketPrice
FROM read_csv_auto('{CSV_PATH}', ignore_errors=true);
""")

latest_date = con.execute("""
SELECT MAX(d)
FROM prices
WHERE marketPrice IS NOT NULL
""").fetchone()[0]

print("Latest date in file:", latest_date)

con.execute(f"""
COPY (
  WITH latest AS (
    SELECT productId, subTypeName, groupId, marketPrice AS p_latest
    FROM prices
    WHERE d = DATE '{latest_date}'
  ),
  prior AS (
    SELECT productId, subTypeName, marketPrice AS p_prior
    FROM prices
    WHERE d = DATE '{latest_date}' - INTERVAL 30 DAY
  )
  SELECT
    l.groupId,
    l.productId,
    l.subTypeName,
    l.p_latest,
    p.p_prior,
    ROUND(((l.p_latest - p.p_prior) / p.p_prior) * 100, 2) AS pct_change_30d
  FROM latest l
  JOIN prior p
    ON l.productId = p.productId
   AND l.subTypeName = p.subTypeName
  WHERE
    p.p_prior >= 5
    AND l.p_latest IS NOT NULL
  ORDER BY pct_change_30d DESC
  LIMIT 300
) TO '{OUT_CSV}' WITH (HEADER, DELIMITER ',');
""")

print("Wrote:", OUT_CSV)
