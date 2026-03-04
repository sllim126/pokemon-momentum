import duckdb

TS  = r"F:\Pokemon historical data extracted\group_23237_timeseries.csv"
OUT = r"F:\Pokemon historical data extracted\group_23237_indicators.csv"

con = duckdb.connect()

con.execute(f"""
COPY (
  SELECT
    d,
    productId,
    subTypeName,
    marketPrice AS price,

    AVG(marketPrice) OVER (
      PARTITION BY productId, subTypeName
      ORDER BY d
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS sma_7,

    AVG(marketPrice) OVER (
      PARTITION BY productId, subTypeName
      ORDER BY d
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS sma_30

  FROM read_csv_auto('{TS}', ignore_errors=true)
) TO '{OUT}' WITH (HEADER, DELIMITER ',');
""")

print("Wrote:", OUT)