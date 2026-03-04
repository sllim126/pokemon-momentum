import duckdb

INDICATORS = r"F:\Pokemon historical data extracted\top200_indicators.csv"
LOOKUP     = r"F:\Pokemon historical data extracted\top200_lookup.csv"
OUT        = r"F:\Pokemon historical data extracted\top200_indicators_latest_named.csv"

con = duckdb.connect()

con.execute(f"""
COPY (
  WITH ind AS (
    SELECT
      CAST(d AS DATE) AS d,
      CAST(productId AS BIGINT) AS productId,
      subTypeName,
      CAST(price AS DOUBLE) AS price,
      CAST(sma_7 AS DOUBLE) AS sma_7,
      CAST(sma_30 AS DOUBLE) AS sma_30,
      trend_state
    FROM read_csv_auto('{INDICATORS}', ignore_errors=true)
  ),
  latest AS (
    SELECT MAX(d) AS d FROM ind
  ),
  ind_latest AS (
    SELECT *
    FROM ind
    WHERE d = (SELECT d FROM latest)
  ),
  lu AS (
    SELECT
      CAST(productId AS BIGINT) AS productId,
      subTypeName,
      groupName,
      productName,
      imageUrl
    FROM read_csv_auto('{LOOKUP}', ignore_errors=true)
  )
  SELECT
    i.d AS asOfDate,
    i.productId,
    i.subTypeName,
    lu.groupName,
    lu.productName,
    lu.imageUrl,
    i.price,
    i.sma_7,
    i.sma_30,
    i.trend_state
  FROM ind_latest i
  LEFT JOIN lu USING(productId, subTypeName)
  ORDER BY i.trend_state, i.price DESC
) TO '{OUT}' WITH (HEADER, DELIMITER ',');
""")

print("Wrote:", OUT)