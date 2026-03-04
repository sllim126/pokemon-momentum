import duckdb

UNIVERSE  = r"F:\Pokemon historical data extracted\top200_universe.csv"
PRODUCTS  = r"F:\Pokemon historical data extracted\pokemon_products.csv"
GROUPS    = r"F:\Pokemon historical data extracted\pokemon_groups.csv"
OUT       = r"F:\Pokemon historical data extracted\top200_lookup.csv"

con = duckdb.connect()

con.execute(f"""
COPY (
  WITH u AS (
    SELECT DISTINCT
      CAST(productId AS BIGINT) AS productId,
      subTypeName
    FROM read_csv_auto('{UNIVERSE}', ignore_errors=true)
  ),
  p AS (
    SELECT
      CAST(productId AS BIGINT) AS productId,
      CAST(groupId AS BIGINT) AS groupId,
      name AS productName,
      imageUrl
    FROM read_csv_auto('{PRODUCTS}', ignore_errors=true)
  ),
  g AS (
    SELECT
      CAST(groupId AS BIGINT) AS groupId,
      name AS groupName
    FROM read_csv_auto('{GROUPS}', ignore_errors=true)
  )
  SELECT
    u.productId,
    u.subTypeName,
    p.productName,
    p.groupId,
    g.groupName,
    p.imageUrl
  FROM u
  LEFT JOIN p USING(productId)
  LEFT JOIN g USING(groupId)
  ORDER BY g.groupName, p.productName, u.subTypeName
) TO '{OUT}' WITH (HEADER, DELIMITER ',');
""")

print("Wrote:", OUT)