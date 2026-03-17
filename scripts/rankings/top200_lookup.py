import duckdb
import pandas as pd

DATA_DIR = "/app/data/extracted"
UNIVERSE = f"{DATA_DIR}/top200_universe.csv"
PRODUCTS = f"{DATA_DIR}/pokemon_products.csv"
GROUPS   = f"{DATA_DIR}/pokemon_groups.csv"
OUT      = f"{DATA_DIR}/top200_lookup.csv"

con = duckdb.connect()

df = con.execute(f"""
WITH u AS (
  SELECT
    CAST(groupId AS BIGINT) AS groupId,
    CAST(productId AS BIGINT) AS productId,
    subTypeName
  FROM read_csv_auto('{UNIVERSE}', ignore_errors=true)
),
p AS (
  SELECT
    CAST(productId AS BIGINT) AS productId,
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
  u.groupId,
  g.groupName,
  u.productId,
  p.productName,
  u.subTypeName,
  p.imageUrl
FROM u
LEFT JOIN g USING(groupId)
LEFT JOIN p USING(productId)
ORDER BY g.groupName, p.productName, u.subTypeName
""").fetchdf()

df.to_csv(OUT, index=False)
print("Wrote:", OUT)
print(df.head(10))