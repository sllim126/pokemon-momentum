import duckdb

EXTRACTED_DIR = "/app/data/extracted"
PROCESSED_DIR = "/app/data/processed"
DB_PATH = f"{PROCESSED_DIR}/prices_db.duckdb"
OUT = f"{EXTRACTED_DIR}/top200_lookup.csv"
TABLE_NAME = "top200_lookup"

con = duckdb.connect(DB_PATH)

con.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} AS
WITH u AS (
    SELECT DISTINCT productId, subTypeName
    FROM top200_universe
  ),
  p AS (
    SELECT productId, groupId, name AS productName, imageUrl
    FROM pokemon_products
  ),
  g AS (
    SELECT groupId, name AS groupName
    FROM pokemon_groups
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
""")
con.execute(
    f"""
    COPY (
      SELECT *
      FROM {TABLE_NAME}
      ORDER BY groupName, productName, subTypeName
    ) TO '{OUT}' WITH (HEADER, DELIMITER ',')
    """
)
con.close()

print("Wrote:", OUT)
print("DuckDB table:", TABLE_NAME)
print("Database:", DB_PATH)
