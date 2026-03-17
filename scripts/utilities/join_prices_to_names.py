import csv
import duckdb
from pathlib import Path

DATA_DIR = "/app/data/extracted"
PRICES_CSV = f"{DATA_DIR}/pokemon_prices_all_days.csv"
GROUPS_CSV = f"{DATA_DIR}/pokemon_groups.csv"
PRODUCTS_CSV = f"{DATA_DIR}/pokemon_products.csv"
OUT_CSV = f"{DATA_DIR}/pokemon_prices_named.csv"
PROCESSED_DIR = Path("/app/data/processed")
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"
TABLE_NAME = "pokemon_prices_named"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
con = duckdb.connect(str(DB_PATH))
con.execute(
    f"""
    CREATE OR REPLACE TABLE {TABLE_NAME} AS
    SELECT
        p.date,
        p.categoryId,
        p.groupId,
        p.productId,
        p.subTypeName,
        p.lowPrice,
        p.midPrice,
        p.highPrice,
        p.marketPrice,
        p.directLowPrice,
        g.name AS groupName,
        pr.name AS productName
    FROM pokemon_prices p
    LEFT JOIN pokemon_groups g USING (groupId)
    LEFT JOIN pokemon_products pr USING (groupId, productId)
    """
)
con.execute(
    f"""
    COPY (
        SELECT *
        FROM {TABLE_NAME}
        ORDER BY date, groupId, productId, subTypeName
    ) TO '{OUT_CSV}' WITH (HEADER, DELIMITER ',')
    """
)
con.close()

print("Wrote:", OUT_CSV)
print("DuckDB table:", TABLE_NAME)
print("Database:", DB_PATH)
