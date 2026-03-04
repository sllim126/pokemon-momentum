import duckdb

CSV_PATH = r"F:\Pokemon historical data extracted\pokemon_prices_all_days.csv"
PRODUCT_ID = 662184

con = duckdb.connect()

rows = con.execute(f"""
SELECT DISTINCT subTypeName
FROM read_csv_auto('{CSV_PATH}', ignore_errors=true)
WHERE productId = {PRODUCT_ID}
ORDER BY subTypeName
""").fetchall()

print("Subtypes for productId", PRODUCT_ID)
for r in rows:
    print("-", r[0])