import duckdb

DATA_DIR = "/app/data/extracted"
CSV_PATH = f"{DATA_DIR}/pokemon_prices_all_days.csv"

con = duckdb.connect()

latest = con.execute(f"""
SELECT MAX(CAST(date AS DATE))
FROM read_csv_auto('{CSV_PATH}', ignore_errors=true)
""").fetchone()

print("Latest date in dataset:", latest[0])