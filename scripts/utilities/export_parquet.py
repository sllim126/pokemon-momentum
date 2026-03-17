from pathlib import Path

import duckdb

DATA_ROOT = Path("/app/data")
PROCESSED_DIR = DATA_ROOT / "processed"
PARQUET_ROOT = DATA_ROOT / "parquet"
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"


def pick_source_table(con: duckdb.DuckDBPyConnection) -> str:
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    if "pokemon_prices" in tables:
        return "pokemon_prices"
    if "prices" in tables:
        return "prices"
    raise RuntimeError("No source table found. Expected 'pokemon_prices' or 'prices' in prices_db.duckdb.")


def parquet_dates() -> set[str]:
    if not PARQUET_ROOT.exists():
        return set()
    return {
        path.name.split("=", 1)[1]
        for path in PARQUET_ROOT.iterdir()
        if path.is_dir() and path.name.startswith("date=")
    }


PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_ROOT.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(DB_PATH))
source_table = pick_source_table(con)
db_dates = {
    str(row[0])
    for row in con.execute(f"SELECT DISTINCT date FROM {source_table} ORDER BY date").fetchall()
    if row[0] is not None
}
existing_parquet_dates = parquet_dates()
dates_to_export = sorted(db_dates - existing_parquet_dates)

if not dates_to_export:
    con.close()
    print("No new dates to export")
    print("Database:", DB_PATH)
    print("Source table:", source_table)
    print("Parquet root:", PARQUET_ROOT)
    raise SystemExit(0)

for date_str in dates_to_export:
    partition_dir = PARQUET_ROOT / f"date={date_str}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    out_file = partition_dir / "data.parquet"
    con.execute(f"""
    COPY (
        SELECT *
        FROM {source_table}
        WHERE date = DATE '{date_str}'
    ) TO '{out_file.as_posix()}'
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

con.close()

print("Export complete")
print("Database:", DB_PATH)
print("Source table:", source_table)
print("Parquet root:", PARQUET_ROOT)
print("Dates exported:", len(dates_to_export))
print("First date exported:", dates_to_export[0])
print("Last date exported:", dates_to_export[-1])
