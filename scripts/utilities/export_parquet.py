from pathlib import Path

import duckdb

DATA_ROOT = Path("/app/data")
PROCESSED_DIR = DATA_ROOT / "processed"
PARQUET_ROOT = DATA_ROOT / "parquet"
PARQUET_GLOB = str(PARQUET_ROOT / "**/*.parquet")
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"


def pick_source_table(con: duckdb.DuckDBPyConnection) -> str:
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    if "pokemon_prices" in tables:
        return "pokemon_prices"
    if "prices" in tables:
        return "prices"
    raise RuntimeError("No source table found. Expected 'pokemon_prices' or 'prices' in prices_db.duckdb.")


def parquet_slices(con: duckdb.DuckDBPyConnection) -> set[tuple[str, int]]:
    """(date, categoryId) pairs already present in the parquet store.

    Checking pairs rather than dates matters: the daily pipeline runs one category
    at a time, so a date folder created by the English run must still receive the
    Japanese rows when that category runs later.
    """
    if not any(PARQUET_ROOT.rglob("*.parquet")):
        return set()
    rows = con.execute(f"SELECT DISTINCT date, categoryId FROM read_parquet('{PARQUET_GLOB}')").fetchall()
    return {(str(day), int(category_id)) for day, category_id in rows}


PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_ROOT.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(str(DB_PATH))
source_table = pick_source_table(con)
db_slices = {
    (str(day), int(category_id))
    for day, category_id in con.execute(
        f"SELECT DISTINCT date, categoryId FROM {source_table} WHERE date IS NOT NULL"
    ).fetchall()
}
slices_to_export = sorted(db_slices - parquet_slices(con))

if not slices_to_export:
    con.close()
    print("No new date/category slices to export")
    print("Database:", DB_PATH)
    print("Source table:", source_table)
    print("Parquet root:", PARQUET_ROOT)
    raise SystemExit(0)

for date_str, category_id in slices_to_export:
    partition_dir = PARQUET_ROOT / f"date={date_str}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    # Legacy files are named data.parquet (category 3 only); new slices get one file
    # per category so a later category never rewrites an existing file.
    out_file = partition_dir / f"category-{category_id}.parquet"
    con.execute(f"""
    COPY (
        SELECT *
        FROM {source_table}
        WHERE date = DATE '{date_str}'
          AND categoryId = {category_id}
    ) TO '{out_file.as_posix()}'
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

con.close()

print("Export complete")
print("Database:", DB_PATH)
print("Source table:", source_table)
print("Parquet root:", PARQUET_ROOT)
print("Slices exported:", len(slices_to_export))
print("First slice exported:", slices_to_export[0])
print("Last slice exported:", slices_to_export[-1])
