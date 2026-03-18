import argparse
import os
import json
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

DATA_DIR = Path("/app/data/extracted")
EXTRACTED_ROOT = DATA_DIR
PROCESSED_DIR = Path("/app/data/processed")
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"

CATEGORY_ID = 3  # Pokemon
OUT_CSV = DATA_DIR / "pokemon_prices_all_days.csv"
TABLE_NAME = "pokemon_prices"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load extracted Pokemon price files into DuckDB incrementally."
    )
    parser.add_argument(
        "--start-date",
        help="Only process extracted folders on or after YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        help="Only process extracted folders on or before YYYY-MM-DD.",
    )
    parser.add_argument(
        "--limit-days",
        type=int,
        default=0,
        help="Only process up to N missing days, newest first when combined with --latest-first.",
    )
    parser.add_argument(
        "--latest-first",
        action="store_true",
        help="Process newest missing days first.",
    )
    parser.add_argument(
        "--refresh-csv",
        action="store_true",
        help="Rebuild pokemon_prices_all_days.csv from DuckDB after loading.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, os.cpu_count() or 1),
        help="Number of worker processes to use while parsing group price files.",
    )
    return parser.parse_args()


def parse_iso_date(value: str | None) -> str | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date().isoformat()


def to_float(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None


def get_loaded_dates(con: duckdb.DuckDBPyConnection) -> set[str]:
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    if TABLE_NAME not in tables:
        return set()
    return {
        str(row[0])
        for row in con.execute(f"SELECT DISTINCT date FROM {TABLE_NAME}").fetchall()
        if row[0] is not None
    }


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date DATE,
            categoryId INTEGER,
            groupId BIGINT,
            productId BIGINT,
            subTypeName VARCHAR,
            lowPrice DOUBLE,
            midPrice DOUBLE,
            highPrice DOUBLE,
            marketPrice DOUBLE,
            directLowPrice DOUBLE
        )
        """
    )
    con.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {TABLE_NAME}_uq
        ON {TABLE_NAME}(date, categoryId, groupId, productId, subTypeName)
        """
    )


def iter_extracted_days() -> list[tuple[str, Path]]:
    extracted_days: list[tuple[str, Path]] = []
    for day_pkg in sorted([p for p in EXTRACTED_ROOT.iterdir() if p.is_dir()]):
        date_folders = [p for p in day_pkg.iterdir() if p.is_dir() and p.name[:4].isdigit()]
        if not date_folders:
            continue
        date_dir = date_folders[0]
        extracted_days.append((date_dir.name, date_dir))
    return extracted_days


def parse_group_prices_file(args: tuple[str, str]) -> tuple[int, list[tuple]]:
    group_dir_str, date_str = args
    group_dir = Path(group_dir_str)
    prices_file = group_dir / "prices"
    if not prices_file.exists():
        return 0, []

    try:
        data = json.loads(prices_file.read_text(encoding="utf-8", errors="ignore"))
        results = data.get("results", [])
    except Exception as e:
        print(f"FAILED parsing: {prices_file}  Error: {e}")
        return 0, []

    try:
        group_id = int(group_dir.name)
    except Exception:
        return 0, []

    rows = [
        (
            date_str,
            CATEGORY_ID,
            group_id,
            item.get("productId"),
            item.get("subTypeName", ""),
            to_float(item.get("lowPrice")),
            to_float(item.get("midPrice")),
            to_float(item.get("highPrice")),
            to_float(item.get("marketPrice")),
            to_float(item.get("directLowPrice")),
        )
        for item in results
    ]
    return 1, rows


def select_target_days(
    extracted_days: list[tuple[str, Path]],
    loaded_dates: set[str],
    start_date: str | None,
    end_date: str | None,
    limit_days: int,
    latest_first: bool,
) -> list[tuple[str, Path]]:
    filtered: list[tuple[str, Path]] = []
    for date_str, date_dir in extracted_days:
        if start_date and date_str < start_date:
            continue
        if end_date and date_str > end_date:
            continue
        if date_str in loaded_dates:
            continue
        filtered.append((date_str, date_dir))

    if latest_first:
        filtered = list(reversed(filtered))
    if limit_days > 0:
        filtered = filtered[:limit_days]
    if latest_first:
        filtered = sorted(filtered)
    return filtered


def load_one_day(
    con: duckdb.DuckDBPyConnection,
    date_str: str,
    date_dir: Path,
    workers: int,
) -> tuple[int, int]:
    cat_dir = date_dir / str(CATEGORY_ID)
    if not cat_dir.exists():
        return 0, 0

    group_dirs = sorted([p for p in cat_dir.iterdir() if p.is_dir()])
    if not group_dirs:
        return 0, 0

    rows: list[tuple] = []
    files_read = 0

    tasks = [(str(group_dir), date_str) for group_dir in group_dirs]
    if workers <= 1:
        for task in tasks:
            file_count, parsed_rows = parse_group_prices_file(task)
            files_read += file_count
            rows.extend(parsed_rows)
    else:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for file_count, parsed_rows in pool.map(parse_group_prices_file, tasks, chunksize=8):
                files_read += file_count
                rows.extend(parsed_rows)

    if not rows:
        return files_read, 0

    df = pd.DataFrame.from_records(
        rows,
        columns=[
            "date",
            "categoryId",
            "groupId",
            "productId",
            "subTypeName",
            "lowPrice",
            "midPrice",
            "highPrice",
            "marketPrice",
            "directLowPrice",
        ],
    )
    con.register("day_rows_df", df)
    try:
        con.execute(
            f"""
            INSERT INTO {TABLE_NAME}
            SELECT
                CAST(date AS DATE),
                CAST(categoryId AS INTEGER),
                CAST(groupId AS BIGINT),
                CAST(productId AS BIGINT),
                CAST(subTypeName AS VARCHAR),
                CAST(lowPrice AS DOUBLE),
                CAST(midPrice AS DOUBLE),
                CAST(highPrice AS DOUBLE),
                CAST(marketPrice AS DOUBLE),
                CAST(directLowPrice AS DOUBLE)
            FROM day_rows_df
            """
        )
    finally:
        con.unregister("day_rows_df")
    return files_read, len(rows)


def refresh_csv(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM {TABLE_NAME}
            ORDER BY date, groupId, productId, subTypeName
        ) TO '{OUT_CSV}' WITH (HEADER, DELIMITER ',')
        """
    )


def main() -> int:
    args = parse_args()
    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB_PATH))
    con.execute(f"PRAGMA threads={max(1, args.workers)}")
    ensure_schema(con)
    loaded_dates = get_loaded_dates(con)
    extracted_days = iter_extracted_days()
    target_days = select_target_days(
        extracted_days,
        loaded_dates,
        start_date,
        end_date,
        args.limit_days,
        args.latest_first,
    )

    print("DuckDB database:", DB_PATH)
    print("DuckDB table:", TABLE_NAME)
    print("Extracted day folders found:", len(extracted_days))
    print("Dates already loaded:", len(loaded_dates))
    print("Dates queued this run:", len(target_days))

    if not target_days:
        print("No missing dates to load.")
        if args.refresh_csv:
            print("Refreshing CSV export from DuckDB...")
            refresh_csv(con)
            print("CSV refreshed:", OUT_CSV)
        con.close()
        return 0

    total_files = 0
    total_rows = 0

    for idx, (date_str, date_dir) in enumerate(target_days, start=1):
        print(f"[{idx}/{len(target_days)}] LOADING {date_str}")
        files_read, rows_inserted = load_one_day(con, date_str, date_dir, args.workers)
        total_files += files_read
        total_rows += rows_inserted
        print(f"[{idx}/{len(target_days)}] DONE {date_str} files={files_read} rows={rows_inserted}")

    if args.refresh_csv:
        print("Refreshing CSV export from DuckDB...")
        refresh_csv(con)
        print("CSV refreshed:", OUT_CSV)

    con.close()

    print("DONE")
    print("Dates processed:", len(target_days))
    print("Price files read:", total_files)
    print("Rows inserted this run:", total_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
