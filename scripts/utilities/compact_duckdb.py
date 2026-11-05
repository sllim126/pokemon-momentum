"""Write a compacted copy of a DuckDB database, optionally leaving some tables out.

DuckDB never shrinks its file: a dropped or replaced table leaves free blocks that later
writes reuse, but the file stays as large as it ever was. The nightly update replaces every
snapshot table each night, so prices_db.duckdb carries dead space, and dropping old tables
alone would not give any disk back. Copying the live tables into a fresh file does.

The copy keeps each table's own CREATE TABLE statement (column types, NOT NULL) and its
indexes, which are created after the rows are in (faster than inserting into an index).
Every table's row count is compared with the source before the script reports success.

It only reads the source, so it is safe while the site has the database open read-only,
but not while the nightly update is writing (02:00 UTC). Swapping the new file in is a
separate, manual step (docs/operator_runbook.txt, "Compacting the price database").

    python scripts/utilities/compact_duckdb.py \\
        --source /app/data/backups/prices_db_before_cleanup_2026-11-05.duckdb \\
        --dest /app/data/processed/prices_db.compact.duckdb \\
        --drop prices --drop groups

Exit codes: 0 copied and verified, 1 a row count differs or a --drop table doesn't exist.
"""

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common.duckdb_settings import connect_capped


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Copy a DuckDB database into a fresh, compact file.")
    parser.add_argument("--source", required=True, help="Database to copy (opened read-only).")
    parser.add_argument("--dest", required=True, help="New database file; must not exist yet.")
    parser.add_argument("--drop", action="append", default=[], metavar="TABLE",
                        help="A table to leave out of the copy. Repeat for several.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dest = Path(args.dest)
    if dest.exists():
        print(f"{dest} already exists; remove it or pick another name.")
        return 1

    con = connect_capped(str(dest))
    con.execute(f"ATTACH '{args.source}' AS src (READ_ONLY)")
    tables = dict(con.execute(
        "SELECT table_name, sql FROM duckdb_tables() WHERE database_name = 'src' ORDER BY table_name"
    ).fetchall())
    missing = sorted(set(args.drop) - set(tables))
    if missing:
        print(f"Not in the source, check the names: {', '.join(missing)}")
        return 1
    keep = [t for t in tables if t not in args.drop]
    indexes = [(table, sql) for table, sql in con.execute(
        "SELECT table_name, sql FROM duckdb_indexes() WHERE database_name = 'src' ORDER BY index_name"
    ).fetchall() if table in keep]

    for table in keep:
        started = time.time()
        # duckdb_tables().sql has no database prefix, so this creates the table in dest.
        con.execute(tables[table])
        con.execute(f'INSERT INTO "{table}" SELECT * FROM src."{table}"')
        print(f"Copied {table} ({time.time() - started:.1f}s)")
    for table, sql in indexes:
        started = time.time()
        con.execute(sql)
        print(f"Indexed {table} ({time.time() - started:.1f}s)")

    bad = []
    for table in keep:
        old = con.execute(f'SELECT COUNT(*) FROM src."{table}"').fetchone()[0]
        new = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        if old != new:
            bad.append(f"{table}: {old:,} in source, {new:,} in copy")
    con.execute("DETACH src")
    con.execute("CHECKPOINT")
    con.close()

    print(f"Left out: {', '.join(args.drop) or 'nothing'}")
    print(f"Kept {len(keep)} tables and {len(indexes)} indexes")
    print(f"Size: {Path(args.source).stat().st_size / 1e9:.2f} GB -> {dest.stat().st_size / 1e9:.2f} GB")
    if bad:
        print("ROW COUNTS DIFFER, do not use the copy:\n  " + "\n  ".join(bad))
        return 1
    print("Row counts match")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
