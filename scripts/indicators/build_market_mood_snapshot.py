"""Market mood snapshot: the daily breadth counts behind the Market mood gauge.

Reads the series and product signal snapshots (so it runs after build_series_snapshot.py) and writes one row per
day to <slug>_market_mood_snapshot. The counting rules live in scripts/common/market_mood.py,
shared with the website's fallback for when this table hasn't been built yet. The table
also carries the Track record's market baseline (the fwd* columns). About 35
seconds for the English market.
"""

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common.category_config import get_category_config
from scripts.common.duckdb_settings import connect_capped
from scripts.common.market_mood import mood_sql, score_rows

DB_PATH = "/app/data/processed/prices_db.duckdb"


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the market mood snapshot for a category.")
    parser.add_argument("--category-id", type=int, default=3, help="Category ID. Default: 3 (Pokemon).")
    args = parser.parse_args()
    category = get_category_config(args.category_id)

    started = time.time()
    con = connect_capped(DB_PATH)
    con.execute(
        f"CREATE OR REPLACE TABLE {category.market_mood_snapshot_table} AS "
        + mood_sql(category.series_snapshot_table, category.product_signal_table, category.category_id)
    )
    cur = con.execute(f"SELECT * FROM {category.market_mood_snapshot_table} ORDER BY date")
    cols = [d[0] for d in cur.description]
    readings = score_rows([dict(zip(cols, row)) for row in cur.fetchall()])
    con.close()

    print(f"Category: {category.label} ({category.category_id})")
    print("DuckDB table:", category.market_mood_snapshot_table)
    print(f"Days: {len(readings)}; built in {time.time() - started:.1f}s")
    if readings:
        latest = readings[-1]
        print(f"Latest: {latest['date']} mood {latest['mood']} (above avg {latest['above_avg']}%, "
              f"rising {latest['rising']}%, new highs {latest['new_highs']}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
