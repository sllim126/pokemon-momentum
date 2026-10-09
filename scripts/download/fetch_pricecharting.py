"""Download PriceCharting's daily Pokemon price CSV and add it to the graded-price history.

PriceCharting only publishes current prices (no history), so each nightly snapshot
is kept: the raw CSV (gzipped) under data/pricecharting/raw/ and the matched rows in
the DuckDB table `pricecharting_prices`, one row per (snapshot_date, productId,
subTypeName). Everything PriceCharting-derived lives in that directory and table so it
can be deleted in one step if the subscription ends.

The token comes from PRICECHARTING_API_TOKEN in the environment or the repo's .env.
"""

import argparse
import csv
import gzip
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common.category_config import get_category_config
from scripts.common.pricecharting import match_rows, parse_row

DATA_DIR = Path("/app/data")
RAW_DIR = DATA_DIR / "pricecharting" / "raw"
DB_PATH = DATA_DIR / "processed" / "prices_db.duckdb"
EXTRACTED_DIR = DATA_DIR / "extracted"
TABLE = "pricecharting_prices"
CSV_URL = "https://www.pricecharting.com/price-guide/download-custom"
USER_AGENT = "Poke6sMarket/1.0.0"
CATEGORY_IDS = (3, 85)

PRICE_FIELDS = ["ungraded", "grade7", "grade8", "grade9", "grade9_5", "psa10", "bgs10", "cgc10", "sgc10"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", help="Snapshot date to record (default: today, UTC).")
    parser.add_argument("--reload", action="store_true", help="Re-match an existing raw file without downloading.")
    return parser.parse_args()


def api_token() -> str:
    token = os.getenv("PRICECHARTING_API_TOKEN", "").strip()
    env_file = ROOT / ".env"
    if not token and env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, _, value = line.partition("=")
            if key.strip() == "PRICECHARTING_API_TOKEN":
                token = value.strip().strip("'\"")
    return token


def download(raw_path: Path) -> None:
    token = api_token()
    if not token:
        raise SystemExit("PRICECHARTING_API_TOKEN is not set; skipping PriceCharting download.")
    response = requests.get(
        CSV_URL,
        params={"t": token, "category": "pokemon-cards"},
        headers={"User-Agent": USER_AGENT},
        timeout=600,
    )
    if response.status_code != 200 or not response.content.startswith(b"id,"):
        # Never echo the URL: it contains the token.
        raise SystemExit(f"PriceCharting download failed: HTTP {response.status_code}, {len(response.content)} bytes")
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = raw_path.with_suffix(".tmp")
    with gzip.open(tmp_path, "wb") as f:
        f.write(response.content)
    tmp_path.rename(raw_path)
    print(f"Downloaded {len(response.content):,} bytes -> {raw_path}")


def tracked_subtypes(con: duckdb.DuckDBPyConnection) -> dict[int, set[str]]:
    """Printings we have prices for, per TCGplayer product, across English and Japanese."""
    tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    subtypes: dict[int, set[str]] = defaultdict(set)
    for category_id in CATEGORY_IDS:
        category = get_category_config(category_id)
        source = category.product_signal_table
        if source not in tables:
            csv_path = EXTRACTED_DIR / category.product_signal_csv
            if not csv_path.exists():
                continue
            source = f"read_csv_auto('{csv_path}')"
        for product_id, subtype in con.execute(f"SELECT DISTINCT productId, subTypeName FROM {source}").fetchall():
            subtypes[int(product_id)].add(subtype)
    return subtypes


def main() -> int:
    args = parse_args()
    snapshot_date = args.date or datetime.now(timezone.utc).date().isoformat()
    raw_path = RAW_DIR / f"pokemon-cards-{snapshot_date}.csv.gz"

    if raw_path.exists():
        print(f"Raw snapshot for {snapshot_date} already downloaded; PriceCharting asks for one pull a day.")
    elif args.reload:
        print(f"ERROR: --reload given but {raw_path} does not exist.")
        return 1
    else:
        download(raw_path)

    with gzip.open(raw_path, "rt", encoding="utf-8", newline="") as f:
        rows = [row for row in (parse_row(r) for r in csv.DictReader(f)) if row]

    con = duckdb.connect(str(DB_PATH))
    try:
        matched, stats = match_rows(rows, tracked_subtypes(con))
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                snapshot_date DATE,
                productId BIGINT,
                subTypeName VARCHAR,
                pc_id BIGINT,
                set_name VARCHAR,
                product_name VARCHAR,
                printing_tag VARCHAR,
                {", ".join(f"{field} DOUBLE" for field in PRICE_FIELDS)},
                sales_volume BIGINT,
                release_date VARCHAR
            )
            """
        )
        con.execute(f"DELETE FROM {TABLE} WHERE snapshot_date = ?", [snapshot_date])
        columns = ["productId", "subTypeName", "pc_id", "set_name", "product_name", "printing_tag", *PRICE_FIELDS, "sales_volume", "release_date"]
        # One bulk insert from a DataFrame; row-by-row executemany takes minutes in DuckDB.
        frame = pd.DataFrame([{c: row[c] for c in columns} for row in matched], columns=columns)
        frame.insert(0, "snapshot_date", pd.Timestamp(snapshot_date).date())
        con.register("pc_snapshot", frame)
        try:
            con.execute(f"INSERT INTO {TABLE} (snapshot_date, {', '.join(columns)}) SELECT snapshot_date, {', '.join(columns)} FROM pc_snapshot")
        finally:
            con.unregister("pc_snapshot")
        total_dates = con.execute(f"SELECT COUNT(DISTINCT snapshot_date) FROM {TABLE}").fetchone()[0]
    finally:
        con.close()

    print(f"PriceCharting {snapshot_date}: {stats}")
    print(f"Stored {len(matched):,} matched printings; history now covers {total_dates} day(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
