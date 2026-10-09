"""Fetch the current TCGCSV price snapshot group-by-group for one category.

TCGCSV removed its daily `prices-YYYY-MM-DD.ppmd.7z` archive in September 2026 and
asks consumers to request categories, groups, and prices individually instead.
This script writes the per-group responses into the same layout the archive used
to extract to, so the DuckDB loader and everything downstream stay unchanged:

    /app/data/extracted/prices-<date>.ppmd/<date>/<categoryId>/<groupId>/prices

The snapshot date comes from https://tcgcsv.com/last-updated.txt (TCGCSV refreshes
once a day around 20:00 UTC). Each category is downloaded into a staging folder
and only moved into place once every group succeeded, because the loader treats a
date as fully loaded the first time it sees rows for that category.
"""

import argparse
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.utilities.tcgcsv_client import (
    TcgcsvBlockedError,
    build_tcgcsv_session,
    tcgcsv_get,
    tcgcsv_get_json,
)

DATA_DIR = Path("/app/data")
EXTRACT_ROOT = DATA_DIR / "extracted"
STAGING_ROOT = DATA_DIR / "staging"
MAX_SNAPSHOT_AGE_HOURS = 48


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download today's TCGCSV prices per group into the extracted archive layout."
    )
    parser.add_argument("--category-id", type=int, default=3, help="TCGplayer categoryId. Default: 3 (Pokemon).")
    return parser.parse_args()


def fetch_snapshot_time(session) -> datetime:
    resp = tcgcsv_get(session, "/last-updated.txt")
    return datetime.strptime(resp.text.strip(), "%Y-%m-%dT%H:%M:%S%z")


def download_category(session, category_id: int) -> int:
    snapshot_time = fetch_snapshot_time(session)
    age_hours = (datetime.now(timezone.utc) - snapshot_time).total_seconds() / 3600
    date_str = snapshot_time.astimezone(timezone.utc).date().isoformat()
    print(f"TCGCSV last updated: {snapshot_time.isoformat()} ({age_hours:.1f}h ago) -> price date {date_str}")
    if age_hours > MAX_SNAPSHOT_AGE_HOURS:
        print(f"ERROR: TCGCSV data is older than {MAX_SNAPSHOT_AGE_HOURS}h; refusing to treat it as a new day.")
        return 2

    final_dir = EXTRACT_ROOT / f"prices-{date_str}.ppmd" / date_str / str(category_id)
    if final_dir.exists():
        # TCGCSV asks clients not to request the same price file more than once a day.
        print(f"SKIP: {final_dir} already downloaded.")
        return 0

    groups_payload = tcgcsv_get_json(session, f"/tcgplayer/{category_id}/groups")
    if not groups_payload or not groups_payload.get("success"):
        print(f"ERROR: could not list groups for category {category_id}: {groups_payload}")
        return 1
    group_ids = sorted({int(group["groupId"]) for group in groups_payload["results"]})
    print(f"Groups to fetch: {len(group_ids)}")

    staging_dir = STAGING_ROOT / f"prices-{date_str}-{category_id}"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True)

    started = time.time()
    groups_with_prices = 0
    price_rows = 0
    try:
        for index, group_id in enumerate(group_ids, start=1):
            resp = tcgcsv_get(session, f"/tcgplayer/{category_id}/{group_id}/prices")
            if resp is None:
                continue
            payload = resp.json()
            if not payload.get("success"):
                raise RuntimeError(f"group {group_id} returned errors: {payload.get('errors')}")
            group_dir = staging_dir / str(group_id)
            group_dir.mkdir()
            # Store the body exactly as served, matching the old archive's files.
            (group_dir / "prices").write_bytes(resp.content)
            results = payload.get("results", [])
            groups_with_prices += 1 if results else 0
            price_rows += len(results)
            if index % 100 == 0:
                print(f"  {index}/{len(group_ids)} groups fetched")
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    print(
        f"Fetched {len(group_ids)} groups in {time.time() - started:.1f}s: "
        f"{groups_with_prices} with prices, {price_rows} price rows"
    )
    if not price_rows:
        print("ERROR: no price rows returned; not publishing an empty day.")
        shutil.rmtree(staging_dir)
        return 1

    final_dir.parent.mkdir(parents=True, exist_ok=True)
    staging_dir.rename(final_dir)
    print(f"Published {final_dir}")
    return 0


def main() -> int:
    args = parse_args()
    try:
        return download_category(build_tcgcsv_session(), args.category_id)
    except TcgcsvBlockedError as exc:
        print(f"ERROR: {exc}")
        print("TCGCSV is blocking or throttling us. Do not retry; check https://tcgcsv.com/docs and the TCGCSV Discord.")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
