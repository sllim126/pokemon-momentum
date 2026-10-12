"""Collect-only daily archive of TCGCSV prices for games the site doesn't show yet.

TCGCSV retired its daily price archive in September 2026, so a game's price history only
exists from the day we start saving it. This job saves one day of TCGCSV data per game
(Magic, Yu-Gi-Oh, One Piece, Lorcana by default) exactly as served, so a future site for
those games can be built on real history. Nothing here touches DuckDB or the website.

For each game and day it writes one compressed file:

    /app/data/tcgcsv_archive/<date>/tcgcsv-<date>-<categoryId>.tar.gz

containing, byte-for-byte as TCGCSV returned them:

    <categoryId>/groups                  the game's set list (every run)
    <categoryId>/<groupId>/prices        each set's prices (every run)
    <categoryId>/<groupId>/products      each set's product names and details (Sundays,
                                         and the first time a game is collected)

The prices layout matches the old archive and `fetch_tcgcsv_prices.py`, so a game can later
be loaded with the same tools. TCGCSV etiquette is kept: each file at most once a day
(a day that is already archived is skipped), requests spaced by `tcgcsv_client`, and the
whole run stops on 403/429.

Run nightly by scripts/pipeline/run_tcgcsv_collect_host.sh. Manual run (in the container):

    python scripts/download/collect_tcgcsv_categories.py                  # default games
    python scripts/download/collect_tcgcsv_categories.py --category-ids 71 --products

Exit codes: 0 every game archived (or already archived today), 1 at least one game failed,
2 TCGCSV data is stale, 3 TCGCSV blocked/throttled us (do not retry until investigated).
"""

import argparse
import json
import shutil
import sys
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.download.fetch_tcgcsv_prices import MAX_SNAPSHOT_AGE_HOURS, fetch_snapshot_time
from scripts.utilities.tcgcsv_client import TcgcsvBlockedError, build_tcgcsv_session, tcgcsv_get

# TCGplayer category ids (confirmed against https://tcgcsv.com/tcgplayer/categories on
# 2026-10-12). Pokemon (3) and Pokemon Japan (85) are not here: the nightly pipeline
# already downloads and loads them.
COLLECT_CATEGORIES = {
    1: "Magic: The Gathering",
    2: "Yu-Gi-Oh",
    68: "One Piece Card Game",
    71: "Disney Lorcana",
}

ARCHIVE_ROOT = Path("/app/data/tcgcsv_archive")
STAGING_ROOT = Path("/app/data/staging")
PRODUCTS_WEEKDAY = 6  # Sunday: product lists change slowly, so refresh them weekly


def parse_args() -> argparse.Namespace:
    """Which games to collect, and whether to force a product-list download."""
    parser = argparse.ArgumentParser(description="Archive one day of TCGCSV data per game (collect only).")
    parser.add_argument(
        "--category-ids", type=int, nargs="+", default=list(COLLECT_CATEGORIES),
        help=f"TCGplayer category ids. Default: {' '.join(str(c) for c in COLLECT_CATEGORIES)}.",
    )
    parser.add_argument("--products", action="store_true", help="Also download product lists, whatever the day.")
    return parser.parse_args()


def archive_path(date_str: str, category_id: int) -> Path:
    """Where one game's day is archived."""
    return ARCHIVE_ROOT / date_str / f"tcgcsv-{date_str}-{category_id}.tar.gz"


def has_product_lists(category_id: int) -> bool:
    """True once any archived day for this game includes product lists."""
    return any(ARCHIVE_ROOT.glob(f"*/tcgcsv-*-{category_id}.products"))


def save(session, path: str, target: Path) -> bytes | None:
    """GET one TCGCSV file and write its body unchanged; None when TCGCSV has no such file."""
    resp = tcgcsv_get(session, path)
    if resp is None:
        return None
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(resp.content)
    return resp.content


def collect_category(session, category_id: int, date_str: str, want_products: bool) -> bool:
    """Download one game's day into staging, then pack it into its archive file.

    Returns True when the day is archived (now or earlier). The archive is written under a
    temporary name and renamed at the end, so a half-finished run never looks complete.
    """
    name = COLLECT_CATEGORIES.get(category_id, f"category {category_id}")
    final = archive_path(date_str, category_id)
    if final.exists():
        print(f"SKIP {category_id} {name}: {date_str} already archived.")
        return True
    with_products = want_products or not has_product_lists(category_id)

    staging = STAGING_ROOT / f"tcgcsv-collect-{date_str}-{category_id}"
    shutil.rmtree(staging, ignore_errors=True)
    started = time.time()
    try:
        groups_body = save(session, f"/tcgplayer/{category_id}/groups", staging / str(category_id) / "groups")
        groups = json.loads(groups_body or b"{}")
        if not groups.get("success"):
            print(f"ERROR {category_id} {name}: could not list groups: {groups.get('errors')}")
            return False
        group_ids = sorted({int(g["groupId"]) for g in groups["results"]})
        print(f"{category_id} {name}: {len(group_ids)} groups{' (with product lists)' if with_products else ''}")

        price_rows = 0
        for index, group_id in enumerate(group_ids, start=1):
            body = save(session, f"/tcgplayer/{category_id}/{group_id}/prices",
                        staging / str(category_id) / str(group_id) / "prices")
            if body:
                price_rows += len(json.loads(body).get("results", []))
            if with_products:
                save(session, f"/tcgplayer/{category_id}/{group_id}/products",
                     staging / str(category_id) / str(group_id) / "products")
            if index % 200 == 0:
                print(f"  {index}/{len(group_ids)} groups")
        if not price_rows:
            print(f"ERROR {category_id} {name}: no price rows returned; not archiving an empty day.")
            return False

        final.parent.mkdir(parents=True, exist_ok=True)
        partial = final.with_name(final.name + ".partial")
        with tarfile.open(partial, "w:gz") as tar:
            tar.add(staging / str(category_id), arcname=str(category_id))
        partial.rename(final)
        if with_products:
            # Marker so later runs know this game's product lists exist (see has_product_lists).
            final.with_name(f"tcgcsv-{date_str}-{category_id}.products").touch()
        size_mb = final.stat().st_size / 1_000_000
        print(f"ARCHIVED {category_id} {name}: {len(group_ids)} groups, {price_rows:,} price rows, "
              f"{size_mb:.1f} MB in {time.time() - started:.0f}s -> {final}")
        return True
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def main() -> int:
    """Archive every requested game; one game failing doesn't stop the others, a block does."""
    args = parse_args()
    session = build_tcgcsv_session()
    try:
        snapshot_time = fetch_snapshot_time(session)
        age_hours = (datetime.now(timezone.utc) - snapshot_time).total_seconds() / 3600
        date_str = snapshot_time.astimezone(timezone.utc).date().isoformat()
        print(f"TCGCSV last updated: {snapshot_time.isoformat()} ({age_hours:.1f}h ago) -> price date {date_str}")
        if age_hours > MAX_SNAPSHOT_AGE_HOURS:
            print(f"ERROR: TCGCSV data is older than {MAX_SNAPSHOT_AGE_HOURS}h; not archiving a repeat day.")
            return 2
        want_products = args.products or datetime.now(timezone.utc).weekday() == PRODUCTS_WEEKDAY
        results = []
        for category_id in args.category_ids:
            try:
                results.append(collect_category(session, category_id, date_str, want_products))
            except TcgcsvBlockedError:
                raise
            except Exception as exc:  # noqa: BLE001 - report and move on to the next game
                print(f"ERROR {category_id}: {exc}")
                results.append(False)
        return 0 if all(results) else 1
    except TcgcsvBlockedError as exc:
        print(f"ERROR: {exc}")
        print("TCGCSV is blocking or throttling us. Do not retry; check https://tcgcsv.com/docs and the TCGCSV Discord.")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
