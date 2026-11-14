"""Download the TCGCSV daily price archives we're missing, once TCGCSV serves them again.

TCGCSV took its daily archive (`/archive/tcgplayer/prices-YYYY-MM-DD.ppmd.7z`) offline
around 2026-09-16 and planned to bring it back. We already hold every file from
2024-02-08 to 2026-09-15 in data/raw. This script fetches the days after that (or any
range given), so that:

- the Pokemon price history gets the 2026-09-16 to 2026-10-07 gap filled (the per-group
  fetcher, fetch_tcgcsv_prices.py, took over on 2026-10-08), and
- data/raw stays a complete day-by-day copy of the archive (every game, every day).

TCGCSV's maintainer is fine with us downloading as needed as long as we follow the usage
rules, which scripts/utilities/tcgcsv_client.py enforces: one request at a time, at least
150 ms apart, our own User-Agent, and stop at once on HTTP 403 or 429. On top of that this
script asks for each file at most once per day (data/raw/backfill_attempts.json), so a
file that isn't published yet is not re-requested by a second run the same day.

Each downloaded file is checked with `7z t` and then extracted to
data/extracted/prices-<date>.ppmd/, the layout the price loader reads. A day that
already has an extracted folder (the per-group fetcher's days from 2026-10-08) keeps it:
only the .7z is stored for those. The next nightly update loads the new days into
DuckDB on its own (build_pokemon_prices_all_days.py loads any day it hasn't seen).

Run inside the container (7z and /app paths are there; DuckDB is not touched, so the
site can stay up):

    docker-compose exec -T pokemon-momentum python scripts/download/backfill_tcgcsv_archive.py --dry-run
    docker-compose exec -T pokemon-momentum python scripts/download/backfill_tcgcsv_archive.py

Exit codes: 0 = done (including "nothing to fetch"), 1 = a download or extract failed,
3 = TCGCSV answered 403/429 (blocked, or the archive is still offline): don't retry
until someone has looked.
"""

import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.utilities.tcgcsv_client import TcgcsvBlockedError, build_tcgcsv_session, tcgcsv_get

DATA_DIR = Path("/app/data")
ARCHIVES_DIR = DATA_DIR / "raw"
EXTRACT_ROOT = DATA_DIR / "extracted"
ATTEMPTS_FILE = ARCHIVES_DIR / "backfill_attempts.json"
FIRST_ARCHIVE_DAY = date(2024, 2, 8)  # TCGCSV's first archive file


def archive_name(day: date) -> str:
    return f"prices-{day.isoformat()}.ppmd.7z"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download missing TCGCSV daily price archives.")
    parser.add_argument("--start", type=date.fromisoformat,
                        help="First day to fetch (default: the day after the newest file in data/raw).")
    parser.add_argument("--end", type=date.fromisoformat,
                        help="Last day to fetch (default: yesterday, UTC; today's file may not exist yet).")
    parser.add_argument("--dry-run", action="store_true", help="List the days that would be fetched; no requests.")
    parser.add_argument("--no-extract", action="store_true", help="Only download and test the .7z files.")
    return parser.parse_args()


def default_start() -> date:
    """The day after the newest archive we hold."""
    days = sorted(p.name[len("prices-"):len("prices-") + 10] for p in ARCHIVES_DIR.glob("prices-*.ppmd.7z"))
    return date.fromisoformat(days[-1]) + timedelta(days=1) if days else FIRST_ARCHIVE_DAY


def load_attempts() -> dict[str, str]:
    try:
        return json.loads(ATTEMPTS_FILE.read_text())
    except (FileNotFoundError, ValueError):
        return {}


def save_attempts(attempts: dict[str, str]) -> None:
    ATTEMPTS_FILE.write_text(json.dumps(attempts, indent=1, sort_keys=True))


def seven_zip(*args: str) -> bool:
    return subprocess.run(["7z", *args], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE).returncode == 0


def main() -> int:
    """Fetch each missing archive day in the range (default: from the day after the newest archive we hold to yesterday).

    A day already asked for today is skipped, and the attempt is recorded before the request,
    so a file is never requested twice in one UTC day (TCGCSV's rule). --dry-run lists what it
    would fetch.
    """
    args = parse_args()
    start = args.start or default_start()
    end = args.end or (datetime.now(timezone.utc).date() - timedelta(days=1))
    today = datetime.now(timezone.utc).date().isoformat()
    attempts = load_attempts()

    wanted = []
    day = start
    while day <= end:
        if not (ARCHIVES_DIR / archive_name(day)).exists():
            wanted.append(day)
        day += timedelta(days=1)
    skipped_today = [d for d in wanted if attempts.get(d.isoformat()) == today]
    todo = [d for d in wanted if d not in skipped_today]
    print(f"Missing archive files {start} to {end}: {len(wanted)}; "
          f"already asked for today: {len(skipped_today)}; to fetch now: {len(todo)}")
    if args.dry_run or not todo:
        for d in todo:
            print(f"  would fetch {archive_name(d)}")
        return 0

    ARCHIVES_DIR.mkdir(parents=True, exist_ok=True)
    session = build_tcgcsv_session()
    fetched, not_published, failed = [], [], []
    for d in todo:
        name = archive_name(d)
        attempts[d.isoformat()] = today  # recorded before asking: one request per file per day
        save_attempts(attempts)
        try:
            resp = tcgcsv_get(session, f"/archive/tcgplayer/{name}")
        except TcgcsvBlockedError as exc:
            # The archive answered 403 the whole time it was offline, so the first 403 here
            # most likely means "not back yet". Either way: stop, no more requests.
            print(f"STOPPED at {name}: {exc}")
            print(f"Fetched {len(fetched)} file(s) before stopping.")
            return 3
        except RuntimeError as exc:
            print(f"FAILED {name}: {exc}")
            failed.append(name)
            continue
        if resp is None:
            print(f"not published yet: {name}")
            not_published.append(name)
            continue
        # Write to a temporary name and test it before it counts as held.
        partial = ARCHIVES_DIR / f"{name}.part"
        partial.write_bytes(resp.content)
        if not seven_zip("t", str(partial)):
            print(f"FAILED {name}: the download doesn't pass 7z t; deleted")
            partial.unlink(missing_ok=True)
            failed.append(name)
            continue
        partial.rename(ARCHIVES_DIR / name)
        print(f"OK {name} ({len(resp.content):,} bytes)")
        fetched.append(d)

    for d in [] if args.no_extract else fetched:
        dest = EXTRACT_ROOT / f"prices-{d.isoformat()}.ppmd"
        if dest.exists():
            print(f"kept existing extracted folder for {d} (per-group fetch); archive stored only")
            continue
        if seven_zip("x", str(ARCHIVES_DIR / archive_name(d)), f"-o{dest}", "-y"):
            print(f"extracted {d}")
        else:
            print(f"FAILED to extract {d}")
            failed.append(archive_name(d))

    print(f"Done: {len(fetched)} fetched, {len(not_published)} not published yet, {len(failed)} failed.")
    if fetched:
        print("The next nightly update loads the new days. To load now: stop the site, run "
              "build_pokemon_prices_all_days.py for categories 3 and 85, then the snapshots (see the runbook).")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
