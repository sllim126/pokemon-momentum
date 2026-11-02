"""Crawl TCGCSV's catalogue (set lists and product lists) for every game, a slice a night.

The daily price archive (data/raw/prices-<date>.ppmd.7z, see backfill_tcgcsv_archive.py) holds
every game's prices but no names: which set a groupId is, which card a productId is. This job
fills that in, so the archive can be read for any game, not only Pokemon. Owner's request
(2026-11-02): download everything we can, inside TCGCSV's rules.

What it saves, byte-for-byte as TCGCSV serves it (gitignored, under data/):

    data/tcgcsv_catalogue/categories.json               every game
    data/tcgcsv_catalogue/<categoryId>/groups.json      that game's sets
    data/tcgcsv_catalogue/<categoryId>/<groupId>/products.json

TCGCSV's rules (https://tcgcsv.com/docs#usage-guidelines) and how this job keeps them:
- at most 10,000 requests a day, for everything we run: this job stops at --budget requests
  a day (default 4,000), next to the nightly Pokemon update and the other-games collector
  (about 2,000-4,000 together). The count is kept per UTC day in the state file, so a second
  run on the same day only spends what is left.
- at least 100 ms between requests, a named User-Agent, stop on 403/429: tcgcsv_client.
- each file at most once a day, and only again after --refresh-days (default 30).
- prices are not fetched: they come from the archive (one file a day for all games).

Games are crawled in the owner's priority order (Pokemon, Pokemon Japan, the games already
collected, then the rest by id). Run nightly by scripts/pipeline/run_tcgcsv_catalogue_host.sh.

    python scripts/download/crawl_tcgcsv_catalogue.py              # tonight's slice
    python scripts/download/crawl_tcgcsv_catalogue.py --dry-run    # what it would fetch

Prints "Requests today: N of B", "Saved <path>", "Catalogue: X of Y files ..." and, when
everything is fresh, "Catalogue complete". Exit codes: 0 ok (finished or out of budget for
today), 1 failed, 3 TCGCSV blocked or throttled us (do not retry until investigated).
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.utilities.tcgcsv_client import TcgcsvBlockedError, build_tcgcsv_session, tcgcsv_get

CATALOGUE_ROOT = ROOT / "data" / "tcgcsv_catalogue"
STATE_FILE = CATALOGUE_ROOT / "state.json"
DEFAULT_BUDGET = 4000
DEFAULT_REFRESH_DAYS = 30
# Owner's order (2026-10-19): Pokemon, Pokemon Japan, the games already collected nightly.
PRIORITY_CATEGORIES = [3, 85, 1, 2, 68, 71]


def parse_args() -> argparse.Namespace:
    """Nightly budget, refresh age, and dry-run."""
    parser = argparse.ArgumentParser(description="Crawl TCGCSV set and product lists, a slice a night.")
    parser.add_argument("--budget", type=int, default=DEFAULT_BUDGET,
                        help=f"Most requests this job makes per UTC day. Default {DEFAULT_BUDGET}.")
    parser.add_argument("--refresh-days", type=int, default=DEFAULT_REFRESH_DAYS,
                        help=f"Fetch a saved file again only after this many days. Default {DEFAULT_REFRESH_DAYS}.")
    parser.add_argument("--dry-run", action="store_true", help="Count what is due; make no requests.")
    return parser.parse_args()


def load_state() -> dict:
    """{"fetched": {path: "YYYY-MM-DD"}, "requests": {"YYYY-MM-DD": n}}."""
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"fetched": {}, "requests": {}}


def save_state(state: dict) -> None:
    """Write the state atomically, keeping only the last week of request counts."""
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=7)).isoformat()
    state["requests"] = {day: n for day, n in state["requests"].items() if day >= cutoff}
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1, sort_keys=True))
    tmp.replace(STATE_FILE)


def is_due(state: dict, rel_path: str, today: str, refresh_days: int) -> bool:
    """A file is due if never saved, or saved more than refresh_days ago (never twice a day)."""
    last = state["fetched"].get(rel_path)
    if last is None or not (CATALOGUE_ROOT / rel_path).exists():
        return True
    age = (datetime.fromisoformat(today) - datetime.fromisoformat(last)).days
    return age >= refresh_days


def read_results(rel_path: str) -> list[dict]:
    """The "results" list of a saved TCGCSV JSON file ([] if missing or unreadable)."""
    try:
        return json.loads((CATALOGUE_ROOT / rel_path).read_text()).get("results") or []
    except (OSError, ValueError):
        return []


def ordered_categories(categories: list[dict]) -> list[int]:
    """Category ids in the owner's priority order, then the rest by id."""
    ids = sorted(int(c["categoryId"]) for c in categories)
    return [c for c in PRIORITY_CATEGORIES if c in ids] + [c for c in ids if c not in PRIORITY_CATEGORIES]


class Crawler:
    """Fetches due files until the day's budget is spent; tracks progress in the state file."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.today = datetime.now(timezone.utc).date().isoformat()
        self.state = load_state()
        self.session = None if args.dry_run else build_tcgcsv_session()
        self.due = 0
        self.saved = 0
        self.handled = 0  # saved, or answered 404 (nothing to save)

    @property
    def used_today(self) -> int:
        return self.state["requests"].get(self.today, 0)

    def budget_left(self) -> bool:
        return self.used_today < self.args.budget

    def fetch(self, rel_path: str, url_path: str) -> bool:
        """Fetch one file if due and in budget. True if the file is now on disk (old or new)."""
        if not is_due(self.state, rel_path, self.today, self.args.refresh_days):
            return True
        self.due += 1
        if self.args.dry_run or not self.budget_left():
            return (CATALOGUE_ROOT / rel_path).exists()
        # Count the request before making it, so a crash can't under-count the day.
        self.state["requests"][self.today] = self.used_today + 1
        resp = tcgcsv_get(self.session, url_path)
        if resp is None:  # 404: TCGCSV has no such file (e.g. a set with no products yet)
            print(f"404 {url_path}")
            self.state["fetched"][rel_path] = self.today
            self.handled += 1
            return False
        out = CATALOGUE_ROOT / rel_path
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(".part")
        tmp.write_bytes(resp.content)
        tmp.replace(out)
        self.state["fetched"][rel_path] = self.today
        self.saved += 1
        self.handled += 1
        if self.saved % 250 == 0:
            save_state(self.state)  # keep progress if the run is interrupted
            print(f"  ... {self.saved} files saved, {self.used_today} requests today")
        return True

    def run(self) -> int:
        print(f"Requests today: {self.used_today} of {self.args.budget}")
        total_files = 1
        if self.fetch("categories.json", "/tcgplayer/categories"):
            categories = read_results("categories.json")
        else:
            categories = []
        for category_id in ordered_categories(categories):
            total_files += 1
            groups_path = f"{category_id}/groups.json"
            if not self.fetch(groups_path, f"/tcgplayer/{category_id}/groups"):
                continue
            for group in read_results(groups_path):
                total_files += 1
                group_id = int(group["groupId"])
                self.fetch(f"{category_id}/{group_id}/products.json", f"/tcgplayer/{category_id}/{group_id}/products")
        if not self.args.dry_run:
            save_state(self.state)
        # Unknown until each game's set list is saved, so the first nights under-count.
        print(f"Catalogue: {total_files - self.due + self.handled} of {total_files} files fresh; "
              f"{len(categories)} games; saved {self.saved} tonight; requests today: {self.used_today}")
        if self.due == self.handled and categories:
            print("Catalogue complete")
        elif self.args.dry_run:
            print(f"Dry run: {self.due} files due")
        else:
            print(f"Out of budget for today; {self.due - self.handled} files left for the next nights")
        return 0


def main() -> int:
    args = parse_args()
    crawler = Crawler(args)
    try:
        return crawler.run()
    except TcgcsvBlockedError as exc:
        print(f"STOPPED: {exc}")
        return 3
    finally:
        if not args.dry_run:
            save_state(crawler.state)


if __name__ == "__main__":
    raise SystemExit(main())
