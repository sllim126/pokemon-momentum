#!/usr/bin/env python3
"""Fill in one game's daily price files from the stored archives, for a history backfill.

The price loader reads data/extracted/prices-<date>.ppmd/<date>/<categoryId>/<groupId>/prices.
Days unpacked from TCGCSV's daily archive already hold every game, but the days fetched set
by set (from 2026-10-08, when the archive was offline) hold only Pokemon (3 and 85). This
copies one category into those days from whichever stored copy has it:

1. data/raw/prices-<date>.ppmd.7z, TCGCSV's daily archive (every game), or
2. data/tcgcsv_archive/<date>/tcgcsv-<date>-<categoryId>.tar.gz, the collect-only job's
   copy (scripts/download/collect_tcgcsv_categories.py; Lorcana from 2026-10-11).

Rules:
- Only days whose folder prices-<date>.ppmd/<date>/ already exists are filled. A folder
  holding only another game would make backfill_tcgcsv_archive.py think the day was
  already unpacked and skip Pokemon for it. Days with no folder are listed, not created.
- Days that already have the category folder are skipped, so re-running is safe.
- Each day is unpacked into a temporary folder and moved into place only when complete,
  so an interrupted run never leaves half a day (the loader would not reload it).
- Local files only: nothing is fetched from TCGCSV and DuckDB is not touched.

Written for Lorcana (category 71, docs/lorcana_market_plan.md). On 2026-11-14 the 7z
archives gave 1,007 days (under a second and about 600 KB each). Run inside the container
(7z is there), then load the new days:

    python scripts/extract/extract_category_from_archives.py --category-id 71
    python scripts/extract/build_pokemon_prices_all_days.py --category-id 71

Exit codes: 0 = done (including nothing to do), 1 = at least one day failed to unpack.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

DATA_DIR = Path("/app/data")
ARCHIVES_DIR = DATA_DIR / "raw"
COLLECT_DIR = DATA_DIR / "tcgcsv_archive"
EXTRACT_ROOT = DATA_DIR / "extracted"
ARCHIVE_NAME = re.compile(r"^prices-(\d{4}-\d{2}-\d{2})\.ppmd\.7z$")
DAY_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fill one category's daily price files from the stored archives.")
    parser.add_argument("--category-id", type=int, required=True, help="TCGplayer categoryId, e.g. 71 for Lorcana.")
    parser.add_argument("--start-date", help="Only days on or after YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Only days on or before YYYY-MM-DD.")
    parser.add_argument("--archives-dir", type=Path, default=ARCHIVES_DIR, help=argparse.SUPPRESS)
    parser.add_argument("--collect-dir", type=Path, default=COLLECT_DIR, help=argparse.SUPPRESS)
    parser.add_argument("--extract-root", type=Path, default=EXTRACT_ROOT, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def in_range(day: str, start: str | None, end: str | None) -> bool:
    return not ((start and day < start) or (end and day > end))


def available_sources(args: argparse.Namespace) -> dict[str, tuple[str, Path]]:
    """day -> ("7z" | "tar", path). The daily archive wins when both exist (it's TCGCSV's own)."""
    sources: dict[str, tuple[str, Path]] = {}
    if args.collect_dir.is_dir():
        for day_dir in args.collect_dir.iterdir():
            tar = day_dir / f"tcgcsv-{day_dir.name}-{args.category_id}.tar.gz"
            if DAY_NAME.match(day_dir.name) and tar.is_file() and tar.stat().st_size > 0:
                sources[day_dir.name] = ("tar", tar)
    for path in args.archives_dir.glob("prices-*.ppmd.7z"):
        match = ARCHIVE_NAME.match(path.name)
        # Empty files exist for days the archive download failed (2026-09-07 and 09-08).
        if match and path.stat().st_size > 0:
            sources[match.group(1)] = ("7z", path)
    return {d: s for d, s in sources.items() if in_range(d, args.start_date, args.end_date)}


def day_dir(extract_root: Path, day: str) -> Path:
    return extract_root / f"prices-{day}.ppmd" / day


def unpack_7z(archive: Path, day: str, category_id: int, tmp_root: Path) -> Path | None:
    """Unpack <day>/<category>/ from a daily archive; returns the unpacked folder."""
    result = subprocess.run(
        ["7z", "x", "-y", f"-o{tmp_root}", str(archive), f"{day}/{category_id}/*", "-r"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip()[:200])
    return tmp_root / day / str(category_id)


def unpack_tar(tar_path: Path, category_id: int, tmp_root: Path) -> Path | None:
    """Unpack only the <category>/<group>/prices files from a collect-only tarball."""
    wanted = re.compile(rf"^{category_id}/\d+/prices$")
    with tarfile.open(tar_path, "r:gz") as tar:
        members = [m for m in tar.getmembers() if m.isfile() and wanted.match(m.name)]
        # filter="data" refuses links and paths outside tmp_root.
        tar.extractall(tmp_root, members=members, filter="data")
    return tmp_root / str(category_id)


def fill_day(day: str, source: tuple[str, Path], category_id: int, extract_root: Path) -> bool:
    """Copy one day's category folder into place. Returns False on failure."""
    kind, path = source
    tmp_root = extract_root / f".tmp-prices-{day}-{category_id}"
    shutil.rmtree(tmp_root, ignore_errors=True)
    try:
        unpacked = unpack_7z(path, day, category_id, tmp_root) if kind == "7z" else unpack_tar(path, category_id, tmp_root)
        if unpacked and unpacked.is_dir():
            unpacked.rename(day_dir(extract_root, day) / str(category_id))
        # No folder: the archive is from before the game existed. Not an error.
        return True
    except (RuntimeError, OSError, tarfile.TarError) as exc:
        print(f"FAILED {day} ({path.name}): {exc}")
        return False
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    sources = available_sources(args)
    todo, no_folder, done = [], [], 0
    for day in sorted(sources):
        if (day_dir(args.extract_root, day) / str(args.category_id)).is_dir():
            done += 1
        elif not day_dir(args.extract_root, day).is_dir():
            no_folder.append(day)
        else:
            todo.append(day)
    print(f"Category {args.category_id}: {len(sources)} stored days, {done} already filled, {len(todo)} to fill")
    if no_folder:
        print(f"Left alone (no day folder yet, so Pokemon isn't there either): {', '.join(no_folder)}")

    failures = 0
    for index, day in enumerate(todo, start=1):
        if not fill_day(day, sources[day], args.category_id, args.extract_root):
            failures += 1
        if index % 100 == 0:
            print(f"  {index}/{len(todo)} done (up to {day})")
    print(f"Finished: {len(todo) - failures} days filled, {failures} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
