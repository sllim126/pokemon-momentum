"""Build a torrent of TCGCSV's daily price archive for one year, one month or a run of months.

Owner's plan (2026-11-02): help TCGCSV with its bandwidth bill by seeding the archive as
torrents: one per finished year (2024, 2025, then 2026 on 2027-01-02) and one per month from
November 2026 on, each built on the 2nd of the following month. Owner, 2026-11-17: also a
2026-so-far torrent (January to October, --months 2026-01:2026-10), so anyone can take the whole
history from torrents and fetch only the few days since the newest monthly one from tcgcsv.com.

Each new torrent also carries the product list (owner, 2026-11-17; promised in #useful-shares
on 2026-11-11 after binaryalias asked, and CptSpaceToaster said go ahead): every game's sets and
products from our catalogue crawl (data/tcgcsv_catalogue/), as one tcgcsv-products-<date>.tar.xz
(~33 MB). The archive files carry prices by ID only, so this gives the names. --no-catalogue
leaves it out. 2024 and 2025 were built before this and don't have it. CptSpaceToaster OK'd sharing
them in the TCGCSV Discord's #useful-shares on 2026-11-02 and can withdraw that at any time
(TCGCSV passes on TCGplayer data with TCGplayer's permission). See docs/operator_runbook.txt "TCGCSV archive torrents".

For a period it makes, under data/torrents/ (gitignored):

    tcgcsv-archive-2024/                 the torrent's folder
        prices-2024-02-08.ppmd.7z ...    hard links to data/raw (no extra disk space)
        tcgcsv-products-<date>.tar.xz    the product list (sets and products, every game)
        README.txt                       what this is, where it's from, how to open it
        SHA256SUMS.txt                   a checksum per archive file
    tcgcsv-archive-2024.torrent
    tcgcsv-archive-2024.magnet.txt       the magnet link, for sharing later

Archive files never change once published, so a period's torrent never needs rebuilding.
A period with missing days is refused unless --allow-gaps (the README then lists them).

Needs libtorrent (`pip install libtorrent`; not in the site's container). Run on the host:

    python scripts/torrents/build_archive_torrent.py --year 2024
    python scripts/torrents/build_archive_torrent.py --month 2026-11
    python scripts/torrents/build_archive_torrent.py --months 2026-01:2026-10
"""

import argparse
import hashlib
import os
import sys
import tarfile
import warnings
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import libtorrent as lt

# libtorrent 2.1 marks the file_storage route as deprecated; it still works and is the
# simplest way to hash a folder, so don't print the warnings on every run.
warnings.filterwarnings("ignore", category=DeprecationWarning, module=__name__)

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
TORRENTS_DIR = ROOT / "data" / "torrents"
CATALOGUE_DIR = ROOT / "data" / "tcgcsv_catalogue"  # written by scripts/download/crawl_tcgcsv_catalogue.py
FIRST_ARCHIVE_DAY = date(2024, 2, 8)  # TCGCSV's archive starts here

# Free public trackers; peers also find each other through DHT. Trackers sit outside the
# torrent's info section, so changing them later doesn't change the magnet link's hash.
TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
]

README = """TCGCSV daily price archive: {label}
{underline}

One file per day, {first} to {last} ({count} days). Each file is TCGCSV's daily archive
(https://tcgcsv.com), byte-for-byte as published at
https://tcgcsv.com/archive/tcgplayer/prices-YYYY-MM-DD.ppmd.7z

What's inside a file: TCGplayer market, low, mid, high and direct-low prices for every
product in every game TCGplayer lists (Pokemon, Magic, Yu-Gi-Oh, Lorcana and ~90 more), as
JSON, laid out as <date>/<categoryId>/<groupId>/prices. Product and set names are not in
the archive; get them from https://tcgcsv.com/tcgplayer/<categoryId>/groups and
.../<groupId>/products.

Open with 7-Zip (7z x prices-2024-02-08.ppmd.7z); the files use PPMd compression.
Check a download with: sha256sum -c SHA256SUMS.txt
{gaps}{catalogue}
Prices are TCGplayer data, shared by TCGCSV with TCGplayer's permission. Credit TCGCSV and
TCGplayer if you use them, and please follow TCGCSV's usage guidelines
(https://tcgcsv.com/docs#usage-guidelines) for anything you fetch from TCGCSV directly.
This torrent exists to take download load off TCGCSV: prefer it to fetching the whole
archive from tcgcsv.com. Torrents cover 2024 and 2025 (a year each), 2026 so far, and each
month after that, posted in the TCGCSV Discord's #useful-shares. Take those, then fetch only
the days after the newest torrent from tcgcsv.com, once each.

Packaged by Poke6s Market Watch.
"""


CATALOGUE_README = """
Product list: {name}
  Every game's sets and products as TCGCSV served them, fetched {first} to {last}
  (TCGCSV's archive files never held product data). Layout inside:
      tcgcsv_catalogue/categories.json                     every game
      tcgcsv_catalogue/<categoryId>/groups.json            that game's sets
      tcgcsv_catalogue/<categoryId>/<groupId>/products.json   that set's products
  Open with 7-Zip, or: tar -xJf {name}
  It is a starting point: sets and products added after {last} are missing, so top up from
  https://tcgcsv.com/tcgplayer/<categoryId>/groups and .../<groupId>/products. It holds no
  prices; those are the archive files.
"""


def build_catalogue(folder: Path) -> str:
    """Pack the catalogue crawl into one .tar.xz in the torrent folder; return its README part.

    xz rather than zstd: same size here (~33 MB from 655 MB) and 7-Zip on Windows opens it.
    Files go in sorted order so the same catalogue always packs the same way.
    """
    files = sorted(p for p in CATALOGUE_DIR.rglob("*.json") if p.name != "state.json")  # state = crawl bookkeeping
    if not files:
        raise SystemExit(f"No catalogue in {CATALOGUE_DIR}; run the catalogue crawl or use --no-catalogue.")
    days = sorted(datetime.fromtimestamp(p.stat().st_mtime, UTC).date() for p in files)
    name = f"tcgcsv-products-{days[-1].isoformat()}.tar.xz"
    for old in folder.glob("tcgcsv-products-*.tar.xz"):
        if old.name != name:
            old.unlink()  # a rebuild with a newer catalogue replaces the old one
    target = folder / name
    if not target.exists():
        partial = target.with_suffix(".partial")
        with tarfile.open(partial, "w:xz", preset=9) as tar:
            for path in files:
                info = tar.gettarinfo(path, arcname=str(Path("tcgcsv_catalogue") / path.relative_to(CATALOGUE_DIR)))
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                with path.open("rb") as handle:
                    tar.addfile(info, handle)
        partial.rename(target)
    return CATALOGUE_README.format(name=name, first=days[0], last=days[-1])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a torrent of TCGCSV archive files for a year or month.")
    period = parser.add_mutually_exclusive_group(required=True)
    period.add_argument("--year", type=int, help="e.g. 2024")
    period.add_argument("--month", help="YYYY-MM, e.g. 2026-11")
    period.add_argument("--months", help="FIRST:LAST months, YYYY-MM:YYYY-MM, e.g. 2026-01:2026-10")
    parser.add_argument("--allow-gaps", action="store_true", help="Build even if some days are missing.")
    parser.add_argument("--no-catalogue", action="store_true", help="Leave the product list out.")
    return parser.parse_args()


def month_end(year: int, month: int) -> date:
    """The last day of a month."""
    return date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1)


def period_days(args: argparse.Namespace) -> tuple[str, list[date]]:
    """The torrent's name and every day it should cover (never before the archive's first day)."""
    if args.year:
        start, end, name = date(args.year, 1, 1), date(args.year, 12, 31), f"tcgcsv-archive-{args.year}"
    elif args.month:
        year, month = (int(part) for part in args.month.split("-"))
        start, end = date(year, month, 1), month_end(year, month)
        name = f"tcgcsv-archive-{year}-{month:02d}"
    else:
        first, _, last = args.months.partition(":")
        (y1, m1), (y2, m2) = ((int(p) for p in part.split("-")) for part in (first, last))
        start, end = date(y1, m1, 1), month_end(y2, m2)
        if end < start:
            raise SystemExit(f"--months {args.months}: the last month is before the first.")
        # e.g. tcgcsv-archive-2026-01-to-10, or tcgcsv-archive-2025-11-to-2026-02 across a year
        name = (f"tcgcsv-archive-{y1}-{m1:02d}-to-{m2:02d}" if y1 == y2
                else f"tcgcsv-archive-{y1}-{m1:02d}-to-{y2}-{m2:02d}")
    start = max(start, FIRST_ARCHIVE_DAY)
    return name, [start + timedelta(days=i) for i in range((end - start).days + 1)]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    args = parse_args()
    name, days = period_days(args)
    files = {day: RAW_DIR / f"prices-{day.isoformat()}.ppmd.7z" for day in days}
    missing = [day for day, path in files.items() if not path.exists()]
    if missing and not args.allow_gaps:
        print(f"Refusing {name}: {len(missing)} day(s) missing, first {missing[0]}. Use --allow-gaps to build anyway.")
        return 1
    present = [day for day in days if day not in missing]

    folder = TORRENTS_DIR / name
    folder.mkdir(parents=True, exist_ok=True)
    sums = []
    for day in present:
        link = folder / files[day].name
        if not link.exists():
            os.link(files[day], link)  # same filesystem: no copy, no extra space
        sums.append(f"{sha256(link)}  {link.name}")
    catalogue = ""
    if args.no_catalogue:
        for old in folder.glob("tcgcsv-products-*.tar.xz"):
            old.unlink()
    else:
        catalogue = build_catalogue(folder)
        packed = next(folder.glob("tcgcsv-products-*.tar.xz"))
        sums.append(f"{sha256(packed)}  {packed.name}")
    (folder / "SHA256SUMS.txt").write_text("\n".join(sums) + "\n")
    gaps = f"\nMissing days (TCGCSV has no file for them): {', '.join(d.isoformat() for d in missing)}\n" if missing else ""
    label = name.removeprefix("tcgcsv-archive-")
    (folder / "README.txt").write_text(README.format(
        label=label, underline="=" * (len("TCGCSV daily price archive: ") + len(label)),
        first=present[0], last=present[-1], count=len(present), gaps=gaps, catalogue=catalogue))

    storage = lt.file_storage()
    lt.add_files(storage, str(folder))
    # v1-only: libtorrent 2.x makes hybrid v1+v2 torrents by default, which older clients
    # handle poorly and which carry two different info hashes.
    torrent = lt.create_torrent(storage, flags=lt.create_torrent.v1_only)
    for tier, tracker in enumerate(TRACKERS):
        torrent.add_tracker(tracker, tier)
    torrent.set_creator("Poke6s Market Watch")
    torrent.set_comment(f"TCGCSV daily price archive, {label} (https://tcgcsv.com)")
    lt.set_piece_hashes(torrent, str(folder.parent))
    torrent_path = TORRENTS_DIR / f"{name}.torrent"
    torrent_path.write_bytes(lt.bencode(torrent.generate()))

    info = lt.torrent_info(str(torrent_path))
    magnet = lt.make_magnet_uri(info)
    (TORRENTS_DIR / f"{name}.magnet.txt").write_text(magnet + "\n")
    size_mb = info.total_size() / 1e6
    print(f"Built {torrent_path.name}: {len(present)} days, {size_mb:,.0f} MB, {info.num_pieces()} pieces")
    print(f"Info hash: {info.info_hashes().v1}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
