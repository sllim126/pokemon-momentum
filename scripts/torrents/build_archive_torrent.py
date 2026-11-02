"""Build a torrent of TCGCSV's daily price archive for one year or one month.

Owner's plan (2026-11-02): help TCGCSV with its bandwidth bill by seeding the archive as
torrents: one per finished year (2024, 2025, then 2026 on 2027-01-02) and one per month from
then on. CptSpaceToaster OK'd sharing them in the TCGCSV Discord's #useful-shares on
2026-11-02 and can withdraw that at any time (TCGCSV passes on TCGplayer data with
TCGplayer's permission). See docs/operator_runbook.txt "TCGCSV archive torrents".

For a period it makes, under data/torrents/ (gitignored):

    tcgcsv-archive-2024/                 the torrent's folder
        prices-2024-02-08.ppmd.7z ...    hard links to data/raw (no extra disk space)
        README.txt                       what this is, where it's from, how to open it
        SHA256SUMS.txt                   a checksum per archive file
    tcgcsv-archive-2024.torrent
    tcgcsv-archive-2024.magnet.txt       the magnet link, for sharing later

Archive files never change once published, so a period's torrent never needs rebuilding.
A period with missing days is refused unless --allow-gaps (the README then lists them).

Needs libtorrent (`pip install libtorrent`; not in the site's container). Run on the host:

    python scripts/torrents/build_archive_torrent.py --year 2024
    python scripts/torrents/build_archive_torrent.py --month 2026-11
"""

import argparse
import hashlib
import os
import sys
import warnings
from datetime import date, timedelta
from pathlib import Path

import libtorrent as lt

# libtorrent 2.1 marks the file_storage route as deprecated; it still works and is the
# simplest way to hash a folder, so don't print the warnings on every run.
warnings.filterwarnings("ignore", category=DeprecationWarning, module=__name__)

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
TORRENTS_DIR = ROOT / "data" / "torrents"
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
{gaps}
Prices are TCGplayer data, shared by TCGCSV with TCGplayer's permission. Credit TCGCSV and
TCGplayer if you use them, and please follow TCGCSV's usage guidelines
(https://tcgcsv.com/docs#usage-guidelines) for anything you fetch from TCGCSV directly.
This torrent exists to take download load off TCGCSV: prefer it to fetching the whole
archive from tcgcsv.com.

Packaged by Poke6s Market Watch.
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a torrent of TCGCSV archive files for a year or month.")
    period = parser.add_mutually_exclusive_group(required=True)
    period.add_argument("--year", type=int, help="e.g. 2024")
    period.add_argument("--month", help="YYYY-MM, e.g. 2026-11")
    parser.add_argument("--allow-gaps", action="store_true", help="Build even if some days are missing.")
    return parser.parse_args()


def period_days(args: argparse.Namespace) -> tuple[str, list[date]]:
    """The torrent's name and every day it should cover (never before the archive's first day)."""
    if args.year:
        start, end, name = date(args.year, 1, 1), date(args.year, 12, 31), f"tcgcsv-archive-{args.year}"
    else:
        year, month = (int(part) for part in args.month.split("-"))
        start = date(year, month, 1)
        end = (date(year + month // 12, month % 12 + 1, 1)) - timedelta(days=1)
        name = f"tcgcsv-archive-{year}-{month:02d}"
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
    (folder / "SHA256SUMS.txt").write_text("\n".join(sums) + "\n")
    gaps = f"\nMissing days (TCGCSV has no file for them): {', '.join(d.isoformat() for d in missing)}\n" if missing else ""
    label = name.removeprefix("tcgcsv-archive-")
    (folder / "README.txt").write_text(README.format(
        label=label, underline="=" * (len("TCGCSV daily price archive: ") + len(label)),
        first=present[0], last=present[-1], count=len(present), gaps=gaps))

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
