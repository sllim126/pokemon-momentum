import os
import sys
import shutil
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


START_DATE = date(2024, 2, 8)
END_DATE = date.today()

ARCHIVES_DIR = Path(r"F:\Pokemon historical data archives")
EXTRACT_ROOT = Path(r"F:\Pokemon historical data extracted")
SEVEN_ZIP = Path(r"C:\Program Files\7-Zip\7z.exe")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def has_files(p: Path) -> bool:
    try:
        return any(p.rglob("*"))
    except FileNotFoundError:
        return False


def download_file(url: str, dest: Path, timeout: int = 60) -> bool:
    """
    Returns True if downloaded successfully, False if missing/failed.
    Cleans up partial files on failure.
    """
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            # Some servers may return 200 with content, otherwise raise HTTPError
            ensure_dir(dest.parent)
            with open(dest, "wb") as f:
                shutil.copyfileobj(resp, f)
        return True
    except HTTPError as e:
        # 404 or other HTTP error
        if dest.exists():
            dest.unlink(missing_ok=True)
        return False
    except URLError:
        if dest.exists():
            dest.unlink(missing_ok=True)
        return False
    except Exception:
        if dest.exists():
            dest.unlink(missing_ok=True)
        return False


def extract_7z(archive: Path, dest_dir: Path) -> None:
    ensure_dir(dest_dir)
    cmd = [str(SEVEN_ZIP), "x", str(archive), f"-o{dest_dir}", "-y"]
    # Silence output like your PS script
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main() -> int:
    ensure_dir(ARCHIVES_DIR)
    ensure_dir(EXTRACT_ROOT)

    if not SEVEN_ZIP.exists():
        print(f"ERROR: 7-Zip not found at: {SEVEN_ZIP}")
        print("Update SEVEN_ZIP to the correct path.")
        return 1

    new_downloads: list[Path] = []

    # 1) Download phase
    for d in daterange(START_DATE, END_DATE):
        date_str = d.strftime("%Y-%m-%d")
        url = f"https://tcgcsv.com/archive/tcgplayer/prices-{date_str}.ppmd.7z"
        out_file = ARCHIVES_DIR / f"prices-{date_str}.ppmd.7z"

        if out_file.exists():
            print(f"SKIP DOWNLOAD: {date_str} (already have archive)")
            continue

        print(f"DOWNLOADING: {date_str}")
        ok = download_file(url, out_file)

        if not ok:
            print(f"SKIP DOWNLOAD: {date_str} (missing or failed)")
            continue

        size = out_file.stat().st_size
        print(f"OK DOWNLOAD: {date_str} ({size} bytes)")
        new_downloads.append(out_file)

    print("\nDOWNLOAD PHASE DONE\n")

    if not new_downloads:
        print("No new downloads this run. Nothing to extract.")
        print("ALL DONE")
        return 0

    # 2) Extract only new downloads
    for archive in new_downloads:
        dest = EXTRACT_ROOT / archive.stem  # strips only .7z -> prices-YYYY-MM-DD.ppmd
        if dest.exists() and has_files(dest):
            print(f"SKIP EXTRACT (already extracted): {archive.name}")
            continue

        print(f"EXTRACTING: {archive.name}")
        try:
            extract_7z(archive, dest)
            print(f"DONE EXTRACT: {archive.name}")
        except subprocess.CalledProcessError:
            print(f"ERROR EXTRACTING: {archive.name}")
            # keep going
            continue

    print("\nALL DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())