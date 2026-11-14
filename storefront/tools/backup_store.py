#!/usr/bin/env python3
"""Nightly backup of the store's data, and the restore that goes with it (review item 9).

What is backed up, from data/storefront/ (the store's data folder):

- store.db         products, stock, orders (names and addresses), the email list. Copied with
                   SQLite's online backup, so it is consistent even while the store is writing.
- visitors.sqlite3 the daily visit counts (same method).
- outbox/          order emails saved while SMTP isn't set up.
- images/          product photos, into one mirror folder (new and changed files are copied;
                   nothing is ever deleted from the mirror, so a photo removed by mistake is
                   still there).

Each night's copy goes into <backup dir>/<YYYY-MM-DD>/ (UTC) with a manifest.json of row
counts, and is checked with PRAGMA integrity_check before it's kept. Folders older than
--keep days (30) are deleted. The backup folder is mode 700 and the files 600: they hold
customers' names and addresses. It lives outside the store container's writable mount, so
the store can't damage its own backups, and the Proxmox Backup Server copies this VM
(data/backups included) off the machine.

With --copy-to DIR the backup folder is then copied to a second device as well (the cron
line uses /mnt/synology-backup/Poke6s, the Synology's Backup share mounted over encrypted
SMB). It mirrors: dated folders pruned here are removed there too, photos never are. DIR
must already exist, so an unmounted share is an error rather than a copy onto this disk.

Usage (standard library only; runs on the host's Python, cron at 03:15 through
scripts/monitoring/run_job.py as `storefront-backup`):

    python3 storefront/tools/backup_store.py backup
    python3 storefront/tools/backup_store.py list
    python3 storefront/tools/backup_store.py restore --to /tmp/check.db         # practice run
    python3 storefront/tools/backup_store.py restore --date 2026-11-12          # the real thing

A restore onto the live database refuses while the store container is running: stop it
first (`docker stop poke6s-storefront`), restore, then start it (`docker start
poke6s-storefront`). The database it replaces is kept beside it as
store.before-restore-<time>.db. Full steps: docs/operator_runbook.txt, "Store backups".
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "storefront"
BACKUP_DIR = ROOT / "data" / "backups" / "storefront"
DATABASES = ("store.db", "visitors.sqlite3")
CONTAINER = "poke6s-storefront"
HEALTH_URL = "http://localhost:8010/healthz"
# Tables whose row counts go in the manifest, so a restore can be compared at a glance.
COUNTED_TABLES = ("products", "orders", "order_items", "notify_signups", "stock_movements")


class BackupError(Exception):
    """Something the operator needs to see; the message is safe to print."""


def copy_database(source: Path, target: Path) -> None:
    """Copy a live SQLite database consistently (online backup API, WAL included)."""
    target.unlink(missing_ok=True)
    src = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    try:
        dst = sqlite3.connect(target)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    os.chmod(target, 0o600)


def check_database(path: Path) -> dict:
    """Integrity-check a database copy and return its row counts; raise if it's damaged."""
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        result = con.execute("PRAGMA integrity_check").fetchone()[0]
        if result != "ok":
            raise BackupError(f"{path.name} failed its integrity check: {result}")
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        return {t: con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in COUNTED_TABLES if t in tables}
    except sqlite3.DatabaseError as exc:
        raise BackupError(f"{path.name} is not a readable database: {exc}") from None
    finally:
        con.close()


def mirror(source: Path, target: Path) -> int:
    """Copy new or changed files from source into target; never delete. Returns files copied."""
    copied = 0
    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        dest = target / path.relative_to(source)
        stat = path.stat()
        if dest.exists() and dest.stat().st_size == stat.st_size and int(dest.stat().st_mtime) == int(stat.st_mtime):
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest)
        copied += 1
    return copied


def private_dir(path: Path) -> Path:
    """Create a folder only the owner can read."""
    path.mkdir(parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


def backup(data_dir: Path, backup_dir: Path, keep_days: int, today: date | None = None) -> Path:
    """Take tonight's backup; returns its folder. A failed check leaves the old copy in place."""
    today = today or datetime.now(timezone.utc).date()
    if not (data_dir / "store.db").exists():
        raise BackupError(f"No store.db in {data_dir}")
    private_dir(backup_dir)
    final = backup_dir / today.isoformat()
    work = private_dir(backup_dir / f".{today.isoformat()}.partial")
    manifest = {"taken_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "files": {}}

    for name in DATABASES:
        if (data_dir / name).exists():
            copy_database(data_dir / name, work / name)
            manifest["files"][name] = {"bytes": (work / name).stat().st_size, "rows": check_database(work / name)}
    if (data_dir / "outbox").is_dir():
        shutil.copytree(data_dir / "outbox", work / "outbox", dirs_exist_ok=True)
        manifest["files"]["outbox"] = {"emails": sum(1 for _ in (work / "outbox").glob("*"))}
    (work / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    # Swap in the finished folder only now, so a run that fails halfway never replaces a good copy.
    if final.exists():
        shutil.rmtree(final)
    work.rename(final)

    if (data_dir / "images").is_dir():
        copied = mirror(data_dir / "images", private_dir(backup_dir / "images"))
        manifest["images_copied"] = copied

    prune(backup_dir, keep_days, today)
    rows = manifest["files"].get("store.db", {}).get("rows", {})
    summary = ", ".join(f"{k} {v}" for k, v in rows.items())
    print(f"BACKUP {today}: store.db ok ({summary}); photos copied {manifest.get('images_copied', 0)}")
    return final


def dated_folders(backup_dir: Path) -> list[tuple[date, Path]]:
    """The nightly folders, oldest first."""
    found = []
    for path in backup_dir.iterdir() if backup_dir.exists() else []:
        try:
            found.append((date.fromisoformat(path.name), path))
        except ValueError:
            continue
    return sorted(found)


def prune(backup_dir: Path, keep_days: int, today: date) -> None:
    """Delete nightly folders older than keep_days; the newest one is always kept."""
    folders = dated_folders(backup_dir)
    cutoff = today - timedelta(days=keep_days)
    for day, path in folders[:-1]:
        if day < cutoff:
            shutil.rmtree(path)


def store_running() -> bool:
    """True if the store container is up (or its health page answers), so a restore must wait."""
    try:
        out = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER],
                             capture_output=True, text=True, timeout=20)
        if out.returncode == 0 and out.stdout.strip() == "true":
            return True
    except (OSError, subprocess.SubprocessError):
        pass
    try:
        with urllib.request.urlopen(HEALTH_URL, timeout=3):
            return True
    except OSError:
        return False


def restore(backup_dir: Path, data_dir: Path, day: str, to: Path | None) -> Path:
    """Put a backup's store.db in place (live, or at `to` for a practice run)."""
    folders = dated_folders(backup_dir)
    if not folders:
        raise BackupError(f"No backups in {backup_dir}")
    chosen = folders[-1][1] if day == "latest" else backup_dir / day
    source = chosen / "store.db"
    if not source.exists():
        raise BackupError(f"No store.db in {chosen}")
    rows = check_database(source)

    live = to is None
    target = data_dir / "store.db" if live else to
    if live and store_running():
        raise BackupError(f"The store is running. Stop it first: docker stop {CONTAINER}")
    if target.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        kept = target.with_name(f"{target.stem}.before-restore-{stamp}.db")
        copy_database(target, kept)  # includes anything still in the WAL file
        print(f"Kept the database it replaces as {kept}")
    for suffix in ("-wal", "-shm"):
        Path(f"{target}{suffix}").unlink(missing_ok=True)
    shutil.copy2(source, target)
    os.chmod(target, 0o600)  # customer data; the store runs as the owner, so it can still read it
    summary = ", ".join(f"{k} {v}" for k, v in rows.items())
    print(f"RESTORED {chosen.name} to {target} ({summary})")
    if live:
        print(f"Now start the store: docker start {CONTAINER}, then check {HEALTH_URL}")
    return target


def copy_off(backup_dir: Path, dest: Path) -> None:
    """Mirror the backup folder onto a second device (see the module docstring)."""
    if not dest.is_dir():
        raise BackupError(f"{dest} doesn't exist: is the NAS share mounted? The local backup is fine.")
    local = {path.name for _, path in dated_folders(backup_dir)}
    copied = 0
    for name in sorted(local):
        copied += mirror(backup_dir / name, dest / name)
    for _, path in dated_folders(dest):
        if path.name not in local:
            shutil.rmtree(path)
    if (backup_dir / "images").is_dir():
        copied += mirror(backup_dir / "images", dest / "images")
    print(f"COPIED to {dest}: {len(local)} nightly folders, {copied} files updated")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--backup-dir", type=Path, default=BACKUP_DIR)
    sub = parser.add_subparsers(dest="command", required=True)
    b = sub.add_parser("backup", help="Take tonight's backup and delete old ones.")
    b.add_argument("--keep", type=int, default=30, help="Days of nightly folders to keep (30).")
    b.add_argument("--copy-to", type=Path, help="Also mirror the backups to this folder (a NAS share).")
    sub.add_parser("list", help="List the backups and their row counts.")
    r = sub.add_parser("restore", help="Restore store.db from a backup.")
    r.add_argument("--date", default="latest", help="YYYY-MM-DD, or latest (default).")
    r.add_argument("--to", type=Path, help="Restore here instead of the live database (practice run).")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.command == "backup":
            backup(args.data_dir, args.backup_dir, args.keep)
            if args.copy_to:
                copy_off(args.backup_dir, args.copy_to)
        elif args.command == "list":
            for day, path in dated_folders(args.backup_dir):
                try:
                    rows = json.loads((path / "manifest.json").read_text())["files"]["store.db"]["rows"]
                except (OSError, ValueError, KeyError):
                    rows = "no manifest"
                print(day, rows)
        else:
            restore(args.backup_dir, args.data_dir, args.date, args.to)
    except BackupError as exc:
        print(f"ERROR: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
