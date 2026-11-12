"""The nightly store backup and its restore (storefront/tools/backup_store.py)."""

import sqlite3
from datetime import date

import pytest

from storefront.tools import backup_store as bs


@pytest.fixture
def store(tmp_path):
    """A small store data folder: a WAL-mode database with an unflushed write, photos, outbox."""
    data = tmp_path / "data"
    (data / "images" / "p").mkdir(parents=True)
    (data / "images" / "p" / "a.webp").write_bytes(b"img")
    (data / "outbox").mkdir()
    (data / "outbox" / "1.eml").write_text("hello")
    con = sqlite3.connect(data / "store.db")
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)")
    con.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
    con.executemany("INSERT INTO products (name) VALUES (?)", [("a",), ("b",)])
    con.commit()  # stays in the WAL file while this connection is open
    yield data, tmp_path / "backups", con
    con.close()


def rows(path, table="products"):
    con = sqlite3.connect(path)
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        con.close()


def test_backup_copies_everything_and_checks_it(store):
    data, backups, _ = store
    folder = bs.backup(data, backups, keep_days=30, today=date(2026, 11, 12))
    assert folder.name == "2026-11-12"
    assert rows(folder / "store.db") == 2  # the write still in the WAL is included
    assert (folder / "outbox" / "1.eml").read_text() == "hello"
    assert (backups / "images" / "p" / "a.webp").read_bytes() == b"img"
    assert '"products": 2' in (folder / "manifest.json").read_text()
    assert oct(backups.stat().st_mode & 0o777) == "0o700"
    assert oct((folder / "store.db").stat().st_mode & 0o777) == "0o600"


def test_old_backups_pruned_but_photos_never_deleted(store):
    data, backups, _ = store
    bs.backup(data, backups, keep_days=30, today=date(2026, 10, 1))
    (data / "images" / "p" / "a.webp").unlink()
    bs.backup(data, backups, keep_days=30, today=date(2026, 11, 12))
    assert [d.isoformat() for d, _ in bs.dated_folders(backups)] == ["2026-11-12"]
    assert (backups / "images" / "p" / "a.webp").exists()


def test_damaged_database_is_refused(tmp_path):
    bad = tmp_path / "store.db"
    bad.write_bytes(b"not a database" * 100)
    with pytest.raises(bs.BackupError):
        bs.check_database(bad)


def test_practice_restore_to_another_path(store, tmp_path):
    data, backups, _ = store
    bs.backup(data, backups, keep_days=30, today=date(2026, 11, 12))
    target = bs.restore(backups, data, "latest", tmp_path / "check.db")
    assert rows(target) == 2


def test_live_restore_refused_while_store_runs(store, monkeypatch):
    data, backups, _ = store
    bs.backup(data, backups, keep_days=30, today=date(2026, 11, 12))
    monkeypatch.setattr(bs, "store_running", lambda: True)
    with pytest.raises(bs.BackupError, match="Stop it first"):
        bs.restore(backups, data, "latest", None)


def test_live_restore_keeps_the_replaced_database(store, monkeypatch):
    data, backups, con = store
    bs.backup(data, backups, keep_days=30, today=date(2026, 11, 12))
    con.execute("INSERT INTO products (name) VALUES ('c')")  # a change made after the backup
    con.commit()
    con.close()
    monkeypatch.setattr(bs, "store_running", lambda: False)
    bs.restore(backups, data, "2026-11-12", None)
    assert rows(data / "store.db") == 2
    kept = list(data.glob("store.before-restore-*.db"))
    assert len(kept) == 1 and rows(kept[0]) == 3
