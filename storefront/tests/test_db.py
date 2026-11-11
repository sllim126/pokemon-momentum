"""Migrations and the read-only public connection."""

from __future__ import annotations

import sqlite3

import pytest

from storefront.app.db import connect, migrate


def test_migrations_apply_once(tmp_path):
    db = tmp_path / "store.db"
    assert migrate(db) == ["001_catalog", "002_orders", "003_notify_list"]
    assert migrate(db) == []
    conn = connect(db)
    assert conn.execute("SELECT count(*) FROM categories").fetchone()[0] == 9
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    conn.close()


def test_public_connection_is_read_only(imported):
    conn = connect(imported.db_path, readonly=True)
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("UPDATE products SET price_cents = 0")
    conn.close()


def test_stock_cannot_go_negative(imported):
    conn = connect(imported.db_path)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE products SET stock = -1 WHERE sku = 'JP-AE-BB'")
    conn.close()
