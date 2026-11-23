"""The admin Overview's bug report panel and owner links (owner home, Phase 3 step 9c): open
reports from the market site's queue (read-only), where each came from, and the links."""

from __future__ import annotations

import sqlite3

from fastapi.testclient import TestClient

from storefront.admin import overview
from storefront.admin.main import create_admin_app

from storefront.tests.test_admin import HOME, admin, config, password_hash, sign_in  # noqa: F401


def tracking_db(path, reports):
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE bug_reports (id INTEGER PRIMARY KEY, created_at TEXT, page_path TEXT, title TEXT, "
                 "status TEXT NOT NULL DEFAULT 'new')")
    conn.executemany("INSERT INTO bug_reports (created_at, page_path, title, status) VALUES (?, ?, ?, ?)", reports)
    conn.commit()
    conn.close()
    return path


def test_open_reports_newest_first(tmp_path):
    db = tracking_db(tmp_path / "t.sqlite3", [
        ("2026-04-15T21:07:17+00:00", "/", "Old search idea", "resolved"),
        ("2026-11-20T10:00:00+00:00", "discord #bug-reports", "Chart won't load", "new"),
        ("2026-11-22T10:00:00+00:00", "/m-next", "Can't sign in", "new"),
    ])
    bugs = overview.bug_reports(db)
    assert bugs["found"] and bugs["open"] == 2
    assert [(b["id"], b["source"]) for b in bugs["newest"]] == [(3, "Phone app"), (2, "Discord")]
    assert overview.bug_reports(tmp_path / "missing.sqlite3") == {"found": False, "open": 0, "newest": []}


def test_overview_shows_bug_reports_and_owner_links(imported, config, tmp_path):  # noqa: F811
    config.bug_reports_db = tracking_db(tmp_path / "t.sqlite3", [
        ("2026-11-20T10:00:00+00:00", "discord #bug-reports", "Chart won't load", "new")])
    with TestClient(create_admin_app(imported, config), client=HOME, follow_redirects=False) as client:
        page = sign_in(client).get("/").text
    assert "Bug reports (1 open)" in page and "#1</strong> Chart won&#39;t load" in page
    assert "Discord · 2026-11-20" in page and "1 open bug report (below)" in page
    assert "/admin\" target=\"_blank\"" in page and "Discord #bug-reports" in page
    assert "All clear" not in page


def test_overview_without_the_queue(admin):  # noqa: F811
    page = sign_in(admin).get("/").text
    assert "Bug reports" in page and "isn't readable from here" in page
