"""One Poke6s account on the store (storefront plan step 9b; app/accounts.py): the shared
poke6s_session cookie set by the market site is checked against the market's session table
(read-only), the header says Sign in / Account, and /account lists the orders placed with the
account's email."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta

from storefront.app import accounts, seo
from storefront.app.db import connect

ADDRESS = {"name": "Misty", "address": {"line1": "1 Cerulean Way", "city": "Cerulean", "state": "UT",
                                        "postal_code": "84095", "country": "US"}}


def market_db(settings, sessions: dict[str, tuple[str, str, str]]) -> None:
    """A stand-in for the market's tracking.sqlite3: token -> (email, created_at, last_seen_at)."""
    path = accounts.tracking_db(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE tracking_users (id INTEGER PRIMARY KEY, username TEXT)")
    conn.execute("CREATE TABLE tracking_sessions (token TEXT PRIMARY KEY, user_id INTEGER, created_at TEXT, "
                 "last_seen_at TEXT)")
    for user_id, (token, (email, created, seen)) in enumerate(sessions.items(), start=1):
        conn.execute("INSERT INTO tracking_users VALUES (?, ?)", (user_id, email))
        conn.execute("INSERT INTO tracking_sessions VALUES (?, ?, ?, ?)",
                     ("sha256:" + hashlib.sha256(token.encode()).hexdigest(), user_id, created, seen))
    conn.commit()
    conn.close()


def now_iso(days_ago: float = 0) -> str:
    return (datetime.now(UTC) - timedelta(days=days_ago)).replace(microsecond=0).isoformat()


def add_order(settings, number: str, email: str, status: str = "paid", mode: str = "live") -> None:
    with connect(settings.db_path) as conn:
        cur = conn.execute(
            "INSERT INTO orders (number, access_token, status, mode, email, name, shipping_json, ship_class, "
            "subtotal_cents, shipping_cents, total_cents, carrier, tracking_number, created_at, paid_at) "
            "VALUES (?, 'tok123', ?, ?, ?, 'Misty', ?, 'large-sealed', 11899, 2000, 13899, 'USPS', ?, ?, ?)",
            (number, status, mode, email, json.dumps(ADDRESS), "9400111" if status == "shipped" else None,
             "2026-11-20T18:00:00Z", None if status in ("pending", "expired") else "2026-11-20T18:01:00Z"))
        conn.execute("INSERT INTO order_items (order_id, sku, title, unit_price_cents, quantity) "
                     "VALUES (?, 'EN-AH-ETB', 'Ascended Heroes Elite Trainer Box', 11899, 1)", (cur.lastrowid,))


def test_account_for_checks_the_shared_session(imported):
    market_db(imported, {"good": ("Misty@Example.com", now_iso(5), now_iso(1)),
                         "idle": ("brock@example.com", now_iso(60), now_iso(31)),
                         "old": ("gary@example.com", now_iso(181), now_iso(0)),
                         "pin": ("ash", now_iso(1), now_iso(0))})
    assert accounts.account_for(imported, "good") == accounts.Account(email="misty@example.com")
    for token in ("idle", "old", "pin", "unknown", "", None):
        assert accounts.account_for(imported, token) is None


def test_no_market_database_means_signed_out(imported):
    assert accounts.account_for(imported, "good") is None


def test_header_and_account_page_signed_out(client, imported):
    page = client.get("/").text
    assert f'href="{imported.market_base_url}/account/store"' in page and ">Sign in</a>" in page
    account = client.get("/account")
    assert account.status_code == 200 and "Sign in with Google" in account.text
    assert account.headers["cache-control"] == "private, no-store"
    assert "Disallow: /account" in seo.robots_txt(dataclasses.replace(imported, allow_indexing=True))


def test_my_orders_signed_in(client, imported):
    market_db(imported, {"good": ("misty@example.com", now_iso(5), now_iso(1))})
    add_order(imported, "P6-2026-0010", "Misty@Example.com", "shipped")
    add_order(imported, "P6-2026-0011", "misty@example.com", "refunded")
    add_order(imported, "P6-2026-0012", "misty@example.com", "expired")          # never paid
    add_order(imported, "P6-2026-0013", "misty@example.com", "paid", mode="test")  # test order
    add_order(imported, "P6-2026-0014", "brock@example.com", "paid")             # someone else's
    client.cookies.set("poke6s_session", "good")
    home = client.get("/")
    assert ">Account</a>" in home.text and home.headers["cache-control"] == "private, no-store"
    page = client.get("/account").text
    assert "Signed in as <strong>misty@example.com</strong>" in page
    assert "P6-2026-0010" in page and "P6-2026-0011" in page
    for other in ("P6-2026-0012", "P6-2026-0013", "P6-2026-0014"):
        assert other not in page
    assert "/order/P6-2026-0010?k=tok123" in page and "USPS 9400111" in page
    assert f'href="{imported.market_base_url}/account/sign-out"' in page
