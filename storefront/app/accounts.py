"""Who's signed in on the store: the one Poke6s account (storefront plan, Phase 3 step 9b).

Accounts live on the market site (market.poke6s.com, Google sign-in; scripts/dashboards/api.py
and tracking_store.py). When someone signs in there, the market's server also sets the
`poke6s_session` cookie for all of .poke6s.com (HttpOnly, Secure, SameSite=Lax). This module
reads that cookie and checks it against the market's session table, read-only:

- the token is stored hashed ("sha256:" + hex), so the same hash is computed here;
- a session is over after 30 idle days or 180 days in all (the market's SESSION_IDLE_DAYS and
  SESSION_MAX_DAYS). The store can't refresh "last seen" (read-only), so a visitor who only
  uses the store is asked to sign in again after 30 days; using the market refreshes it.

The store never writes to the market's database and never sees a Google token. Signing in and
out happen on the market (sign_in_url / sign_out_url), which come straight back here.

The account is the Google email (verified by Google), and "My orders" are the store's live
orders placed with that email, including guest orders from before the account existed.
"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .settings import Settings

COOKIE = "poke6s_session"
SESSION_IDLE_DAYS = 30
SESSION_MAX_DAYS = 180
# Orders that took money (the rest never became a sale).
ORDER_STATUSES = ("paid", "shipped", "partially_refunded", "refunded")


@dataclass(frozen=True)
class Account:
    email: str


def tracking_db(settings: Settings) -> Path:
    """The market's account database (data/processed/tracking.sqlite3, mounted read-only)."""
    return settings.data_dir.parent / "processed" / "tracking.sqlite3"


def sign_in_url(settings: Settings) -> str:
    return f"{settings.market_base_url}/account/store"


def sign_out_url(settings: Settings) -> str:
    return f"{settings.market_base_url}/account/sign-out"


def _parse(stamp: str) -> datetime:
    moment = datetime.fromisoformat(stamp)
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def account_for(settings: Settings, token: str | None, now: datetime | None = None) -> Account | None:
    """The signed-in account for a poke6s_session cookie, or None (no cookie, unknown or expired
    session, or the market's database isn't there)."""
    token = (token or "").strip()
    path = tracking_db(settings)
    if not token or len(token) > 200 or not path.is_file():
        return None
    stored = "sha256:" + hashlib.sha256(token.encode("utf-8")).hexdigest()
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            row = conn.execute(
                "SELECT s.created_at, s.last_seen_at, u.username FROM tracking_sessions s "
                "JOIN tracking_users u ON u.id = s.user_id WHERE s.token = ?", (stored,)).fetchone()
        finally:
            conn.close()
    except sqlite3.Error:
        return None
    if row is None:
        return None
    created, last_seen, username = row
    now = now or datetime.now(UTC)
    try:
        if (now - _parse(last_seen) > timedelta(days=SESSION_IDLE_DAYS)
                or now - _parse(created) > timedelta(days=SESSION_MAX_DAYS)):
            return None
    except (TypeError, ValueError):
        return None
    email = str(username or "").strip().lower()
    # Old username+PIN accounts (retired) have no email: they can't match any order.
    return Account(email=email) if "@" in email else None


def my_orders(conn: sqlite3.Connection, account: Account) -> list[dict]:
    """The account's live orders that took money, newest first, with their items."""
    rows = conn.execute(
        f"SELECT * FROM orders WHERE lower(email) = ? AND mode = 'live' AND status IN "
        f"({','.join('?' * len(ORDER_STATUSES))}) ORDER BY paid_at DESC",
        (account.email, *ORDER_STATUSES)).fetchall()
    orders = [dict(row) for row in rows]
    for order in orders:
        order["items"] = [dict(r) for r in conn.execute(
            "SELECT title, quantity FROM order_items WHERE order_id = ? ORDER BY id", (order["id"],))]
    return orders
