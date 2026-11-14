"""The "email me when the store opens" list (owner, 2026-11-11).

While checkout is off, product pages and /notify offer a one-field form. An address is stored
once (case-insensitive), with the page it came from; there is no confirmation step. Tables:
migrations/003_notify_list.sql and 004_mailing_list_unsubscribe.sql.

The same list receives the store's marketing emails (app/mailing.py). Each address gets a
random unsubscribe token the first time it is emailed; unsubscribing keeps the row with
`unsubscribed_at` set, so it is skipped from then on. Signing up again clears it.
"""

from __future__ import annotations

import re
import secrets
import sqlite3
from datetime import UTC, datetime

# Deliberately loose: one @, something before it, a dot in the domain, no spaces. Real checking
# happens when the email is sent; this only keeps typos and junk out.
_EMAIL = re.compile(r"^[^@\s]{1,64}@[^@\s]+\.[^@\s]{2,}$")
MAX_LENGTH = 254


def clean_email(raw: str) -> str | None:
    """The address, trimmed and lower-cased, or None if it doesn't look like one."""
    email = (raw or "").strip().lower()
    if len(email) > MAX_LENGTH or not _EMAIL.match(email):
        return None
    return email


def clean_source(raw: str) -> str:
    """Only a path on this site is kept (it is a record of where people signed up, nothing more)."""
    source = (raw or "").strip()[:200]
    return source if source.startswith("/") and not source.startswith("//") else ""


# Tokens are 32 URL-safe characters; anything else in an unsubscribe link is not ours.
_TOKEN = re.compile(r"^[A-Za-z0-9_-]{20,64}$")


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def add(conn: sqlite3.Connection, email: str, source: str = "") -> bool:
    """Store the address. Returns False if it was already on the list. Someone who had
    unsubscribed and signs up again is back on the list (and True is returned)."""
    cur = conn.execute(
        "INSERT OR IGNORE INTO notify_signups (email, source, created_at) VALUES (?, ?, ?)",
        (email, clean_source(source), _now()))
    if cur.rowcount == 1:
        return True
    cur = conn.execute("UPDATE notify_signups SET unsubscribed_at = NULL, created_at = ?, source = ? "
                       "WHERE email = ? AND unsubscribed_at IS NOT NULL", (_now(), clean_source(source), email))
    return cur.rowcount == 1


def subscribers(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Everyone who may be emailed: on the list and not unsubscribed, oldest first."""
    return conn.execute("SELECT * FROM notify_signups WHERE unsubscribed_at IS NULL ORDER BY id").fetchall()


def token_for(conn: sqlite3.Connection, email: str) -> str:
    """The address's unsubscribe token, made (and stored) the first time it is needed."""
    row = conn.execute("SELECT unsubscribe_token FROM notify_signups WHERE email = ?", (email,)).fetchone()
    if row is None:
        raise ValueError(f"{email} is not on the list")
    if row[0]:
        return row[0]
    token = secrets.token_urlsafe(24)
    conn.execute("UPDATE notify_signups SET unsubscribe_token = ? WHERE email = ?", (token, email))
    return token


def find_by_token(conn: sqlite3.Connection, token: str) -> sqlite3.Row | None:
    if not _TOKEN.match(token or ""):
        return None
    return conn.execute("SELECT * FROM notify_signups WHERE unsubscribe_token = ?", (token,)).fetchone()


def unsubscribe(conn: sqlite3.Connection, token: str) -> bool:
    """Take the address with this token off the list. True if the token is ours (also when it
    was already unsubscribed: pressing twice is fine)."""
    row = find_by_token(conn, token)
    if row is None:
        return False
    if not row["unsubscribed_at"]:
        conn.execute("UPDATE notify_signups SET unsubscribed_at = ? WHERE id = ?", (_now(), row["id"]))
    return True
