"""The "email me when the store opens" list (owner, 2026-11-11).

While checkout is off, product pages and /notify offer a one-field form. An address is stored
once (case-insensitive), with the page it came from; nothing is emailed yet, so there is no
confirmation step. When the store opens, the owner sends one email to the list and records it
in `notified_at`. Table: migrations/003_notify_list.sql.
"""

from __future__ import annotations

import re
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


def add(conn: sqlite3.Connection, email: str, source: str = "") -> bool:
    """Store the address. Returns False if it was already on the list."""
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    cur = conn.execute(
        "INSERT OR IGNORE INTO notify_signups (email, source, created_at) VALUES (?, ?, ?)",
        (email, clean_source(source), now))
    return cur.rowcount == 1
