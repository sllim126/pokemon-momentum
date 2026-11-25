"""Support messages (one Poke6s account, storefront plan step 9b, stage 3; owner 2026-11-23).

The Contact page offers three ways to reach the owner: email (a mailto link), Discord (the
server invite, often fastest) and a message box. A message is saved here (migration 010),
pings the private Discord order channel with its number only (names, emails and the text stay
out of Discord), and is listed on the store admin's Overview until marked answered. The owner
replies by email.

Spam: a hidden "website" field bots fill in, an hourly limit per visitor and for the whole site
(in main.py), and length limits. No CAPTCHA (nothing to pay for or load from Google).
Bugs still go to Discord #bug-reports (AGENTS.md); this box is for orders and questions.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, datetime, timedelta

from .notify_list import clean_email
from .orders import now_iso

MAX_NAME = 100
MAX_MESSAGE = 4000
MIN_MESSAGE = 5
KEEP_ANSWERED_DAYS = 365
ORDER_NUMBER = re.compile(r"^P6-\d{4}-\d{4}$")


def clean(name: str, email: str, order: str, message: str) -> tuple[dict | None, str]:
    """({name, email, order_number, message}, "") when the form is usable, else (None, why)."""
    name = " ".join(str(name or "").split())[:MAX_NAME]
    address = clean_email(str(email or ""))
    order = str(order or "").strip().upper()
    text = str(message or "").strip()
    if not name:
        return None, "Please tell us your name."
    if address is None:
        return None, "Please give an email address we can reply to."
    if order and not ORDER_NUMBER.match(order):
        return None, "Order numbers look like P6-2026-0012 (or leave it empty)."
    if len(text) < MIN_MESSAGE:
        return None, "Please tell us how we can help."
    if len(text) > MAX_MESSAGE:
        return None, f"Please keep the message under {MAX_MESSAGE:,} characters."
    return {"name": name, "email": address, "order_number": order, "message": text}, ""


def add(conn: sqlite3.Connection, fields: dict, signed_in: bool = False) -> int:
    cur = conn.execute(
        "INSERT INTO support_messages (created_at, name, email, order_number, message, signed_in) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (now_iso(), fields["name"], fields["email"], fields["order_number"] or None, fields["message"],
         1 if signed_in else 0))
    conn.commit()
    return int(cur.lastrowid)


def open_messages(conn: sqlite3.Connection, limit: int = 10) -> tuple[int, list[dict]]:
    """(how many are unanswered, the newest `limit` of them)."""
    count = conn.execute("SELECT count(*) FROM support_messages WHERE status = 'new'").fetchone()[0]
    rows = conn.execute("SELECT * FROM support_messages WHERE status = 'new' ORDER BY created_at DESC LIMIT ?",
                        (limit,)).fetchall()
    return count, [dict(r) for r in rows]


def mark_answered(conn: sqlite3.Connection, message_id: int) -> bool:
    changed = conn.execute("UPDATE support_messages SET status = 'answered', answered_at = ? "
                           "WHERE id = ? AND status = 'new'", (now_iso(), message_id)).rowcount
    conn.commit()
    return changed == 1


def drop_old(conn: sqlite3.Connection, now: datetime | None = None) -> int:
    """Delete answered messages older than KEEP_ANSWERED_DAYS (privacy page)."""
    cutoff = ((now or datetime.now(UTC)) - timedelta(days=KEEP_ANSWERED_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    gone = conn.execute("DELETE FROM support_messages WHERE status = 'answered' AND created_at < ?",
                        (cutoff,)).rowcount
    conn.commit()
    return gone
