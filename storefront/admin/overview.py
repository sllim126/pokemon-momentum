"""What the admin's Overview page shows: what needs doing today, and whether the shop is healthy.

Read-only: every function takes an open connection (or a folder) and returns plain data.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path

from ..app import support
from ..app.settings import Settings

# A paid order should ship within 1 business day (Shipping page). Older than this is flagged.
SHIP_LATE_HOURS = 24
# Most sealed products are stocked one or two at a time, so "1 left" is normal and isn't
# listed; the Overview lists products on show that have sold out, and counts those at their last.
# The nightly backup runs at 03:15; older than this means a night was missed.
BACKUP_STALE_HOURS = 36
RECENT_ORDERS = 10
# Statuses that never became a sale: left out of "recent orders" and the month's totals.
NOT_SALES = ("pending", "expired", "cancelled")


def _parse(iso: str | None) -> datetime | None:
    if not iso:
        return None
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _item_counts(conn: sqlite3.Connection, order_ids: list[int]) -> dict[int, int]:
    if not order_ids:
        return {}
    marks = ",".join("?" * len(order_ids))
    rows = conn.execute(f"SELECT order_id, SUM(quantity) AS n FROM order_items WHERE order_id IN ({marks}) "
                        "GROUP BY order_id", order_ids)
    return {row["order_id"]: row["n"] for row in rows}


def _order_rows(conn: sqlite3.Connection, sql: str, params=(), now: datetime | None = None) -> list[dict]:
    now = now or datetime.now(UTC)
    rows = [dict(row) for row in conn.execute(sql, params)]
    counts = _item_counts(conn, [row["id"] for row in rows])
    for row in rows:
        row["items"] = counts.get(row["id"], 0)
        paid = _parse(row.get("paid_at"))
        row["waiting_hours"] = (now - paid).total_seconds() / 3600 if paid else 0
        row["late"] = row["status"] == "paid" and row["waiting_hours"] > SHIP_LATE_HOURS
    return rows


def to_ship(conn: sqlite3.Connection, now: datetime | None = None) -> list[dict]:
    """Paid orders not yet shipped, oldest first: the day's packing list."""
    return _order_rows(conn, "SELECT id, number, mode, status, name, total_cents, paid_at, created_at "
                             "FROM orders WHERE status = 'paid' ORDER BY paid_at", now=now)


def disputed(conn: sqlite3.Connection) -> list[dict]:
    return _order_rows(conn, "SELECT id, number, mode, status, name, total_cents, paid_at, created_at "
                             "FROM orders WHERE status = 'disputed' ORDER BY created_at")


def recent_orders(conn: sqlite3.Connection, limit: int = RECENT_ORDERS) -> list[dict]:
    marks = ",".join("?" * len(NOT_SALES))
    return _order_rows(conn, "SELECT id, number, mode, status, name, total_cents, paid_at, created_at "
                             f"FROM orders WHERE status NOT IN ({marks}) ORDER BY created_at DESC LIMIT ?",
                       (*NOT_SALES, limit))


def month_totals(conn: sqlite3.Connection, today: date | None = None) -> dict:
    """Live orders paid this calendar month (UTC): count, sales, refunds. Test orders are left out."""
    today = today or datetime.now(UTC).date()
    start = today.replace(day=1).isoformat()
    marks = ",".join("?" * len(NOT_SALES))
    row = conn.execute(
        "SELECT COUNT(*) AS orders, COALESCE(SUM(total_cents), 0) AS sales, "
        "COALESCE(SUM(refunded_cents), 0) AS refunded, COALESCE(SUM(tax_cents), 0) AS tax "
        f"FROM orders WHERE mode = 'live' AND status NOT IN ({marks}) AND paid_at >= ?",
        (*NOT_SALES, start)).fetchone()
    totals = dict(row)
    # Net of refunds: what the month actually brought in (before Stripe's fees).
    return {"month": today.strftime("%B %Y"), "net": totals["sales"] - totals["refunded"], **totals}


def sold_out(conn: sqlite3.Connection) -> list[dict]:
    """Products still on show that track stock and have none left (they show as sold out)."""
    return [dict(row) for row in conn.execute(
        "SELECT sku, title, stock FROM products WHERE visible = 1 AND track_stock = 1 AND stock = 0 "
        "ORDER BY title")]


def last_one(conn: sqlite3.Connection) -> int:
    """How many products on show are down to their last unit."""
    return conn.execute("SELECT COUNT(*) FROM products WHERE visible = 1 AND track_stock = 1 "
                        "AND stock = 1").fetchone()[0]


def held_stock(conn: sqlite3.Connection) -> int:
    """Units held by checkouts still open (pending orders' reserved lines)."""
    row = conn.execute("SELECT COALESCE(SUM(i.quantity), 0) AS n FROM order_items i JOIN orders o "
                       "ON o.id = i.order_id WHERE o.status = 'pending' AND i.reserved = 1").fetchone()
    return row["n"]


def email_queue(settings: Settings) -> dict:
    """Order emails waiting for a retry, and those the shop gave up on (notify.py)."""
    def count(name: str) -> int:
        folder = settings.outbox_dir / name
        return sum(1 for _ in folder.glob("*.json")) if folder.is_dir() else 0
    return {"retrying": count("retry"), "failed": count("failed")}


def last_backup(backup_dir: Path, now: datetime | None = None) -> dict:
    """The newest nightly backup (storefront/tools/backup_store.py) and whether it's recent."""
    now = now or datetime.now(UTC)
    newest = None
    if backup_dir.is_dir():
        for path in backup_dir.iterdir():
            try:
                date.fromisoformat(path.name)
            except ValueError:
                continue
            if (path / "manifest.json").exists() and (newest is None or path.name > newest.name):
                newest = path
    if newest is None:
        return {"found": False, "stale": True}
    manifest = json.loads((newest / "manifest.json").read_text(encoding="utf-8"))
    taken = _parse(manifest.get("taken_at"))
    hours = (now - taken).total_seconds() / 3600 if taken else None
    rows = manifest.get("files", {}).get("store.db", {}).get("rows", {})
    return {"found": True, "day": newest.name, "taken_at": manifest.get("taken_at"), "hours": hours,
            "stale": hours is None or hours > BACKUP_STALE_HOURS, "rows": rows}


def store_status(settings: Settings) -> dict:
    """Checkout, payments and email set-up, as the public store sees them."""
    if not settings.checkout_switch:
        checkout = "off (catalog only)"
    elif settings.checkout_enabled:
        checkout = "on"
    else:
        checkout = "off: " + (settings.setup_problem or "no Stripe key, or a live key without STOREFRONT_ALLOW_LIVE=1")
    return {"checkout": checkout, "checkout_on": settings.checkout_enabled,
            "stripe_mode": settings.stripe_mode if settings.stripe_secret_key else "none",
            "email": settings.email_transport, "tax": settings.tax_mode}


# Bug reports (owner home, Phase 3 step 9c). Visitors report bugs in Discord #bug-reports, the
# phone app's feedback form, or (older) the market site; all land in the market site's
# `bug_reports` table (data/processed/tracking.sqlite3), which the admin reads read-only. They're
# resolved by whoever fixes them: scripts/utilities/bug_reports.py resolve <id>.
DONE_STATUSES = ("resolved", "closed", "duplicate", "wontfix")
BUG_LIST_LIMIT = 5


def _bug_source(page_path: str | None) -> str:
    """Where a report came from (Discord, Phone app, Market site), from its page_path."""
    path = (page_path or "").lower()
    if path.startswith("discord"):
        return "Discord"
    if path.startswith("/m") or path.startswith("/mobile"):
        return "Phone app"
    return "Market site"


def bug_reports(db_path: Path | None) -> dict:
    """{"found", "open" (count), "newest" (up to BUG_LIST_LIMIT open reports, newest first)}.
    found=False when the market's database isn't there (a fresh install or tests)."""
    if not db_path or not db_path.is_file():
        return {"found": False, "open": 0, "newest": []}
    marks = ",".join("?" * len(DONE_STATUSES))
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        where = f"WHERE coalesce(status, 'new') NOT IN ({marks})"
        count = conn.execute(f"SELECT count(*) FROM bug_reports {where}", DONE_STATUSES).fetchone()[0]
        rows = conn.execute(f"SELECT id, created_at, page_path, title FROM bug_reports {where} "
                            "ORDER BY created_at DESC LIMIT ?", (*DONE_STATUSES, BUG_LIST_LIMIT)).fetchall()
    except sqlite3.Error:
        return {"found": False, "open": 0, "newest": []}
    finally:
        conn.close()
    return {"found": True, "open": count,
            "newest": [{"id": r["id"], "created_at": r["created_at"], "source": _bug_source(r["page_path"]),
                        "title": (r["title"] or "(no title)")[:120]} for r in rows]}


def build(conn: sqlite3.Connection, settings: Settings, backup_dir: Path, now: datetime | None = None,
          bug_db: Path | None = None) -> dict:
    """Everything the Overview page shows, gathered fresh on each load (bug_db: the market's tracking database)."""
    now = now or datetime.now(UTC)
    return {
        "bugs": bug_reports(bug_db),
        # Messages from the Contact page's box (app/support.py), unanswered, newest first.
        "support": dict(zip(("open", "newest"), support.open_messages(conn))),
        "to_ship": to_ship(conn, now),
        "disputed": disputed(conn),
        "recent": recent_orders(conn),
        "month": month_totals(conn, now.date()),
        "sold_out": sold_out(conn),
        "last_one": last_one(conn),
        "held": held_stock(conn),
        "emails": email_queue(settings),
        "backup": last_backup(backup_dir, now),
        "status": store_status(settings),
    }


def hours_label(hours: float | None) -> str:
    if hours is None:
        return "unknown"
    if hours < 1:
        return "under an hour"
    if hours < 48:
        return f"{int(hours)} h"
    return f"{int(hours // 24)} days"

