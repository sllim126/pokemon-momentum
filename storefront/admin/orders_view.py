"""What the admin's Orders pages show: the order list with its tabs and search, and the extra
facts on one order's page. Read-only: every function takes an open connection.

Changes to orders (ship, notes, address) live in storefront/app/orders.py, shared with
orders_cli, so the command line and the admin can't drift apart.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

from ..app import notify
from . import overview

# Tabs on /orders: name -> (label, statuses shown). "all" is every real sale; "unfinished" is
# checkouts that never became a sale (kept for questions like "I tried to pay but...").
VIEWS = {
    "to-ship": ("To ship", ("paid",)),
    "shipped": ("Shipped", ("shipped",)),
    "problems": ("Refunds & disputes", ("refunded", "partially_refunded", "disputed")),
    "all": ("All sales", ("paid", "shipped", "refunded", "partially_refunded", "disputed")),
    "unfinished": ("Unfinished checkouts", ("pending", "expired", "cancelled")),
}
DEFAULT_VIEW = "to-ship"
PAGE_SIZE = 100
# Carriers offered when marking shipped; the first four get a "Track package" link in the email
# (notify.TRACKING_URLS). "Other" ships without a link.
CARRIERS = ("USPS", "UPS", "FedEx", "DHL", "Other")


def counts(conn: sqlite3.Connection) -> dict[str, int]:
    """How many orders each tab holds, for the tab labels."""
    by_status = {row["status"]: row["n"] for row in conn.execute(
        "SELECT status, COUNT(*) AS n FROM orders GROUP BY status")}
    return {name: sum(by_status.get(s, 0) for s in statuses) for name, (_label, statuses) in VIEWS.items()}


def list_orders(conn: sqlite3.Connection, view: str, query: str = "", now: datetime | None = None) -> list[dict]:
    """One tab of the order list, newest first ("To ship": oldest first, the packing order).
    `query` matches the order number, the customer's name or email (case-insensitive)."""
    _label, statuses = VIEWS.get(view, VIEWS[DEFAULT_VIEW])
    marks = ",".join("?" * len(statuses))
    where, params = [f"status IN ({marks})"], list(statuses)
    query = query.strip()
    if query:
        like = f"%{query.lower()}%"
        where.append("(lower(number) LIKE ? OR lower(coalesce(name, '')) LIKE ? OR lower(coalesce(email, '')) LIKE ?)")
        params += [like, like, like]
    order_by = "paid_at" if view == "to-ship" else "created_at DESC"
    rows = overview._order_rows(
        conn, "SELECT id, number, mode, status, name, email, total_cents, paid_at, created_at, shipped_at, "
              f"carrier, tracking_number, shipping_json FROM orders WHERE {' AND '.join(where)} "
              f"ORDER BY {order_by} LIMIT ?", (*params, PAGE_SIZE), now=now)
    for row in rows:
        row["destination"] = destination(row.pop("shipping_json"))
    return rows


def destination(shipping_json: str | None) -> str:
    """'Garland, TX' for the list (the full address is on the order's page)."""
    if not shipping_json:
        return ""
    address = (json.loads(shipping_json) or {}).get("address") or {}
    city = (address.get("city") or "").strip()
    return ", ".join(p for p in [city.title() if city.islower() else city, address.get("state") or ""] if p)


def stripe_url(order: dict) -> str:
    """The payment in Stripe's dashboard (test-mode orders live under /test)."""
    if not order.get("stripe_payment_intent"):
        return ""
    prefix = "test/" if order.get("mode") == "test" else ""
    return f"https://dashboard.stripe.com/{prefix}payments/{order['stripe_payment_intent']}"


def timeline(order: dict) -> list[tuple[str, str]]:
    """The order's dated steps, in order, for the detail page."""
    steps = [("Checkout started", order.get("created_at")), ("Paid", order.get("paid_at")),
             ("Shipped", order.get("shipped_at")), ("Closed", order.get("closed_at"))]
    return [(label, notify.local_time(when)) for label, when in steps if when]


def waiting_hours(order: dict, now: datetime | None = None) -> float | None:
    if order.get("status") != "paid" or not order.get("paid_at"):
        return None
    paid = datetime.fromisoformat(order["paid_at"].replace("Z", "+00:00"))
    return ((now or datetime.now(UTC)) - paid).total_seconds() / 3600
