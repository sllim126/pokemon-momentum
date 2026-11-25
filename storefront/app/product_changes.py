"""Changing products from the admin (Phase 3, step 5): price, visibility and stock, each logged.

Two logs, kept apart on purpose:

- stock_movements (migration 001): every change to products.stock. Its sum per product must
  equal products.stock (test_import.py checks this), so stock only ever moves by a logged
  amount, never by overwriting the column. The admin adds three kinds:
    received      +N, reason 'restock'        (a delivery, a return put back on the shelf)
    sold_elsewhere -N, reason 'tcgplayer_sale' (sold on TCGplayer)
    counted       set to what's on the shelf, reason 'adjustment' (the difference is logged)
- product_changes (migration 005): price, compare-at price, visible, featured and the
  per-order limit, with the old and new value and the reason. reprice.py writes here too.

products.stock is what can still be sold, so it already leaves out units held by checkouts in
progress (orders.py reserves stock when Stripe Checkout opens). "Counted" therefore takes the
held units off the shelf count: 3 on the shelf with 1 in someone's checkout means 2 available.

Every function takes a `writer` connection (db.connect(..., writer=True), autocommit) and wraps
its change in BEGIN IMMEDIATE, like orders.py, so a checkout can't slip in between the read and
the write.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime

from .orders import now_iso

MAX_REASON = 300
# Stripe won't charge less than $0.50, so a visible product can't cost less.
MIN_PRICE_CENTS = 50
MAX_PRICE_CENTS = 1_000_000          # $10,000: anything above is a typo
MAX_PER_ORDER = 99
# A price change bigger than this (either way) must be confirmed on the form, which catches a
# typed 1299 meant as 12.99. Fraction of the old price.
BIG_CHANGE = 0.5
STOCK_KINDS = {
    "received": ("restock", "Received"),
    "sold_elsewhere": ("tcgplayer_sale", "Sold on TCGplayer"),
    "counted": ("adjustment", "Counted"),
}
MAX_STOCK_STEP = 10_000
MONEY = re.compile(r"^\$?\s*(\d{1,3}(?:,\d{3})+|\d+)(?:\.(\d{1,2}))?$")


def parse_money(text: str) -> int | None:
    """'12.99', '$12.99', '1,299' or '12.5' -> cents. Empty -> None. Anything else -> ValueError."""
    text = (text or "").strip()
    if not text:
        return None
    match = MONEY.match(text)
    if not match:
        raise ValueError(f"“{text[:20]}” isn't a price. Type it like 12.99.")
    dollars = int(match.group(1).replace(",", ""))
    cents = int((match.group(2) or "0").ljust(2, "0"))
    return dollars * 100 + cents


def clean_reason(text: str) -> str:
    return " ".join((text or "").split())[:MAX_REASON]


def held(conn: sqlite3.Connection, product_id: int) -> int:
    """Units in checkouts that are open right now (reserved, not yet paid or released)."""
    row = conn.execute(
        "SELECT coalesce(sum(i.quantity), 0) AS n FROM order_items i JOIN orders o ON o.id = i.order_id "
        "WHERE i.product_id = ? AND i.reserved = 1 AND o.status = 'pending'", (product_id,)).fetchone()
    return int(row["n"])


def _product(conn: sqlite3.Connection, product_id: int) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    if row is None:
        raise ValueError("That product doesn't exist")
    return row


def _value(field: str, value) -> str | None:
    return None if value is None else str(int(value))


def update_product(conn: sqlite3.Connection, product_id: int, wanted: dict, reason: str = "", *,
                   confirmed: bool = False, source: str = "admin", now: datetime | None = None) -> list[str]:
    """Change any of price_cents, compare_at_cents, visible, featured, max_per_order, price_manual
    (1 = priced by hand: the nightly repricing leaves it alone; migration 011).

    `wanted` holds only the fields the form sent. Unchanged fields are skipped; the changed ones
    are written and logged in one transaction. Returns the changed field names ([] if nothing
    changed). Raises ValueError with a message for the owner when a value isn't allowed.
    """
    reason = clean_reason(reason)
    at = now_iso(now)
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = _product(conn, product_id)
        new = {key: wanted.get(key, row[key]) for key in
               ("price_cents", "compare_at_cents", "visible", "featured", "max_per_order", "price_manual")}
        price = new["price_cents"]
        if price is None or not MIN_PRICE_CENTS <= price <= MAX_PRICE_CENTS:
            raise ValueError("The price must be between $0.50 and $10,000.")
        if new["compare_at_cents"] is not None and new["compare_at_cents"] <= price:
            raise ValueError("The “was” price must be higher than the price, or left empty.")
        if new["max_per_order"] is not None and not 1 <= new["max_per_order"] <= MAX_PER_ORDER:
            raise ValueError(f"The per-order limit must be 1 to {MAX_PER_ORDER}, or left empty for no limit.")
        changed = [key for key in new if new[key] != row[key]]
        if "price_cents" in changed:
            if not reason:
                raise ValueError("Say why the price changed (it goes in the product's history).")
            old = row["price_cents"]
            if old and abs(price - old) / old > BIG_CHANGE and not confirmed:
                raise ValueError(f"That's a big change ({old / 100:,.2f} → {price / 100:,.2f}). "
                                 "Tick “Yes, this price is right” to save it.")
        if not changed:
            conn.rollback()
            return []
        sets = ", ".join(f"{key} = ?" for key in changed)
        conn.execute(f"UPDATE products SET {sets}, updated_at = ? WHERE id = ?",
                     [new[key] for key in changed] + [at, product_id])
        for key in changed:
            field = key.removesuffix("_cents")
            conn.execute("INSERT INTO product_changes (product_id, field, old_value, new_value, reason, source, at) "
                         "VALUES (?, ?, ?, ?, ?, ?, ?)",
                         (product_id, field, _value(key, row[key]), _value(key, new[key]), reason, source, at))
        conn.commit()
        return changed
    except BaseException:
        conn.rollback()
        raise


def adjust_stock(conn: sqlite3.Connection, product_id: int, kind: str, quantity: int, note: str = "",
                 now: datetime | None = None) -> int:
    """Record received stock, a TCGplayer sale or a shelf count (see the module docstring).

    Returns the change made to the available count (0 when a count matched). Raises ValueError
    with a message for the owner.
    """
    if kind not in STOCK_KINDS:
        raise ValueError("Pick what happened to the stock.")
    if not 0 <= quantity <= MAX_STOCK_STEP or (kind != "counted" and quantity == 0):
        raise ValueError("Type how many (a whole number).")
    reason, label = STOCK_KINDS[kind]
    note = clean_reason(note)
    at = now_iso(now)
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = _product(conn, product_id)
        if not row["track_stock"]:
            raise ValueError("This product isn't stock-counted (it sells without a count), so there's nothing to change.")
        if kind == "received":
            change = quantity
        elif kind == "sold_elsewhere":
            change = -quantity
            if row["stock"] + change < 0:
                raise ValueError(f"Only {row['stock']} available, so {quantity} can't have sold.")
        else:
            in_checkout = held(conn, product_id)
            if quantity < in_checkout:
                raise ValueError(f"{in_checkout} are in a checkout right now, so the shelf can't hold fewer. "
                                 "Count again after it finishes or expires (30 minutes at most).")
            change = quantity - in_checkout - row["stock"]
        if change:
            conn.execute("UPDATE products SET stock = stock + ?, updated_at = ? WHERE id = ?",
                         (change, at, product_id))
            reference = f"{label}: {note}" if note else label
            conn.execute("INSERT INTO stock_movements (product_id, change, reason, reference, at) "
                         "VALUES (?, ?, ?, ?, ?)", (product_id, change, reason, reference, at))
        conn.commit()
        return change
    except BaseException:
        conn.rollback()
        raise


def history(conn: sqlite3.Connection, product_id: int, limit: int = 100) -> list[dict]:
    """The product's stock movements and field changes, newest first, as one list for the page."""
    rows = conn.execute(
        """
        SELECT at, 'stock' AS kind, reason AS field, NULL AS old_value, change AS new_value,
               coalesce(reference, '') AS reason, '' AS source
          FROM stock_movements WHERE product_id = ?
        UNION ALL
        SELECT at, 'change', field, old_value, new_value, reason, source
          FROM product_changes WHERE product_id = ?
        ORDER BY at DESC LIMIT ?
        """, (product_id, product_id, limit)).fetchall()
    return [dict(row) for row in rows]
