"""The wishlist (one Poke6s account, storefront plan step 9b, stage 2).

A signed-in visitor saves products with the ♡ on a product page; the account page lists them
with their price and whether they're in stock. Rows are keyed by the account's email (see
accounts.py) in the store's own database (migration 009), never in the market's.

Later: back-in-stock emails could read this table (who saved a product that just came back).
"""

from __future__ import annotations

import sqlite3

from . import catalog
from .accounts import tracking_db
from .orders import now_iso
from .settings import Settings

MAX_ITEMS = 200   # a generous cap, so one account can't fill the table


def has(conn: sqlite3.Connection, email: str, product_id: int) -> bool:
    return conn.execute("SELECT 1 FROM wishlist WHERE email = ? AND product_id = ?",
                        (email, product_id)).fetchone() is not None


def add(conn: sqlite3.Connection, email: str, product_id: int) -> bool:
    """Save a visible product. False when there's no such product or the list is full."""
    if conn.execute("SELECT 1 FROM products WHERE id = ? AND visible = 1", (product_id,)).fetchone() is None:
        return False
    if conn.execute("SELECT count(*) FROM wishlist WHERE email = ?", (email,)).fetchone()[0] >= MAX_ITEMS:
        return False
    conn.execute("INSERT OR IGNORE INTO wishlist (email, product_id, created_at) VALUES (?, ?, ?)",
                 (email, product_id, now_iso()))
    conn.commit()
    return True


def remove(conn: sqlite3.Connection, email: str, product_id: int) -> None:
    conn.execute("DELETE FROM wishlist WHERE email = ? AND product_id = ?", (email, product_id))
    conn.commit()


def items(conn: sqlite3.Connection, email: str) -> list[dict]:
    """The account's saved products that are still on show, newest saved first, as catalog rows
    (price, in_stock, slug, image)."""
    rows = conn.execute(
        """SELECT p.*, (SELECT file_name FROM product_images i WHERE i.product_id = p.id
                        ORDER BY sort_order LIMIT 1) AS image
           FROM wishlist w JOIN products p ON p.id = w.product_id
           WHERE w.email = ? AND p.visible = 1 ORDER BY w.created_at DESC""", (email,)).fetchall()
    return [catalog._row(r) for r in rows]


def drop_deleted_accounts(conn: sqlite3.Connection, settings: Settings) -> int:
    """Remove wishlists whose account no longer exists on the market (deleted there). Does
    nothing when the market's database can't be read, so a missing file never wipes lists."""
    path = tracking_db(settings)
    if not path.is_file():
        return 0
    try:
        market = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            known = {str(r[0]).strip().lower() for r in market.execute("SELECT username FROM tracking_users")}
        finally:
            market.close()
    except sqlite3.Error:
        return 0
    if not known:
        return 0   # an empty or replaced user table is a fault, not every account deleted
    emails = [r[0] for r in conn.execute("SELECT DISTINCT email FROM wishlist")]
    gone = [email for email in emails if email not in known]
    if gone:
        conn.executemany("DELETE FROM wishlist WHERE email = ?", [(email,) for email in gone])
        conn.commit()
    return len(gone)
