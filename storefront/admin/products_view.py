"""What the admin's Products pages show (Phase 3, step 5): the product list with its tabs and
search, and the extra facts on one product's page. Read-only: every function takes an open
connection. Changes go through storefront/app/product_changes.py.
"""

from __future__ import annotations

import sqlite3

from ..app.market import MarketReference
from ..app.product_changes import STOCK_KINDS, held

# Tabs on /products: name -> (label, SQL condition on p).
VIEWS = {
    "on-sale": ("On the store", "p.visible = 1"),
    "sold-out": ("Sold out on the store", "p.visible = 1 AND p.track_stock = 1 AND p.stock = 0"),
    "hidden": ("Hidden", "p.visible = 0"),
    "hidden-in-stock": ("Hidden, in stock", "p.visible = 0 AND p.track_stock = 1 AND p.stock > 0"),
    "all": ("All", "1 = 1"),
}
DEFAULT_VIEW = "on-sale"
PAGE_SIZE = 200
# Field names in product_changes -> what the history says.
FIELD_LABELS = {"price": "Price", "compare_at": "“Was” price", "visible": "Visible",
                "featured": "Featured", "max_per_order": "Per-order limit"}
STOCK_LABELS = {"sale": "Sold here", "reservation": "In a checkout", "release": "Checkout ended",
                "restock": "Received", "adjustment": "Counted", "tcgplayer_sale": "Sold on TCGplayer",
                "import": "Imported"}


def counts(conn: sqlite3.Connection) -> dict[str, int]:
    """How many products each tab holds, for the tab labels."""
    return {name: conn.execute(f"SELECT COUNT(*) FROM products p WHERE {cond}").fetchone()[0]
            for name, (_label, cond) in VIEWS.items()}


def _thumb(conn: sqlite3.Connection, product_id: int) -> str | None:
    row = conn.execute("SELECT file_name FROM product_images WHERE product_id = ? ORDER BY sort_order, id LIMIT 1",
                       (product_id,)).fetchone()
    return f"/images/480/{row['file_name']}.webp" if row else None


def _with_market(product: dict, market: MarketReference) -> dict:
    summary = market.summary(product["tcgplayer_product_ids"])
    product["market"] = summary
    product["vs_market"] = (round((product["price_cents"] / 100 / summary.price - 1) * 100)
                            if summary and summary.price else None)
    return product


def list_products(conn: sqlite3.Connection, view: str, query: str, market: MarketReference) -> list[dict]:
    """Products in one tab, matching the search (title, set, SKU), by title."""
    condition = VIEWS.get(view, VIEWS[DEFAULT_VIEW])[1]
    params: list = []
    if query:
        condition += " AND (p.search_text LIKE ? OR p.sku LIKE ? OR p.title LIKE ?)"
        like = f"%{query.lower()}%"
        params += [like, f"%{query}%", f"%{query}%"]
    rows = conn.execute(f"SELECT p.* FROM products p WHERE {condition} ORDER BY p.title LIMIT ?",
                        params + [PAGE_SIZE]).fetchall()
    products = []
    for row in rows:
        product = _with_market(dict(row), market)
        product["thumb"] = _thumb(conn, row["id"])
        products.append(product)
    return products


def get_product(conn: sqlite3.Connection, product_id: int, market: MarketReference) -> dict | None:
    row = conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    if row is None:
        return None
    product = _with_market(dict(row), market)
    product["thumb"] = _thumb(conn, product_id)
    product["held"] = held(conn, product_id)
    product["categories"] = [r["name"] for r in conn.execute(
        "SELECT c.name FROM categories c JOIN product_categories pc ON pc.category_id = c.id "
        "WHERE pc.product_id = ? ORDER BY c.sort_order", (product_id,))]
    product["sold_here"] = -int(conn.execute(
        "SELECT coalesce(sum(change), 0) FROM stock_movements WHERE product_id = ? AND reason = 'sale'",
        (product_id,)).fetchone()[0])
    return product


def describe(entry: dict) -> tuple[str, str]:
    """(what, detail) for one history row from product_changes.history()."""
    if entry["kind"] == "stock":
        change = int(entry["new_value"])
        return STOCK_LABELS.get(entry["field"], entry["field"]), f"{change:+d}"
    label = FIELD_LABELS.get(entry["field"], entry["field"])
    old, new = entry["old_value"], entry["new_value"]
    if entry["field"] in ("price", "compare_at"):
        show = lambda v: "none" if v is None else f"${int(v) / 100:,.2f}"  # noqa: E731
    elif entry["field"] in ("visible", "featured"):
        show = lambda v: "yes" if v == "1" else "no"  # noqa: E731
    else:
        show = lambda v: "no limit" if v is None else v  # noqa: E731
    return label, f"{show(old)} → {show(new)}"


STOCK_FORM_KINDS = [(key, label) for key, (_reason, label) in STOCK_KINDS.items()]
