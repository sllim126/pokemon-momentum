"""Read-only catalog queries for the public pages: listing with filters, search, one product."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

from . import classify

PAGE_SIZE = 48

SORTS = {
    "newest": "p.created_at DESC, p.id DESC",
    "price-asc": "p.price_cents ASC, p.id",
    "price-desc": "p.price_cents DESC, p.id",
}
SORT_LABELS = {"newest": "Newest", "price-asc": "Price: low to high", "price-desc": "Price: high to low"}

# Available first, then sold out; within each, the chosen sort.
IN_STOCK_FIRST = "(CASE WHEN p.track_stock = 0 OR p.stock > 0 THEN 0 ELSE 1 END)"


@dataclass
class Filters:
    q: str = ""
    lang: str = ""          # 'en' | 'ja'
    type: str = ""          # product_type
    set: str = ""           # set_slug
    category: str = ""      # category slug
    sort: str = "newest"
    page: int = 1

    def clean(self) -> "Filters":
        self.q = " ".join(self.q.split())[:100]
        self.lang = self.lang if self.lang in classify.LANGUAGE_LABELS else ""
        self.type = self.type if self.type in classify.PRODUCT_TYPES else ""
        self.set = classify.slugify(self.set) if self.set else ""
        self.sort = self.sort if self.sort in SORTS else "newest"
        self.page = max(1, min(self.page, 1000))
        return self


@dataclass
class Page:
    products: list[dict]
    total: int
    page: int
    pages: int
    facets: dict = field(default_factory=dict)


def _row(row: sqlite3.Row) -> dict:
    product = dict(row)
    product["in_stock"] = bool(product["track_stock"] == 0 or product["stock"] > 0)
    product["type_label"] = classify.PRODUCT_TYPE_LABELS.get(product["product_type"], "")
    product["language_label"] = classify.LANGUAGE_LABELS.get(product["language"] or "", "")
    return product


def _where(filters: Filters) -> tuple[list[str], list]:
    clauses, params = ["p.visible = 1"], []
    if filters.lang:
        clauses.append("p.language = ?")
        params.append(filters.lang)
    if filters.type:
        clauses.append("p.product_type = ?")
        params.append(filters.type)
    if filters.set:
        clauses.append("p.set_slug = ?")
        params.append(filters.set)
    if filters.category:
        clauses.append(
            "p.id IN (SELECT pc.product_id FROM product_categories pc "
            "JOIN categories c ON c.id = pc.category_id WHERE c.slug = ?)"
        )
        params.append(filters.category)
    # Every word must appear somewhere in the product's search text. LIKE's wildcards in the
    # query are escaped so "100%" searches for the text, not a pattern.
    for word in classify.fold(filters.q).split():
        escaped = word.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        clauses.append("p.search_text LIKE ? ESCAPE '\\'")
        params.append(f"%{escaped}%")
    return clauses, params


def list_products(conn: sqlite3.Connection, filters: Filters) -> Page:
    filters.clean()
    clauses, params = _where(filters)
    where = " AND ".join(clauses)
    total = conn.execute(f"SELECT count(*) FROM products p WHERE {where}", params).fetchone()[0]
    pages = max(1, -(-total // PAGE_SIZE))
    page = min(filters.page, pages)
    rows = conn.execute(
        f"""SELECT p.*, (SELECT file_name FROM product_images i WHERE i.product_id = p.id
                         ORDER BY sort_order LIMIT 1) AS image
            FROM products p WHERE {where}
            ORDER BY {IN_STOCK_FIRST}, {SORTS[filters.sort]} LIMIT ? OFFSET ?""",
        (*params, PAGE_SIZE, (page - 1) * PAGE_SIZE),
    ).fetchall()
    return Page([_row(r) for r in rows], total, page, pages, facets(conn, filters))


def facets(conn: sqlite3.Connection, filters: Filters) -> dict:
    """Counts for each filter option, given the other filters (so options never lead to zero)."""
    out = {}
    for key, column in (("lang", "language"), ("type", "product_type"), ("set", "set_slug")):
        others = Filters(**{**filters.__dict__, key: ""})
        clauses, params = _where(others)
        label = ", max(p.set_name)" if key == "set" else ""
        rows = conn.execute(
            f"SELECT p.{column} AS value, count(*) AS n{label} FROM products p "
            f"WHERE {' AND '.join(clauses)} AND p.{column} IS NOT NULL GROUP BY p.{column} ORDER BY n DESC",
            params,
        ).fetchall()
        options = []
        for r in rows:
            if key == "lang":
                name = classify.LANGUAGE_LABELS.get(r["value"], r["value"])
            elif key == "type":
                name = classify.PRODUCT_TYPE_LABELS.get(r["value"], r["value"])
            else:
                name = r[2]
            options.append({"value": r["value"], "name": name, "count": r["n"]})
        out[key] = sorted(options, key=lambda o: o["name"]) if key == "set" else options
    return out


def get_product(conn: sqlite3.Connection, slug: str) -> dict | None:
    row = conn.execute("SELECT * FROM products WHERE slug = ? AND visible = 1", (slug,)).fetchone()
    if row is None:
        return None
    product = _row(row)
    product["images"] = [dict(r) for r in conn.execute(
        "SELECT file_name, alt_text FROM product_images WHERE product_id = ? ORDER BY sort_order, id",
        (row["id"],))]
    product["image"] = product["images"][0]["file_name"] if product["images"] else None
    product["tags"] = [r["name"] for r in conn.execute(
        "SELECT t.name FROM tags t JOIN product_tags pt ON pt.tag_id = t.id WHERE pt.product_id = ? ORDER BY t.name",
        (row["id"],))]
    category = conn.execute(
        "SELECT c.slug, c.name FROM categories c JOIN product_categories pc ON pc.category_id = c.id "
        "WHERE pc.product_id = ? ORDER BY c.sort_order LIMIT 1", (row["id"],)).fetchone()
    product["category"] = dict(category) if category else None
    return product


def related(conn: sqlite3.Connection, product: dict, limit: int = 4) -> list[dict]:
    """Other in-stock items from the same set, else the same type."""
    rows = conn.execute(
        f"""SELECT p.*, (SELECT file_name FROM product_images i WHERE i.product_id = p.id
                         ORDER BY sort_order LIMIT 1) AS image
            FROM products p WHERE p.visible = 1 AND p.id != ?
            ORDER BY (p.set_slug IS NOT NULL AND p.set_slug = ?) DESC, (p.product_type = ?) DESC,
                     {IN_STOCK_FIRST}, p.created_at DESC LIMIT ?""",
        (product["id"], product.get("set_slug"), product["product_type"], limit),
    ).fetchall()
    return [_row(r) for r in rows]


def get_category(conn: sqlite3.Connection, slug: str) -> dict | None:
    row = conn.execute("SELECT * FROM categories WHERE slug = ?", (slug,)).fetchone()
    return dict(row) if row else None


def nav_categories(conn: sqlite3.Connection) -> list[dict]:
    """Categories that have at least one visible product, in menu order."""
    return [dict(r) for r in conn.execute(
        """SELECT c.slug, c.name, count(p.id) AS n FROM categories c
           JOIN product_categories pc ON pc.category_id = c.id
           JOIN products p ON p.id = pc.product_id AND p.visible = 1
           GROUP BY c.id ORDER BY c.sort_order""")]


def list_sets(conn: sqlite3.Connection) -> list[dict]:
    """Sets with visible products, most recently added first."""
    sets = []
    for r in conn.execute(
        """SELECT set_slug AS slug, max(set_name) AS name, count(*) AS n,
                  group_concat(DISTINCT language) AS languages
           FROM products WHERE visible = 1 AND set_slug IS NOT NULL
           GROUP BY set_slug ORDER BY max(created_at) DESC"""):
        item = dict(r)
        codes = sorted((item.pop("languages") or "").split(","))
        item["language_names"] = [classify.LANGUAGE_LABELS[c] for c in codes if c in classify.LANGUAGE_LABELS]
        sets.append(item)
    return sets


def get_set(conn: sqlite3.Connection, slug: str) -> dict | None:
    row = conn.execute(
        "SELECT set_slug AS slug, max(set_name) AS name, count(*) AS n FROM products "
        "WHERE visible = 1 AND set_slug = ? GROUP BY set_slug", (slug,)).fetchone()
    return dict(row) if row else None


def home_sections(conn: sqlite3.Connection, limit: int = 8) -> dict:
    base = """SELECT p.*, (SELECT file_name FROM product_images i WHERE i.product_id = p.id
                           ORDER BY sort_order LIMIT 1) AS image
              FROM products p WHERE p.visible = 1 AND (p.track_stock = 0 OR p.stock > 0)"""
    featured = conn.execute(f"{base} AND p.featured = 1 ORDER BY p.created_at DESC LIMIT ?", (limit,)).fetchall()
    if not featured:
        # Nothing marked featured yet (the admin arrives in Phase 3): show the priciest boxes.
        featured = conn.execute(f"{base} ORDER BY p.price_cents DESC LIMIT ?", (limit,)).fetchall()
    newest = conn.execute(f"{base} ORDER BY p.created_at DESC LIMIT ?", (limit,)).fetchall()
    return {"featured": [_row(r) for r in featured], "newest": [_row(r) for r in newest]}


def sitemap_entries(conn: sqlite3.Connection) -> dict:
    return {
        "products": [dict(r) for r in conn.execute(
            "SELECT slug, updated_at FROM products WHERE visible = 1 ORDER BY id")],
        "categories": nav_categories(conn),
        "sets": list_sets(conn),
    }


def product_redirect(conn: sqlite3.Connection, old_path: str) -> str | None:
    row = conn.execute(
        "SELECT p.slug, p.visible FROM product_redirects r JOIN products p ON p.id = r.product_id "
        "WHERE r.old_path = ?", (old_path,)).fetchone()
    if row is None:
        return None
    # A hidden product's old address goes to the shop rather than to a 404.
    return f"/p/{row['slug']}" if row["visible"] else "/shop"
