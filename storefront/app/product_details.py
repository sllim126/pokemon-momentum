"""Adding products, editing their details, and their photos, from the admin (Phase 3, step 5b).

Price, visibility and stock changes are in product_changes.py; this module covers the rest of
a product: what it is (title, type, language, set, TCGplayer IDs, barcode, description) and its
photos. Every change is logged in product_changes (migration 006) with the old and new value.

Rules kept the same as the Squarespace importer (importer.py), so a product added here behaves
like an imported one:
- kind, category and shipping class follow the product type (classify.py PRODUCT_TYPES);
  "bulk" in a sealed product's title makes it a bulk lot.
- The set comes from the first TCGplayer ID's group in the market reference when not typed
  ("SV11B: Black Bolt" -> code sv11b, name Black Bolt).
- The slug comes from the title once, at creation, and never changes (a changed slug would break
  links people already have).
- New products start hidden: add photos and check the details, then show it.
- No singles (D10: singles stay on TCGplayer).

Photos are stored by images.py (original kept, 480 and 1200 px WebP copies, file names carry a
content hash). Functions that change the database take a `writer` connection and wrap the change
in BEGIN IMMEDIATE ... COMMIT.
"""

from __future__ import annotations

import io
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from PIL import Image, UnidentifiedImageError

from . import classify, images
from .market import MarketReference
from .orders import now_iso
from .product_changes import MAX_PRICE_CENTS, MIN_PRICE_CENTS, MAX_STOCK_STEP, clean_reason

# Types the admin offers (D10: no singles on poke6s.com), in the order the form lists them.
TYPES = [key for key in classify.PRODUCT_TYPES if key != "single"]
LANGUAGES = {"en": "English", "ja": "Japanese", "": "No language (accessory)"}
SKU = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{1,39}$")
GTIN = re.compile(r"^\d{8,14}$")
MAX_TITLE = 150
MAX_DESCRIPTION = 10_000
MAX_SET = 80
MAX_IDS = 20
MAX_PHOTO_BYTES = 15 * 1024 * 1024
MAX_PHOTOS = 12
PHOTO_FORMATS = {"JPEG": "jpg", "PNG": "png", "WEBP": "webp"}
NOT_A_SET = {"Miscellaneous Cards & Products"}   # same as importer.py


def parse_ids(text: str) -> list[str]:
    """'630431, 630432' or '630431|630432' -> ['630431', '630432']. Anything else -> ValueError."""
    ids = [part for part in re.split(r"[\s,|]+", text or "") if part]
    bad = [part for part in ids if not part.isdigit() or len(part) > 10]
    if bad:
        raise ValueError(f"TCGplayer IDs are numbers (from the product's TCGplayer address); “{bad[0][:20]}” isn't one.")
    if len(ids) > MAX_IDS:
        raise ValueError(f"At most {MAX_IDS} TCGplayer IDs.")
    return list(dict.fromkeys(ids))


def lookups(ids: list[str], market: MarketReference) -> list[dict]:
    """What the market reference knows about each ID, so the owner can see they typed the right one."""
    found = []
    for pid in ids:
        entry = market.entry(pid) or {}
        found.append({"id": pid, "name": entry.get("name"), "group": entry.get("group_name"),
                      "price": entry.get("price"), "url": entry.get("url")})
    return found


def already_listed(conn: sqlite3.Connection, ids: list[str]) -> list[dict]:
    """Products that already use any of these TCGplayer IDs, so the add page can warn before a
    duplicate is made (hidden products count: often the one to show again instead)."""
    found = {}
    for pid in ids:
        for row in conn.execute(
                "SELECT id, sku, title, visible, stock, track_stock FROM products "
                "WHERE '|' || tcgplayer_product_ids || '|' LIKE ?", (f"%|{pid}|%",)):
            found[row["id"]] = dict(row)
    return list(found.values())


def set_from_market(ids: list[str], market: MarketReference) -> tuple[str | None, str | None]:
    """(set_code, set_name) from the first ID whose TCGplayer group is a real set."""
    for pid in ids:
        group = (market.entry(pid) or {}).get("group_name")
        if group and group not in NOT_A_SET:
            return classify.split_group_name(group)
    return None, None


def suggest(ids: list[str], market: MarketReference) -> dict:
    """Title, type and set to pre-fill the add form from the first known TCGplayer ID."""
    for pid in ids:
        entry = market.entry(pid)
        if entry and entry.get("name"):
            title = entry["name"]
            language = "ja" if entry.get("market") == "JP" else "en"
            if LANGUAGES[language] not in title:
                title = f"{title} - {LANGUAGES[language]}"
            _code, set_name = set_from_market([pid], market)
            return {"title": title, "language": language, "set_name": set_name or "",
                    "product_type": classify.detect_product_type(title, [], [])}
    return {}


def _clean(fields: dict, market: MarketReference) -> dict:
    """Check and tidy the detail fields from a form. Raises ValueError with a message for the owner."""
    title = " ".join((fields.get("title") or "").split())[:MAX_TITLE]
    if len(title) < 3:
        raise ValueError("Give the product a title.")
    product_type = fields.get("product_type") or ""
    if product_type not in TYPES:
        raise ValueError("Pick what kind of product it is.")
    language = fields.get("language") or ""
    if language not in LANGUAGES:
        raise ValueError("Pick a language.")
    ids = parse_ids(fields.get("tcgplayer_ids") or "")
    set_name = " ".join((fields.get("set_name") or "").split())[:MAX_SET]
    set_code = None
    if ids:
        market_code, market_name = set_from_market(ids, market)
        if not set_name or set_name == market_name:
            set_code, set_name = market_code, set_name or market_name or ""
    gtin = "".join((fields.get("gtin") or "").split())
    if gtin and not GTIN.match(gtin):
        raise ValueError("The barcode (UPC/EAN) is 8 to 14 digits, or leave it empty.")
    description = (fields.get("description_md") or "").replace("\r\n", "\n").strip()[:MAX_DESCRIPTION]
    kind = classify.kind_for_type(product_type)
    if kind == "sealed" and re.search(r"\bbulk\b", classify.fold(title)):
        kind = "bulk"
    return {
        "title": title, "product_type": product_type, "kind": kind,
        "language": (language or None) if kind != "accessory" else None,
        "set_code": set_code, "set_name": set_name or None,
        "set_slug": classify.slugify(set_name) if set_name else None,
        "tcgplayer_product_ids": "|".join(ids), "gtin": gtin or None, "description_md": description,
        # "Oversize" (ticked on the form) overrides the type's class for big boxes; only sealed
        # products can be oversize.
        "ship_class": ("oversize" if fields.get("oversize") and kind in ("sealed", "bulk")
                       else classify.ship_class_for_type(product_type)),
    }


def _search_text(sku: str, clean: dict) -> str:
    return classify.search_text(clean["title"], clean["set_name"], sku,
                                classify.PRODUCT_TYPE_LABELS.get(clean["product_type"]),
                                classify.LANGUAGE_LABELS.get(clean["language"] or ""))


def _set_category(conn: sqlite3.Connection, product_id: int, product_type: str) -> None:
    conn.execute("DELETE FROM product_categories WHERE product_id = ?", (product_id,))
    conn.execute("INSERT INTO product_categories (product_id, category_id) SELECT ?, id FROM categories WHERE slug = ?",
                 (product_id, classify.category_for_type(product_type)))


def _log(conn: sqlite3.Connection, product_id: int, field: str, old, new, reason: str, at: str) -> None:
    conn.execute("INSERT INTO product_changes (product_id, field, old_value, new_value, reason, source, at) "
                 "VALUES (?, ?, ?, ?, ?, 'admin', ?)",
                 (product_id, field, None if old is None else str(old), None if new is None else str(new), reason, at))


def _unique_slug(conn: sqlite3.Connection, base: str) -> str:
    slug, n = base, 2
    while conn.execute("SELECT 1 FROM products WHERE slug = ?", (slug,)).fetchone():
        slug, n = f"{base}-{n}", n + 1
    return slug


def create_product(conn: sqlite3.Connection, fields: dict, market: MarketReference, *, price_cents: int | None,
                   stock: int, now: datetime | None = None) -> int:
    """Add a product, hidden, with its starting stock. Returns its id."""
    sku = (fields.get("sku") or "").strip()
    if not SKU.match(sku):
        raise ValueError("The SKU is 2 to 40 letters, digits and dashes, e.g. ENG-PO-ETB.")
    if price_cents is None or not MIN_PRICE_CENTS <= price_cents <= MAX_PRICE_CENTS:
        raise ValueError("The price must be between $0.50 and $10,000.")
    if not 0 <= stock <= MAX_STOCK_STEP:
        raise ValueError("Stock must be a whole number, 0 or more.")
    clean = _clean(fields, market)
    at = now_iso(now)
    conn.execute("BEGIN IMMEDIATE")
    try:
        if conn.execute("SELECT 1 FROM products WHERE sku = ? COLLATE NOCASE", (sku,)).fetchone():
            raise ValueError(f"There's already a product with SKU {sku}.")
        columns = {**clean, "sku": sku, "slug": _unique_slug(conn, classify.slugify(clean["title"])),
                   "price_cents": price_cents, "stock": stock, "track_stock": 1, "visible": 0,
                   "search_text": _search_text(sku, clean), "source": f"admin:{at[:10]}",
                   "created_at": at, "updated_at": at}
        product_id = conn.execute(
            f"INSERT INTO products ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
            tuple(columns.values())).lastrowid
        if stock:
            conn.execute("INSERT INTO stock_movements (product_id, change, reason, reference, at) "
                         "VALUES (?, ?, 'restock', 'Received: new product', ?)", (product_id, stock, at))
        _set_category(conn, product_id, clean["product_type"])
        _log(conn, product_id, "created", None, sku, "Added in the admin", at)
        conn.commit()
        return product_id
    except BaseException:
        conn.rollback()
        raise


# Form field -> (products column, product_changes field) for detail edits.
DETAIL_FIELDS = {
    "title": ("title", "title"), "description_md": ("description_md", "description"),
    "product_type": ("product_type", "product_type"), "language": ("language", "language"),
    "set_name": ("set_name", "set_name"), "tcgplayer_product_ids": ("tcgplayer_product_ids", "tcgplayer_ids"),
    "gtin": ("gtin", "gtin"),
}


def update_details(conn: sqlite3.Connection, product_id: int, fields: dict, market: MarketReference,
                   reason: str = "", now: datetime | None = None) -> list[str]:
    """Change what a product is. Returns the changed field names ([] if nothing changed)."""
    clean = _clean(fields, market)
    reason = clean_reason(reason)
    at = now_iso(now)
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
        if row is None:
            raise ValueError("That product doesn't exist")
        if row["kind"] == "single":
            raise ValueError("Singles aren't edited here (D10: singles stay on TCGplayer).")
        changed = [key for key in DETAIL_FIELDS if clean[key] != row[key]]
        # The class only changes when the Oversize box flips or the type/title does, so a class
        # an import set is never rewritten by an unrelated edit.
        was_oversize = row["ship_class"] == "oversize"
        ship_changed = (clean["ship_class"] == "oversize") != was_oversize or (
            {"product_type", "title"} & set(changed) and clean["ship_class"] != row["ship_class"])
        if ship_changed:
            changed.append("ship_class")
        if not changed:
            conn.rollback()
            return []
        # The derived columns follow along: kind and shipping class with the type (or a title
        # that now says "bulk"), the set code and slug with the set, the search text with all.
        # Only what the change touches, so an untouched imported value is never rewritten.
        columns = {key: clean[key] for key in changed if key in DETAIL_FIELDS}
        if {"product_type", "title"} & set(changed):
            columns.update(kind=clean["kind"])
        if ship_changed:
            columns.update(ship_class=clean["ship_class"])
        if {"set_name", "tcgplayer_product_ids"} & set(changed):
            columns.update(set_code=clean["set_code"], set_slug=clean["set_slug"])
        columns.update(search_text=_search_text(row["sku"], clean), updated_at=at)
        conn.execute(f"UPDATE products SET {', '.join(f'{k} = ?' for k in columns)} WHERE id = ?",
                     (*columns.values(), product_id))
        if "product_type" in changed:
            _set_category(conn, product_id, clean["product_type"])
        for key in changed:
            if key == "ship_class":
                _log(conn, product_id, "ship_class", row["ship_class"], clean["ship_class"], reason, at)
                continue
            old, new = row[key], clean[key]
            if key == "description_md":   # long text: the history notes the change, not the text
                old, new = f"{len(old or '')} characters", f"{len(new or '')} characters"
            _log(conn, product_id, DETAIL_FIELDS[key][1], old, new, reason, at)
        conn.commit()
        return changed
    except BaseException:
        conn.rollback()
        raise


def photos(conn: sqlite3.Connection, product_id: int) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM product_images WHERE product_id = ? ORDER BY sort_order, id", (product_id,))]


def check_photo(data: bytes) -> str:
    """The file extension for an uploaded photo, after checking it really is a JPEG, PNG or WebP."""
    if not data:
        raise ValueError("That file is empty.")
    if len(data) > MAX_PHOTO_BYTES:
        raise ValueError("Photos can be up to 15 MB.")
    try:
        with Image.open(io.BytesIO(data)) as image:
            image_format = image.format
            image.verify()
    except (UnidentifiedImageError, Image.DecompressionBombError, OSError, SyntaxError, ValueError):
        raise ValueError("That file isn't a photo the store can use (JPEG, PNG or WebP).") from None
    if image_format not in PHOTO_FORMATS:
        raise ValueError("Photos must be JPEG, PNG or WebP.")
    return PHOTO_FORMATS[image_format]


def add_photos(conn: sqlite3.Connection, images_dir: Path, product_id: int, uploads: list[bytes],
               now: datetime | None = None) -> int:
    """Store uploaded photos after the product's existing ones. All are checked before any is saved."""
    exts = [check_photo(data) for data in uploads]
    at = now_iso(now)
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT slug, title FROM products WHERE id = ?", (product_id,)).fetchone()
        if row is None:
            raise ValueError("That product doesn't exist")
        have = photos(conn, product_id)
        if len(have) + len(uploads) > MAX_PHOTOS:
            raise ValueError(f"A product can have up to {MAX_PHOTOS} photos.")
        order = max((p["sort_order"] for p in have), default=-1) + 1
        added = 0
        for data, ext in zip(uploads, exts):
            name, ext = images.store_original(images_dir, row["slug"], data, ext)
            cursor = conn.execute(
                "INSERT OR IGNORE INTO product_images (product_id, file_name, original_ext, source_url, alt_text, sort_order) "
                "VALUES (?, ?, ?, 'admin upload', ?, ?)", (product_id, name, ext, row["title"], order))
            if cursor.rowcount:   # the same photo uploaded twice is stored once
                _log(conn, product_id, "photo", None, name, "Uploaded", at)
                order, added = order + 1, added + 1
        conn.commit()
        return added
    except BaseException:
        conn.rollback()
        raise


def make_main_photo(conn: sqlite3.Connection, product_id: int, image_id: int, now: datetime | None = None) -> None:
    """Put one photo first (the one lists and the top of the product page show)."""
    at = now_iso(now)
    conn.execute("BEGIN IMMEDIATE")
    try:
        have = photos(conn, product_id)
        chosen = next((p for p in have if p["id"] == image_id), None)
        if chosen is None:
            raise ValueError("That photo isn't on this product.")
        for order, photo in enumerate([chosen] + [p for p in have if p["id"] != image_id]):
            conn.execute("UPDATE product_images SET sort_order = ? WHERE id = ?", (order, photo["id"]))
        _log(conn, product_id, "photo", None, chosen["file_name"], "Made the main photo", at)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def remove_photo(conn: sqlite3.Connection, images_dir: Path, product_id: int, image_id: int,
                 now: datetime | None = None) -> None:
    """Take a photo off a product. Its resized copies are deleted when no other product uses them;
    the original stays in images/original (and the nightly backup), so a mistake can be undone."""
    at = now_iso(now)
    conn.execute("BEGIN IMMEDIATE")
    try:
        photo = conn.execute("SELECT * FROM product_images WHERE id = ? AND product_id = ?",
                             (image_id, product_id)).fetchone()
        if photo is None:
            raise ValueError("That photo isn't on this product.")
        conn.execute("DELETE FROM product_images WHERE id = ?", (image_id,))
        _log(conn, product_id, "photo", photo["file_name"], None, "Removed", at)
        still_used = conn.execute("SELECT 1 FROM product_images WHERE file_name = ?", (photo["file_name"],)).fetchone()
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    if not still_used:
        for width in images.SIZES:
            (images_dir / str(width) / f"{photo['file_name']}.webp").unlink(missing_ok=True)
