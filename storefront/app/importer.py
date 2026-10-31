"""Load the catalog from the Squarespace product export and the intake CSVs.

Run inside the storefront container:

    docker-compose exec -T storefront python -m storefront.app.importer \
        --export data/archive/squarespace_exports/<latest>.csv [--fetch-images] [--dry-run]

What it does (plan "Moving off Squarespace", step 3):
- one product per export row. Squarespace variant rows (blank product ID, e.g. desk-mat
  sizes) become products of their own until variants arrive in Phase 5;
- keeps SKUs; makes clean slugs from titles and records each old `/shop/p/<slug>` address in
  `product_redirects`, so it answers with a 301;
- TCGplayer product IDs come from `data/squarespace_tcgplayer_mapping.csv` and
  `data/new_sealed_intake.csv`; the set name comes from the market reference file;
- visibility: shown only if visible on Squarespace, priced, and in stock (or untracked).
  Singles are skipped unless --include-singles (owner decision D1: singles stay on TCGplayer),
  and even then they're imported hidden;
- stock changes are written as `import` stock movements, so stock always equals the sum of
  movements;
- --fetch-images downloads the photos from Squarespace's CDN (a plain read of public files)
  and makes the WebP copies. Without it no network request is made.

Re-running updates existing SKUs from the export (price, stock, visibility, text) but keeps
their slug and creation time. Once products are edited in the admin (Phase 3), run with
--only-new so the admin's edits aren't overwritten.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlparse

from . import classify, images
from .db import connect, migrate
from .markdown import html_to_markdown
from .settings import REPO_ROOT, load_settings

DEFAULT_MAPPING_CSV = REPO_ROOT / "data" / "squarespace_tcgplayer_mapping.csv"
DEFAULT_SEALED_INTAKE_CSV = REPO_ROOT / "data" / "new_sealed_intake.csv"
EXPORT_DIR = REPO_ROOT / "data" / "archive" / "squarespace_exports"

# Intake `product_type` values -> our product types.
INTAKE_TYPES = {
    "booster_box": "booster-box",
    "booster_bundle": "booster-bundle",
    "booster_pack": "booster-pack",
    "two_pack_blister": "booster-pack",
    "etb": "elite-trainer-box",
    "pokemon_center_etb": "elite-trainer-box",
    "collection_box": "collection-box",
    "premium_collection": "collection-box",
    "tin": "tin",
}

# TCGplayer groups that hold products from many sets; never shown as a set.
NOT_A_SET = {"Miscellaneous Cards & Products"}

IMAGE_FETCH_DELAY_SECONDS = 0.25
USER_AGENT = "Poke6s storefront importer (+https://market.poke6s.com)"


@dataclass
class ImportRow:
    sku: str
    title: str
    old_slug: str
    description_md: str
    price_cents: int
    compare_at_cents: int | None
    stock: int
    track_stock: bool
    visible_on_squarespace: bool
    tags: list[str]
    categories: list[str]
    weight_oz: float | None
    length_in: float | None
    width_in: float | None
    height_in: float | None
    gtin: str
    image_urls: list[str]
    source: str
    product_type: str = "other"
    kind: str = "sealed"
    language: str | None = None
    tcgplayer_product_ids: str = ""
    set_code: str | None = None
    set_name: str | None = None


@dataclass
class ImportReport:
    created: int = 0
    updated: int = 0
    skipped: list[str] = field(default_factory=list)
    visible: int = 0
    images_fetched: int = 0
    image_errors: list[str] = field(default_factory=list)

    def lines(self) -> list[str]:
        out = [
            f"created {self.created}, updated {self.updated}, visible {self.visible}",
            f"skipped {len(self.skipped)}",
            f"images fetched {self.images_fetched}, image errors {len(self.image_errors)}",
        ]
        out += [f"  image error: {e}" for e in self.image_errors[:20]]
        return out


def now_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_cents(value: str) -> int | None:
    value = (value or "").strip().replace("$", "").replace(",", "")
    if not value:
        return None
    try:
        return int(round(float(value) * 100))
    except ValueError:
        return None


def to_float(value: str) -> float | None:
    try:
        number = float((value or "").strip())
    except ValueError:
        return None
    return number if number > 0 else None


def split_list(value: str) -> list[str]:
    return [part.strip() for part in (value or "").split(",") if part.strip()]


def latest_export() -> Path | None:
    exports = sorted(EXPORT_DIR.glob("squarespace_export_*.csv"))
    return exports[-1] if exports else None


def load_tcgplayer_ids(mapping_csv: Path, intake_csv: Path) -> tuple[dict[str, list[str]], dict[str, dict]]:
    """SKU -> TCGplayer product IDs, and SKU -> sealed intake row."""
    ids: dict[str, list[str]] = {}
    for path in (mapping_csv, intake_csv):
        if not path.exists():
            continue
        with path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                sku = (row.get("sku") or "").strip()
                for pid in (row.get("tcgplayer_product_id") or "").split("|"):
                    pid = pid.strip()
                    if sku and pid and pid not in ids.setdefault(sku, []):
                        ids[sku].append(pid)
    intake: dict[str, dict] = {}
    if intake_csv.exists():
        with intake_csv.open(encoding="utf-8-sig", newline="") as handle:
            intake = {r["sku"].strip(): r for r in csv.DictReader(handle) if (r.get("sku") or "").strip()}
    return ids, intake


def read_export(path: Path) -> list[ImportRow]:
    """Parse the export. Variant rows (blank product ID) inherit the parent's text and photos."""
    with path.open(encoding="utf-8-sig", newline="") as handle:
        raw_rows = list(csv.DictReader(handle))

    # Group a product row with the variant rows that follow it.
    groups: list[list[dict]] = []
    for raw in raw_rows:
        if (raw.get("Product ID [Non Editable]") or "").strip() or not groups:
            groups.append([raw])
        else:
            groups[-1].append(raw)

    rows: list[ImportRow] = []
    for group in groups:
        parent = group[0]
        for raw in group:
            sku = (raw.get("SKU") or "").strip()
            if not sku:
                continue
            title = (parent.get("Title") or "").strip()
            if len(group) > 1:
                option = " ".join(
                    (raw.get(f"Option Value {n}") or "").strip() for n in range(1, 7)
                ).strip()
                if option:
                    title = f"{title} – {option}"
            stock_text = (raw.get("Stock") or "").strip()
            unlimited = stock_text.lower() == "unlimited"
            price = to_cents(raw.get("Price") or "") or 0
            sale = to_cents(raw.get("Sale Price") or "")
            on_sale = (raw.get("On Sale") or "").strip().lower() == "yes" and sale
            weight_lb = to_float(raw.get("Weight") or parent.get("Weight") or "")
            rows.append(ImportRow(
                sku=sku,
                title=title,
                old_slug=(parent.get("Product URL") or "").strip(),
                description_md=html_to_markdown(parent.get("Description") or ""),
                price_cents=sale if on_sale else price,
                compare_at_cents=price if on_sale and price > (sale or 0) else None,
                stock=0 if unlimited else max(0, int(float(stock_text or 0))),
                track_stock=not unlimited,
                visible_on_squarespace=(parent.get("Visible") or "").strip().lower() == "yes",
                tags=split_list(parent.get("Tags") or ""),
                categories=split_list(parent.get("Categories") or ""),
                # Squarespace exports weight in pounds.
                weight_oz=round(weight_lb * 16, 2) if weight_lb else None,
                length_in=to_float(raw.get("Length") or ""),
                width_in=to_float(raw.get("Width") or ""),
                height_in=to_float(raw.get("Height") or ""),
                gtin=(raw.get("GTIN") or "").strip(),
                image_urls=(parent.get("Hosted Image URLs") or "").split(),
                source=f"squarespace:{(raw.get('Variant ID [Non Editable]') or '').strip()}",
            ))
    return rows


def classify_row(row: ImportRow, tcg_ids: dict[str, list[str]], intake: dict[str, dict], market: dict) -> None:
    folded_title = classify.fold(row.title)
    intake_row = intake.get(row.sku) or {}
    product_type = INTAKE_TYPES.get((intake_row.get("product_type") or "").strip())
    if not product_type:
        product_type = classify.detect_product_type(row.title, row.tags, row.categories, row.sku)
    row.product_type = product_type
    row.kind = classify.kind_for_type(product_type)
    # Bulk lots ("Japanese AR Bulk (100 Cards)"); "Binders & Bulk" in an accessory title isn't one.
    if row.kind == "sealed" and re.search(r"\bbulk\b", folded_title):
        row.kind = "bulk"
    row.language = classify.detect_language(row.title, row.sku) if row.kind != "accessory" else None
    ids = tcg_ids.get(row.sku) or ([row.sku.split("-")[0]] if row.kind == "single" else [])
    row.tcgplayer_product_ids = "|".join(ids)

    # Set name: the TCGplayer group of the first mapped product, else the Squarespace set
    # category (e.g. /mega-evolution/perfect-order -> "Perfect Order").
    products = (market or {}).get("products") or {}
    for pid in ids:
        entry = products.get(pid)
        if entry and entry.get("group_name") and entry["group_name"] not in NOT_A_SET:
            row.set_code, row.set_name = classify.split_group_name(entry["group_name"])
            break
    if not row.set_name:
        for path in row.categories:
            parts = [p for p in path.strip("/").split("/") if p]
            if len(parts) == 2 and parts[0] in {"mega-evolution", "scarlet-violet", "sword-shield"}:
                row.set_name = parts[1].replace("-", " ").title().replace("Sv 151", "151")
                break


def unique_slug(conn: sqlite3.Connection, base: str, product_id: int | None = None) -> str:
    slug, n = base, 2
    while True:
        row = conn.execute("SELECT id FROM products WHERE slug = ?", (slug,)).fetchone()
        if row is None or row["id"] == product_id:
            return slug
        slug, n = f"{base}-{n}", n + 1


def set_tags(conn: sqlite3.Connection, product_id: int, tags: list[str]) -> None:
    conn.execute("DELETE FROM product_tags WHERE product_id = ?", (product_id,))
    for tag in dict.fromkeys(t for t in tags if t):
        conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag,))
        tag_id = conn.execute("SELECT id FROM tags WHERE name = ?", (tag,)).fetchone()["id"]
        conn.execute("INSERT OR IGNORE INTO product_tags VALUES (?, ?)", (product_id, tag_id))


def set_category(conn: sqlite3.Connection, product_id: int, product_type: str) -> None:
    conn.execute("DELETE FROM product_categories WHERE product_id = ?", (product_id,))
    conn.execute(
        "INSERT INTO product_categories (product_id, category_id) "
        "SELECT ?, id FROM categories WHERE slug = ?",
        (product_id, classify.category_for_type(product_type)),
    )


def record_stock(conn: sqlite3.Connection, product_id: int, new_stock: int, reference: str) -> None:
    """Move stock to new_stock with one `import` movement for the difference."""
    current = conn.execute("SELECT stock FROM products WHERE id = ?", (product_id,)).fetchone()["stock"]
    change = new_stock - current
    if change:
        conn.execute(
            "INSERT INTO stock_movements (product_id, change, reason, reference, at) VALUES (?, ?, 'import', ?, ?)",
            (product_id, change, reference, now_utc()),
        )
        conn.execute("UPDATE products SET stock = ? WHERE id = ?", (new_stock, product_id))


def fetch_images(conn: sqlite3.Connection, product_id: int, slug: str, title: str, urls: list[str],
                 images_dir: Path, report: ImportReport) -> None:
    import requests  # only needed with --fetch-images

    have = {r["source_url"] for r in conn.execute(
        "SELECT source_url FROM product_images WHERE product_id = ?", (product_id,))}
    for order, url in enumerate(urls):
        if url in have:
            continue
        try:
            response = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
            response.raise_for_status()
            ext = Path(urlparse(url).path).suffix or ".jpg"
            name, ext = images.store_original(images_dir, slug, response.content, ext)
        except Exception as exc:  # keep importing; report the photo
            report.image_errors.append(f"{slug}: {type(exc).__name__}")
            continue
        finally:
            time.sleep(IMAGE_FETCH_DELAY_SECONDS)
        conn.execute(
            "INSERT OR IGNORE INTO product_images (product_id, file_name, original_ext, source_url, alt_text, sort_order) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (product_id, name, ext, url, title, order),
        )
        report.images_fetched += 1


def import_rows(conn: sqlite3.Connection, rows: list[ImportRow], *, reference: str,
                include_singles: bool = False, only_new: bool = False,
                fetch: bool = False, images_dir: Path | None = None) -> ImportReport:
    report = ImportReport()
    # Rows later in the export were added to Squarespace later; spacing creation times by a
    # second keeps "newest first" in that order.
    base_time = datetime.now(UTC) - timedelta(seconds=len(rows))
    for index, row in enumerate(rows):
        if row.kind == "single" and not include_singles:
            report.skipped.append(f"{row.sku}: single (D1: singles stay on TCGplayer)")
            continue
        visible = (
            row.visible_on_squarespace
            and row.price_cents > 0
            and (row.stock > 0 or not row.track_stock)
            and row.kind != "single"
        )
        search = classify.search_text(
            row.title, row.set_name, row.sku, " ".join(row.tags),
            classify.PRODUCT_TYPE_LABELS.get(row.product_type),
            classify.LANGUAGE_LABELS.get(row.language or ""),
        )
        stamp = now_utc()
        existing = conn.execute("SELECT id, slug FROM products WHERE sku = ?", (row.sku,)).fetchone()
        if existing and only_new:
            continue
        fields = dict(
            title=row.title, kind=row.kind, product_type=row.product_type, language=row.language,
            set_code=row.set_code, set_name=row.set_name,
            set_slug=classify.slugify(row.set_name) if row.set_name else None,
            description_md=row.description_md, price_cents=row.price_cents,
            compare_at_cents=row.compare_at_cents, track_stock=int(row.track_stock),
            visible=int(visible), weight_oz=row.weight_oz, length_in=row.length_in,
            width_in=row.width_in, height_in=row.height_in,
            ship_class=classify.ship_class_for_type(row.product_type), gtin=row.gtin or None,
            tcgplayer_product_ids=row.tcgplayer_product_ids, search_text=search,
            source=row.source, updated_at=stamp,
        )
        if existing:
            product_id, slug = existing["id"], existing["slug"]
            conn.execute(
                f"UPDATE products SET {', '.join(f'{k} = ?' for k in fields)} WHERE id = ?",
                (*fields.values(), product_id),
            )
            report.updated += 1
        else:
            slug = unique_slug(conn, classify.slugify(row.title))
            created = (base_time + timedelta(seconds=index)).strftime("%Y-%m-%dT%H:%M:%SZ")
            columns = {"sku": row.sku, "slug": slug, "created_at": created, **fields}
            cursor = conn.execute(
                f"INSERT INTO products ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
                tuple(columns.values()),
            )
            product_id = cursor.lastrowid
            report.created += 1
        record_stock(conn, product_id, row.stock, reference)
        set_tags(conn, product_id, row.tags)
        set_category(conn, product_id, row.product_type)
        if row.old_slug:
            conn.execute(
                "INSERT OR IGNORE INTO product_redirects (old_path, product_id) VALUES (?, ?)",
                (f"/shop/p/{row.old_slug}", product_id),
            )
        if fetch and images_dir is not None and row.image_urls:
            fetch_images(conn, product_id, slug, row.title, row.image_urls, images_dir, report)
        report.visible += int(visible)
    return report


def run(export: Path, *, db_path: Path, market_reference: Path, images_dir: Path,
        mapping_csv: Path = DEFAULT_MAPPING_CSV, intake_csv: Path = DEFAULT_SEALED_INTAKE_CSV,
        include_singles: bool = False, only_new: bool = False, fetch: bool = False,
        dry_run: bool = False) -> ImportReport:
    migrate(db_path)
    tcg_ids, intake = load_tcgplayer_ids(mapping_csv, intake_csv)
    market = json.loads(market_reference.read_text(encoding="utf-8")) if market_reference.exists() else {}
    rows = read_export(export)
    for row in rows:
        classify_row(row, tcg_ids, intake, market)
    conn = connect(db_path)
    try:
        conn.execute("BEGIN")
        report = import_rows(conn, rows, reference=export.name, include_singles=include_singles,
                             only_new=only_new, fetch=fetch and not dry_run, images_dir=images_dir)
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return report


def main(argv: list[str] | None = None) -> int:
    settings = load_settings()
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--export", type=Path, default=None, help="Squarespace product export CSV (default: latest in data/archive/squarespace_exports)")
    parser.add_argument("--mapping", type=Path, default=DEFAULT_MAPPING_CSV)
    parser.add_argument("--sealed-intake", type=Path, default=DEFAULT_SEALED_INTAKE_CSV)
    parser.add_argument("--include-singles", action="store_true", help="Import singles too (always hidden; D1)")
    parser.add_argument("--only-new", action="store_true", help="Leave existing SKUs untouched")
    parser.add_argument("--fetch-images", action="store_true", help="Download photos from Squarespace's CDN")
    parser.add_argument("--dry-run", action="store_true", help="Report what would change, then roll back")
    args = parser.parse_args(argv)

    export = args.export or latest_export()
    if export is None or not export.exists():
        print("No Squarespace export found.", file=sys.stderr)
        return 1
    report = run(
        export, db_path=settings.db_path, market_reference=settings.market_reference_path,
        images_dir=settings.images_dir, mapping_csv=args.mapping, intake_csv=args.sealed_intake,
        include_singles=args.include_singles, only_new=args.only_new,
        fetch=args.fetch_images, dry_run=args.dry_run,
    )
    print(f"{'[dry run] ' if args.dry_run else ''}import from {export.name}")
    for line in report.lines():
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
