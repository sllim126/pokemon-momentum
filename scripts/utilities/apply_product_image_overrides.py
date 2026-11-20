"""Give products that have no TCGplayer picture a picture of our own, or a stand-in.

About 60 products have no picture on TCGplayer at any size (their image link returns 403):
mostly cases and multi-packs, and Master Ball pattern cards. This script swaps in a
better `imageUrl` for those products in the product catalog (the `<slug>_products.csv`
export and the `<slug>_products` DuckDB table). Every page and snapshot reads its picture
from there, so nothing else has to change.

Two sources, in order of preference:

1. **Our own picture**: a file `images/products/<productId>.jpg` (or .jpeg, .png, .webp)
   in the repo. It is served by the site at `/images/products/<file>`. Drop a file in
   and it wins over any stand-in.
2. **A stand-in**: a row in `scripts/dashboards/product_image_standins.csv` naming another
   product whose TCGplayer picture is close enough (one item for a case, the Poke Ball
   pattern for a Master Ball card). The `why` column says what the stand-in shows.

It runs at the end of `export_products_for_my_groups.py` each night (so the snapshots
built after it pick the pictures up) and can be run by hand:

    docker-compose exec -T pokemon-momentum python scripts/utilities/apply_product_image_overrides.py

Removing a stand-in row doesn't restore TCGplayer's (broken) link straight away; the
product gets it back the next time its set's product list is refetched.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common.category_config import get_category_config

OWN_IMAGE_DIR = ROOT / "images" / "products"
OWN_IMAGE_URL = "/images/products/"
OWN_IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp")
STANDINS_CSV = ROOT / "scripts" / "dashboards" / "product_image_standins.csv"
DATA_DIR = ROOT / "data" / "extracted"
DB_PATH = ROOT / "data" / "processed" / "prices_db.duckdb"
# English and Japanese Pokemon: the categories the market site shows.
DEFAULT_CATEGORIES = (3, 85)


def own_images(image_dir: Path = OWN_IMAGE_DIR) -> dict[int, str]:
    """productId -> site URL for every picture we've added ourselves."""
    found: dict[int, str] = {}
    if not image_dir.is_dir():
        return found
    for path in sorted(image_dir.iterdir()):
        if path.suffix.lower() in OWN_IMAGE_EXTENSIONS and path.stem.isdigit():
            found[int(path.stem)] = OWN_IMAGE_URL + path.name
    return found


def standins(csv_path: Path = STANDINS_CSV) -> dict[int, int]:
    """productId -> stand-in productId from the hand-kept list."""
    if not csv_path.exists():
        return {}
    with csv_path.open(newline="", encoding="utf-8") as handle:
        return {
            int(row["productId"]): int(row["standin_productId"])
            for row in csv.DictReader(handle)
            if (row.get("productId") or "").strip() and (row.get("standin_productId") or "").strip()
        }


def resolve_overrides(image_urls: dict[int, str], own: dict[int, str], subs: dict[int, int]) -> dict[int, str]:
    """Work out the new imageUrl for each product that should change.

    `image_urls` is the catalog's current productId -> imageUrl. A stand-in uses the
    stand-in product's picture as the catalog has it (which may itself be our own file).
    """
    overrides: dict[int, str] = {}
    for product_id, standin_id in subs.items():
        url = own.get(standin_id) or image_urls.get(standin_id)
        if product_id in image_urls and url:
            overrides[product_id] = url
    for product_id, url in own.items():
        if product_id in image_urls:
            overrides[product_id] = url
    return {pid: url for pid, url in overrides.items() if image_urls.get(pid) != url}


def apply_category(category_id: int, own: dict[int, str], subs: dict[int, int]) -> int:
    """Patch one category's CSV export and DuckDB table. Returns how many products changed."""
    category = get_category_config(category_id)
    csv_path = DATA_DIR / category.products_csv
    if not csv_path.exists():
        print(f"{category.label}: no {csv_path.name}, skipped")
        return 0
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        rows = list(reader)
    image_urls = {int(r["productId"]): r.get("imageUrl") or "" for r in rows if (r.get("productId") or "").isdigit()}
    overrides = resolve_overrides(image_urls, own, subs)
    if not overrides:
        print(f"{category.label}: pictures already up to date")
        return 0

    for row in rows:
        pid = row.get("productId") or ""
        if pid.isdigit() and int(pid) in overrides:
            row["imageUrl"] = overrides[int(pid)]
    tmp = csv_path.with_suffix(".csv.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(csv_path)

    # In the nightly update /app/data/processed is the copy being built, so this writes the
    # copy. Run by hand while the website is up, the live file is locked: the CSV (which most
    # routes read) is still patched and the table catches up at the next nightly update.
    try:
        con = duckdb.connect(str(DB_PATH))
    except duckdb.Error as exc:
        print(f"{category.label}: {len(overrides)} pictures replaced in {csv_path.name}; "
              f"database busy ({type(exc).__name__}), table updates at the next nightly run")
        return len(overrides)
    try:
        has_table = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [category.products_table]
        ).fetchone()[0]
        if has_table:
            con.executemany(
                f"UPDATE {category.products_table} SET imageUrl = ? WHERE productId = ?",
                [(url, pid) for pid, url in overrides.items()],
            )
    finally:
        con.close()
    print(f"{category.label}: {len(overrides)} product pictures replaced")
    return len(overrides)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--category-id", type=int, action="append",
                        help="Category to patch (repeatable). Default: English and Japanese Pokemon.")
    args = parser.parse_args()
    own, subs = own_images(), standins()
    for category_id in args.category_id or DEFAULT_CATEGORIES:
        apply_category(category_id, own, subs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
