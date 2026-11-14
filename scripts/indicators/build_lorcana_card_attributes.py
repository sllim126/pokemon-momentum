#!/usr/bin/env python3
"""Build lorcana_card_attributes: the Lorcana fields the shared product table doesn't keep.

The product table (lorcana_products) has name, image, rarity and number, the same columns
as Pokemon. Lorcana pages also need the film a card comes from, its ink colour and card
type, and the promo type (docs/lorcana_market_plan.md, "Research views"). TCGCSV sends
them in each product's extendedData; this step reads them from local copies only:

1. data/extracted/lorcana_products_raw/<groupId>.json, saved by the nightly metadata step
   (export_products_for_my_groups.py) whenever it fetches a set, and
2. data/tcgcsv_catalogue/71/<groupId>/products.json, the daily catalogue crawl's copy
   (refreshed about monthly), used for any set the nightly step hasn't fetched yet.

For each set the newer of the two files wins. No TCGCSV requests are made.

Output, one row per product (cards only; sealed products have none of these fields):

    productId, groupId, property (film), ink (first ink), inks (all, "Amber;Steel"),
    card_type, classifications, promo_type, cost, lore, strength, willpower, rarity_tier

written to the DuckDB table lorcana_card_attributes (replaced each run) and
data/extracted/lorcana_card_attributes.csv. Run for category 71 by run_daily_update.py after
the product metadata refresh; by hand:

    python scripts/indicators/build_lorcana_card_attributes.py
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common.lorcana_rarity import lorcana_rarity_tier

DATA_DIR = Path("/app/data")
RAW_DIR = DATA_DIR / "extracted" / "lorcana_products_raw"
CATALOGUE_DIR = DATA_DIR / "tcgcsv_catalogue" / "71"
OUT_CSV = DATA_DIR / "extracted" / "lorcana_card_attributes.csv"
DB_PATH = DATA_DIR / "processed" / "prices_db.duckdb"
TABLE = "lorcana_card_attributes"

COLUMNS = [
    "productId", "groupId", "property", "ink", "inks", "card_type", "classifications",
    "promo_type", "cost", "lore", "strength", "willpower", "rarity_tier",
]
# TCGCSV extendedData name -> our column. "Classification" holds e.g. "Storyborn, Hero, Princess".
FIELD_MAP = {
    "Property": "property",
    "InkType": "inks",
    "CardType": "card_type",
    "Classification": "classifications",
    "PromoType": "promo_type",
    "Cost Ink": "cost",
    "Lore Value": "lore",
    "Strength": "strength",
    "Willpower": "willpower",
}
NUMERIC = {"cost", "lore", "strength", "willpower"}


def newest_product_files(raw_dir: Path, catalogue_dir: Path) -> dict[int, Path]:
    """groupId -> the newer of the nightly copy and the catalogue copy of its product list."""
    files: dict[int, Path] = {}
    candidates = list(catalogue_dir.glob("*/products.json")) if catalogue_dir.is_dir() else []
    candidates += list(raw_dir.glob("*.json")) if raw_dir.is_dir() else []
    for path in candidates:
        stem = path.parent.name if path.name == "products.json" else path.stem
        if not stem.isdigit():
            continue
        group_id = int(stem)
        if group_id not in files or path.stat().st_mtime > files[group_id].stat().st_mtime:
            files[group_id] = path
    return files


def to_int(value: str | None) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def attribute_row(group_id: int, product: dict) -> dict | None:
    """One output row, or None for products without any card fields (sealed products)."""
    extended = {e.get("name"): e.get("value") for e in product.get("extendedData") or []}
    row: dict = {"productId": product.get("productId"), "groupId": group_id}
    for source, column in FIELD_MAP.items():
        value = extended.get(source)
        value = value.strip() if isinstance(value, str) else value
        row[column] = to_int(value) if column in NUMERIC else (value or None)
    if row["productId"] is None or not any(row[c] is not None for c in FIELD_MAP.values()):
        return None
    row["ink"] = row["inks"].split(";")[0] if row["inks"] else None
    row["rarity_tier"] = lorcana_rarity_tier(extended.get("Rarity"))
    return row


def build_rows(files: dict[int, Path]) -> list[dict]:
    rows: dict[int, dict] = {}
    for group_id, path in sorted(files.items()):
        payload = json.loads(path.read_text(encoding="utf-8"))
        for product in payload.get("results", []):
            row = attribute_row(group_id, product)
            if row is not None:
                rows[row["productId"]] = row
    return [rows[k] for k in sorted(rows)]


def write_outputs(rows: list[dict], out_csv: Path, db_path: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    con = duckdb.connect(str(db_path))
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE {TABLE} (
                productId BIGINT, groupId BIGINT, property VARCHAR, ink VARCHAR, inks VARCHAR,
                card_type VARCHAR, classifications VARCHAR, promo_type VARCHAR, cost INTEGER,
                lore INTEGER, strength INTEGER, willpower INTEGER, rarity_tier VARCHAR
            )
        """)
        if rows:
            con.executemany(
                f"INSERT INTO {TABLE} VALUES ({', '.join('?' for _ in COLUMNS)})",
                [[row[c] for c in COLUMNS] for row in rows],
            )
    finally:
        con.close()


def main() -> int:
    files = newest_product_files(RAW_DIR, CATALOGUE_DIR)
    if not files:
        print("No Lorcana product lists found (nightly copies or catalogue); nothing built.")
        return 1
    rows = build_rows(files)
    write_outputs(rows, OUT_CSV, DB_PATH)
    nightly = sum(1 for p in files.values() if p.parent == RAW_DIR)
    print(f"{TABLE}: {len(rows)} cards from {len(files)} sets ({nightly} nightly copies, {len(files) - nightly} catalogue)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
