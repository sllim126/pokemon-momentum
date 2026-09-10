#!/usr/bin/env python3
"""
Build an import-ready Squarespace CSV for sealed listing cleanup.

This uses an existing Squarespace export as the base so hosted image URLs and
current product ids stay intact, then replaces the fields we want to improve:
- Title
- Description
- Categories
- Tags
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "output"
DEFAULT_BASE_CSV = REPO_ROOT / "products_Apr-09_04-31-18PM.csv"
DEFAULT_CREATED_CSV = OUTPUT_DIR / "squarespace_created_sealed_listings.csv"
DEFAULT_UPDATE_SHEET_CSV = OUTPUT_DIR / "squarespace_sealed_inventory_update_sheet.csv"
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "squarespace_sealed_import_update.csv"

IMPORT_FIELDS = [
    "Product ID [Non Editable]",
    "Variant ID [Non Editable]",
    "Product Type [Non Editable]",
    "Product Page",
    "Product URL",
    "Title",
    "Description",
    "SKU",
    "GTIN",
    "MPN",
    "Price",
    "Sale Price",
    "On Sale",
    "Stock",
    "Categories",
    "Tags",
    "Weight",
    "Length",
    "Width",
    "Height",
    "Visible",
    "Hosted Image URLs",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an import-ready Squarespace CSV for sealed listing cleanup."
    )
    parser.add_argument("--base-csv", default=str(DEFAULT_BASE_CSV))
    parser.add_argument("--created-csv", default=str(DEFAULT_CREATED_CSV))
    parser.add_argument("--update-sheet-csv", default=str(DEFAULT_UPDATE_SHEET_CSV))
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV))
    return parser.parse_args()


def clean_text(value: str | None) -> str:
    return " ".join(str(value or "").strip().split())


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t" if path.suffix == ".txt" else ","))


def write_csv(path: Path, fieldnames: Iterable[str], rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_categories_path_string(product_type: str, set_name: str, title: str) -> str:
    normalized_type = clean_text(product_type)
    normalized_set = clean_text(set_name)
    normalized_title = clean_text(title).lower()

    set_slug_map = {
        "ME: Ascended Heroes": "/mega-evolution/ascended-heroes",
        "ME03: Perfect Order": "/mega-evolution/perfect-order",
        "ME04: Chaos Rising": "/mega-evolution/chaos-rising",
        "ME05: Pitch Black": "/mega-evolution/pitch-black",
        "SV: Scarlet & Violet 151": "/scarlet-violet/sv-151",
        "SV: Black Bolt": "/scarlet-violet/black-bolt",
        "SWSH: Crown Zenith": "/sword-shield/crown-zenith",
    }
    if normalized_type == "booster_box":
        categories = ["/booster-box", "/booster-box/english"]
    elif normalized_type == "booster_bundle":
        categories = ["/booster-bundle"]
    elif normalized_type in {"etb", "pokemon_center_etb"}:
        categories = ["/elite-trainer-box"]
    elif normalized_type in {"collection_box", "premium_collection"}:
        categories = ["/other/collection-boxes"]
    elif normalized_type == "two_pack_blister":
        categories = ["/booster-pack", "/booster-pack/english"]
    elif normalized_type in {"bundle_combo", "tin"}:
        categories = ["/other/tins" if normalized_type == "tin" else "/other/other"]
    else:
        categories = ["/other/other"]

    if "costco" in normalized_title and "/other/other" not in categories:
        categories.append("/other/other")

    set_slug = set_slug_map.get(normalized_set)
    if set_slug and set_slug not in categories:
        categories.append(set_slug)

    unique: list[str] = []
    for category in categories:
        if category and category not in unique:
            unique.append(category)
    return ", ".join(unique)


def build_tags_string(recommended_tags: str) -> str:
    tags = [clean_text(part) for part in str(recommended_tags or "").split(",") if clean_text(part)]
    # Keep tags broad and useful for search/maintenance rather than mirroring
    # every category on the product.
    filtered: list[str] = []
    skip_exact = {
        "Booster Box",
        "Booster Bundle",
        "Elite Trainer Box",
        "Pokemon Center Elite Trainer Box",
        "Collection Box",
        "Premium Collection",
        "Tin",
        "2-Pack Blister",
    }
    for tag in tags:
        if tag in skip_exact:
            continue
        filtered.append(tag)
    tags = filtered
    return ", ".join(tags)


def build_row(
    *,
    base_row: dict[str, str],
    created_row: dict[str, str],
    update_row: dict[str, str],
) -> dict[str, str]:
    title = clean_text(created_row.get("title"))
    product_type = clean_text(created_row.get("product_type"))
    set_name = clean_text(update_row.get("set_name"))
    description_html = str(update_row.get("description_html") or "").strip()
    categories = build_categories_path_string(product_type, set_name, title)
    tags = build_tags_string(update_row.get("recommended_tags"))

    product_url = clean_text(base_row.get("Product URL")) or clean_text(created_row.get("url_slug"))
    price = clean_text(created_row.get("target_price")) or clean_text(base_row.get("Price"))
    stock = clean_text(created_row.get("quantity")) or clean_text(base_row.get("Stock"))
    visible = clean_text(base_row.get("Visible")) or "No"
    sale_price = clean_text(base_row.get("Sale Price")) or "0"
    on_sale = clean_text(base_row.get("On Sale")) or "No"
    hosted_images = str(base_row.get("Hosted Image URLs") or "").strip()

    return {
        "Product ID [Non Editable]": clean_text(created_row.get("product_id")),
        "Variant ID [Non Editable]": clean_text(created_row.get("variant_id")),
        "Product Type [Non Editable]": clean_text(base_row.get("Product Type [Non Editable]")) or "PHYSICAL",
        "Product Page": clean_text(base_row.get("Product Page")) or "shop",
        "Product URL": product_url,
        "Title": title,
        "Description": description_html,
        "SKU": clean_text(created_row.get("sku")),
        "GTIN": clean_text(base_row.get("GTIN")),
        "MPN": clean_text(base_row.get("MPN")),
        "Price": price,
        "Sale Price": sale_price,
        "On Sale": on_sale,
        "Stock": stock,
        "Categories": categories,
        "Tags": tags,
        "Weight": clean_text(base_row.get("Weight")) or "0",
        "Length": clean_text(base_row.get("Length")) or "0",
        "Width": clean_text(base_row.get("Width")) or "0",
        "Height": clean_text(base_row.get("Height")) or "0",
        "Visible": visible,
        "Hosted Image URLs": hosted_images,
    }


def main() -> int:
    args = parse_args()
    base_csv = Path(args.base_csv)
    created_csv = Path(args.created_csv)
    update_sheet_csv = Path(args.update_sheet_csv)
    output_csv = Path(args.output_csv)

    if not base_csv.exists():
        raise SystemExit(f"Base CSV not found: {base_csv}")
    if not created_csv.exists():
        raise SystemExit(f"Created CSV not found: {created_csv}")
    if not update_sheet_csv.exists():
        raise SystemExit(f"Update sheet CSV not found: {update_sheet_csv}")

    base_rows = read_csv_rows(base_csv)
    created_rows = read_csv_rows(created_csv)
    update_rows = read_csv_rows(update_sheet_csv)

    base_by_sku = {clean_text(row.get("SKU")): row for row in base_rows}
    update_by_sku = {clean_text(row.get("sku")): row for row in update_rows}

    output_rows: list[dict[str, str]] = []
    for created_row in created_rows:
        sku = clean_text(created_row.get("sku"))
        base_row = base_by_sku.get(sku)
        if base_row is None:
            raise SystemExit(f"SKU missing from base CSV: {sku}")
        update_row = update_by_sku.get(sku)
        if update_row is None:
            raise SystemExit(f"SKU missing from update sheet CSV: {sku}")
        output_rows.append(
            build_row(
                base_row=base_row,
                created_row=created_row,
                update_row=update_row,
            )
        )

    write_csv(output_csv, IMPORT_FIELDS, output_rows)
    print(f"Wrote {len(output_rows)} rows to {output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
