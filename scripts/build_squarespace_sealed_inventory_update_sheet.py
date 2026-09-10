#!/usr/bin/env python3
"""
Build a manual-review sheet for newly created Squarespace sealed listings.

This sheet is intended to help with post-create cleanup inside Squarespace by
giving the operator cleaner recommended tags, categories, and descriptions.
"""

from __future__ import annotations

import argparse
import csv
import html
import re
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "output"
DEFAULT_CREATED_CSV = OUTPUT_DIR / "squarespace_created_sealed_listings.csv"
DEFAULT_DRAFT_CSV = OUTPUT_DIR / "squarespace_sealed_listing_drafts.csv"
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "squarespace_sealed_inventory_update_sheet.csv"

OUTPUT_FIELDS = [
    "sku",
    "title",
    "squarespace_url",
    "product_type",
    "set_name",
    "quantity",
    "recommended_tags",
    "recommended_categories",
    "description_plain",
    "description_html",
    "notes",
]

PRODUCT_TYPE_LABELS = {
    "booster_box": "Booster Box",
    "booster_bundle": "Booster Bundle",
    "bundle_combo": "Bundle Combo",
    "collection_box": "Collection Box",
    "etb": "Elite Trainer Box",
    "pokemon_center_etb": "Pokemon Center Elite Trainer Box",
    "premium_collection": "Premium Collection",
    "tin": "Tin",
    "two_pack_blister": "2-Pack Blister",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a manual review sheet for sealed Squarespace listings."
    )
    parser.add_argument("--created-csv", default=str(DEFAULT_CREATED_CSV))
    parser.add_argument("--draft-csv", default=str(DEFAULT_DRAFT_CSV))
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV))
    return parser.parse_args()


def clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: Iterable[str], rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def display_set_name(raw_set_name: str) -> str:
    value = clean_text(raw_set_name)
    if not value or value.lower() == "miscellaneous cards & products":
        return ""
    return value


def extract_set_tag(raw_set_name: str) -> str:
    value = display_set_name(raw_set_name)
    if not value:
        return ""
    if ":" in value:
        return clean_text(value.split(":", 1)[1])
    return value


def category_set_name(raw_set_name: str) -> str:
    value = extract_set_tag(raw_set_name)
    aliases = {
        "Scarlet & Violet 151": "S&V 151",
        "Prismatic Evolutions": "SV: Prismatic Evolutions",
        "Crown Zenith": "Crown Zenith",
        "Black Bolt": "Black Bolt",
        "White Flare": "White Flare",
        "Surging Sparks": "Surging Sparks",
        "Perfect Order": "Perfect Order",
        "Chaos Rising": "Chaos Rising",
        "Pitch Black": "Pitch Black",
        "Ascended Heroes": "Ascended Heroes",
    }
    return aliases.get(value, value)


def era_category(raw_set_name: str) -> str:
    value = clean_text(raw_set_name)
    if value.startswith("ME"):
        return "Mega Evolution"
    if value.startswith("SV:") or value.startswith("SVP") or value.startswith("SV "):
        return "Scarlet & Violet"
    if value.startswith("SWSH:") or value.startswith("SWSH"):
        return "Sword & Shield"
    return ""


def product_type_label(product_type: str) -> str:
    return PRODUCT_TYPE_LABELS.get(product_type, clean_text(product_type).replace("_", " ").title())


def recommended_tags(row: dict[str, str]) -> str:
    tags: list[str] = ["English"]
    ptype = clean_text(row.get("product_type"))
    label = product_type_label(ptype)
    if label:
        tags.append(label)

    set_tag = extract_set_tag(row.get("set_name", ""))
    if set_tag:
        tags.append(set_tag)

    title = clean_text(row.get("title") or row.get("product_name"))
    notes = clean_text(row.get("notes"))
    lower_title = title.lower()
    lower_notes = notes.lower()

    if "pokemon center" in lower_title or "pokemon center" in lower_notes or ptype == "pokemon_center_etb":
        tags.append("Pokemon Center")
    if "costco" in lower_title:
        tags.append("Costco")
    if "random design" in lower_notes:
        tags.append("Random Design")
    if "poke ball tin" in lower_title:
        tags.append("Poke Ball Tin")

    unique: list[str] = []
    for tag in tags:
        tag = clean_text(tag)
        if tag and tag not in unique:
            unique.append(tag)
    return ", ".join(unique)


def recommended_categories(row: dict[str, str]) -> str:
    ptype = clean_text(row.get("product_type"))
    title = clean_text(row.get("title") or row.get("product_name"))
    set_name = clean_text(row.get("set_name"))
    era = era_category(set_name)
    set_category = category_set_name(set_name)

    if ptype == "booster_box":
        categories = ["Booster Box", "English Booster Boxes"]
    elif ptype == "booster_bundle":
        categories = ["Booster Bundle"]
    elif ptype in {"etb", "pokemon_center_etb"}:
        categories = ["Elite Trainer Box"]
    elif ptype in {"collection_box", "premium_collection"}:
        categories = ["Other Boxes, Gifts & Bundles", "ex Boxes"]
    elif ptype == "two_pack_blister":
        categories = ["Booster Pack", "English Booster Packs"]
    elif ptype == "bundle_combo":
        categories = ["Other Boxes, Gifts & Bundles", "Other"]
    elif ptype == "tin":
        categories = ["Other Boxes, Gifts & Bundles", "Other"]
    else:
        categories = ["Other Boxes, Gifts & Bundles", "Other"]

    if era and era not in categories:
        categories.append(era)
    if set_category and set_category not in categories:
        categories.append(set_category)

    if "costco" in title.lower() and "Other" not in categories:
        categories.insert(1, "Other")

    unique: list[str] = []
    for category in categories:
        category = clean_text(category)
        if category and category not in unique:
            unique.append(category)
    return ", ".join(unique)


def description_plain(row: dict[str, str]) -> str:
    product_name = clean_text(row.get("product_name") or row.get("title"))
    set_name = display_set_name(row.get("set_name", ""))
    type_label = product_type_label(clean_text(row.get("product_type")))
    notes = clean_text(row.get("notes"))

    lines = [
        f"Factory sealed English Pokemon TCG {type_label.lower()}.",
        f"Product: {product_name}.",
    ]
    if set_name:
        lines.append(f"Set: {set_name}.")
    lines.append("Condition: Sealed.")
    if notes:
        lines.append(f"Notes: {notes}.")
    lines.append(
        "Please review photos for the exact packaging or assortment included. "
        "Contact us before purchase if you would like us to confirm availability or product details."
    )
    return " ".join(lines)


def description_html(row: dict[str, str]) -> str:
    product_name = html.escape(clean_text(row.get("product_name") or row.get("title")))
    set_name = html.escape(display_set_name(row.get("set_name", "")))
    type_label = html.escape(product_type_label(clean_text(row.get("product_type"))))
    notes = html.escape(clean_text(row.get("notes")))

    bullet_lines = [
        f"<li><strong>Product:</strong> {product_name}</li>",
        f"<li><strong>Type:</strong> {type_label}</li>",
        "<li><strong>Language:</strong> English</li>",
        "<li><strong>Condition:</strong> Sealed</li>",
    ]
    if set_name:
        bullet_lines.insert(2, f"<li><strong>Set:</strong> {set_name}</li>")

    paragraphs = [
        '<p style="white-space:pre-wrap;" data-rte-preserve-empty="true"><strong>Factory sealed English Pokemon TCG product.</strong></p>',
        '<ul data-rte-list="true">' + "".join(bullet_lines) + "</ul>",
    ]
    if notes:
        paragraphs.append(
            '<p style="white-space:pre-wrap;" data-rte-preserve-empty="true">'
            f"<strong>Notes:</strong> {notes}"
            "</p>"
        )
    paragraphs.append(
        '<p style="white-space:pre-wrap;" data-rte-preserve-empty="true">'
        "Please review photos for the exact packaging or assortment included. "
        "Contact us before purchase if you would like us to confirm availability or product details."
        "</p>"
    )
    return "".join(paragraphs)


def build_rows(created_rows: list[dict[str, str]], draft_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    draft_by_sku = {clean_text(row.get("sku")): row for row in draft_rows}
    output_rows: list[dict[str, str]] = []
    for created in created_rows:
        sku = clean_text(created.get("sku"))
        draft = draft_by_sku.get(sku, {})
        merged = {**draft, **created}
        merged["sku"] = sku
        output_rows.append(
            {
                "sku": sku,
                "title": clean_text(created.get("title")),
                "squarespace_url": clean_text(created.get("squarespace_url")),
                "product_type": clean_text(created.get("product_type")),
                "set_name": clean_text(draft.get("set_name")),
                "quantity": clean_text(created.get("quantity")),
                "recommended_tags": recommended_tags(merged),
                "recommended_categories": recommended_categories(merged),
                "description_plain": description_plain(merged),
                "description_html": description_html(merged),
                "notes": clean_text(created.get("notes")),
            }
        )
    return output_rows


def main() -> int:
    args = parse_args()
    created_csv = Path(args.created_csv)
    draft_csv = Path(args.draft_csv)
    output_csv = Path(args.output_csv)

    if not created_csv.exists():
        raise SystemExit(f"Created CSV not found: {created_csv}")
    if not draft_csv.exists():
        raise SystemExit(f"Draft CSV not found: {draft_csv}")

    rows = build_rows(
        created_rows=read_csv_rows(created_csv),
        draft_rows=read_csv_rows(draft_csv),
    )
    write_csv(output_csv, OUTPUT_FIELDS, rows)
    print(f"Wrote {len(rows)} rows to {output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
