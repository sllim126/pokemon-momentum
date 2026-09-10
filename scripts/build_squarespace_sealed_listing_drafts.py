#!/usr/bin/env python3
"""
Build reviewable Squarespace sealed listing drafts from local Pokemon data.

The workflow is intentionally file-based:
- intake rows live in `data/new_sealed_intake.csv`
- this script enriches them from local market snapshots
- the result is a review/approval CSV that can be checked before any API writes
"""

from __future__ import annotations

import argparse
import csv
import html
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
EXTRACTED_DIR = DATA_DIR / "extracted"
OUTPUT_DIR = REPO_ROOT / "output"
DEFAULT_INTAKE_CSV = DATA_DIR / "new_sealed_intake.csv"
DEFAULT_SIGNAL_CSV = EXTRACTED_DIR / "pokemon_product_signal_snapshot.csv"
DEFAULT_EXPORT_CSV = REPO_ROOT / "products_Apr-09_04-31-18PM.csv"
DEFAULT_CREATED_CSV = OUTPUT_DIR / "squarespace_created_sealed_listings.csv"
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "squarespace_sealed_listing_drafts.csv"

INTAKE_FIELDS = [
    "sku",
    "product_name",
    "language",
    "product_type",
    "quantity",
    "price_override",
    "title_override",
    "tcgplayer_product_id",
    "notes",
    "lookup_status",
]

DRAFT_FIELDS = [
    "draft_status",
    "errors",
    "warnings",
    "review_status",
    "sku",
    "product_name",
    "language",
    "product_type",
    "quantity",
    "price_override",
    "title_override",
    "tcgplayer_product_id",
    "notes",
    "lookup_status",
    "final_title",
    "target_price",
    "price_source",
    "market_price",
    "set_name",
    "market_title",
    "image_url",
    "short_description",
    "description_html",
    "tags",
    "categories",
    "url_slug",
    "visibility",
    "stock_quantity",
    "existing_store_product_id",
    "existing_store_variant_id",
]

LANGUAGE_ALIASES = {
    "en": "english",
    "eng": "english",
    "english": "english",
}

PRODUCT_TYPE_LABELS = {
    "tin": "Tin",
    "premium_collection": "Premium Collection",
    "pokemon_center_etb": "Pokemon Center Elite Trainer Box",
    "booster_bundle": "Booster Bundle",
    "two_pack_blister": "2-Pack Blister",
    "collection_box": "Collection Box",
    "etb": "Elite Trainer Box",
    "booster_box": "Booster Box",
    "bundle_combo": "Bundle Combo",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build reviewable Squarespace sealed listing drafts from local metadata."
    )
    parser.add_argument("--intake-csv", default=str(DEFAULT_INTAKE_CSV))
    parser.add_argument("--signal-csv", default=str(DEFAULT_SIGNAL_CSV))
    parser.add_argument("--squarespace-export", default=str(DEFAULT_EXPORT_CSV))
    parser.add_argument("--created-csv", default=str(DEFAULT_CREATED_CSV))
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV))
    return parser.parse_args()


def read_csv_rows(path: Path) -> List[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: Iterable[str], rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def slugify(value: str | None) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", clean_text(value).lower())
    return re.sub(r"-{2,}", "-", text).strip("-")


def normalize_language(value: str | None) -> str:
    return LANGUAGE_ALIASES.get(clean_text(value).lower(), "")


def parse_decimal(value: str | None) -> Optional[Decimal]:
    raw = clean_text(value)
    if not raw:
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def money_str(value: Decimal | None) -> str:
    if value is None:
        return ""
    return f"{value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}"


def parse_quantity(value: str | None) -> int | None:
    raw = clean_text(value)
    if not raw:
        return None
    try:
        quantity = int(raw)
    except ValueError:
        return None
    return quantity if quantity >= 0 else None


def split_product_ids(value: str | None) -> List[str]:
    return [token.strip() for token in re.split(r"[|,]", str(value or "")) if token.strip()]


def load_signal_by_product_id(path: Path) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for row in read_csv_rows(path):
        product_id = clean_text(row.get("productId"))
        if product_id:
            rows[product_id] = row
    return rows


def load_existing_store_mapping(path: Path) -> dict[str, dict[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    if not path.exists():
        return mapping
    for row in read_csv_rows(path):
        sku = clean_text(row.get("SKU") or row.get("sku"))
        if not sku:
            continue
        mapping[sku] = {
            "product_id": clean_text(row.get("Product ID [Non Editable]") or row.get("product_id")),
            "variant_id": clean_text(row.get("Variant ID [Non Editable]") or row.get("variant_id")),
        }
    return mapping


def average_price(rows: List[dict[str, str]]) -> Decimal | None:
    prices = [parse_decimal(row.get("latest_price")) for row in rows]
    valid_prices = [price for price in prices if price is not None]
    if not valid_prices:
        return None
    return sum(valid_prices, Decimal("0")) / Decimal(len(valid_prices))


def build_title(product_name: str, language: str) -> str:
    title = clean_text(product_name)
    language_label = language.title()
    if language_label and language_label.lower() not in title.lower():
        return f"{title} - {language_label}"
    return title


def build_short_description(
    *,
    product_name: str,
    language: str,
    set_name: str,
    product_type: str,
    notes: str,
) -> str:
    parts = [
        f"{product_name}.",
        f"Language: {language.title()}.",
    ]
    product_type_label = PRODUCT_TYPE_LABELS.get(product_type, clean_text(product_type).replace("_", " ").title())
    if product_type_label:
        parts.append(f"Product type: {product_type_label}.")
    if set_name and set_name.lower() != "miscellaneous cards & products":
        parts.append(f"Set or source line: {set_name}.")
    notes_text = clean_text(notes)
    if notes_text:
        parts.append(f"Notes: {notes_text}.")
    parts.append("Item ships sealed unless otherwise noted.")
    return " ".join(parts)


def build_description_html(short_description: str) -> str:
    escaped = html.escape(short_description)
    return (
        "<p style=\"white-space:pre-wrap;\" data-rte-preserve-empty=\"true\">"
        f"{escaped}"
        "</p>"
    )


def build_tags(language: str, product_type: str, notes: str) -> List[str]:
    tags = ["Sealed", language.title()]
    label = PRODUCT_TYPE_LABELS.get(product_type)
    if label:
        tags.append(label)
    if clean_text(notes):
        tags.append("Sealed Intake")
    unique: List[str] = []
    for tag in tags:
        if tag and tag not in unique:
            unique.append(tag)
    return unique


def build_categories(language: str, product_type: str, set_name: str) -> List[str]:
    categories = [f"/sealed/{language}"]
    product_slug = slugify(product_type.replace("_", "-"))
    if product_slug:
        categories.append(f"/sealed/type/{product_slug}")
    set_slug = slugify(set_name)
    if set_slug and set_slug != "miscellaneous-cards-products":
        categories.append(f"/sealed/{language}/{set_slug}")
    unique: List[str] = []
    for category in categories:
        if category and category not in unique:
            unique.append(category)
    return unique


def build_url_slug(title: str, sku: str) -> str:
    title_slug = slugify(title)
    sku_slug = slugify(sku)
    if sku_slug and sku_slug not in title_slug:
        return slugify(f"{title_slug}-{sku_slug}")
    return title_slug


def resolve_price(
    *,
    intake_row: dict[str, str],
    signal_rows: List[dict[str, str]],
) -> tuple[str, str, str]:
    price_override = parse_decimal(intake_row.get("price_override"))
    if price_override is not None:
        market_price = average_price(signal_rows)
        return money_str(price_override), "price_override", money_str(market_price)

    market_price = average_price(signal_rows)
    if market_price is None:
        return "", "", ""
    if len(signal_rows) > 1:
        return money_str(market_price), "product_signal_snapshot.average_latest_price", money_str(market_price)
    return money_str(market_price), "product_signal_snapshot.latest_price", money_str(market_price)


def draft_row(
    intake_row: dict[str, str],
    signal_by_product_id: dict[str, dict[str, str]],
    existing_by_sku: dict[str, dict[str, str]],
    seen_skus: set[str],
) -> dict[str, str]:
    row = {field: clean_text(intake_row.get(field)) for field in INTAKE_FIELDS}
    errors: List[str] = []
    warnings: List[str] = []

    sku = row["sku"]
    if not sku:
        errors.append("missing sku")
    elif sku in seen_skus:
        errors.append("duplicate sku in intake")
    else:
        seen_skus.add(sku)

    language = normalize_language(row.get("language"))
    if not language:
        errors.append("unsupported language; sealed workflow currently supports english")
    row["language"] = language or row.get("language", "")

    quantity = parse_quantity(row.get("quantity"))
    if quantity is None:
        errors.append("quantity must be a non-negative integer")

    product_type = clean_text(row.get("product_type"))
    if not product_type:
        errors.append("missing product_type")

    tcgplayer_product_ids = split_product_ids(row.get("tcgplayer_product_id"))
    if not tcgplayer_product_ids:
        errors.append("missing tcgplayer_product_id")

    signal_rows: List[dict[str, str]] = []
    missing_product_ids: List[str] = []
    for product_id in tcgplayer_product_ids:
        signal_row = signal_by_product_id.get(product_id)
        if signal_row is None:
            missing_product_ids.append(product_id)
            continue
        signal_rows.append(signal_row)
    if missing_product_ids:
        errors.append(f"product metadata not found for tcgplayer_product_id: {', '.join(missing_product_ids)}")

    existing = existing_by_sku.get(sku)
    if existing:
        errors.append("sku already exists in Squarespace/export records")

    lookup_status = clean_text(row.get("lookup_status"))
    if lookup_status == "verify_existing_listing":
        warnings.append("verify listing already exists before creating a duplicate")
    elif lookup_status and lookup_status != "ready_for_pricing_mapping":
        warnings.append(f"lookup_status={lookup_status}")

    target_price, price_source, market_price = resolve_price(intake_row=row, signal_rows=signal_rows)
    if not target_price:
        errors.append("unable to derive target price from intake or signal snapshot")

    input_product_name = clean_text(row.get("product_name"))
    first_signal_row = signal_rows[0] if signal_rows else {}
    signal_title = clean_text(first_signal_row.get("productName"))
    set_name = clean_text(first_signal_row.get("groupName"))
    if signal_title and input_product_name and signal_title.lower() != input_product_name.lower():
        warnings.append(f"market title differs from intake title: {signal_title}")

    final_title = clean_text(row.get("title_override")) or build_title(
        input_product_name or signal_title or sku,
        language or "english",
    )
    short_description = build_short_description(
        product_name=input_product_name or signal_title or final_title,
        language=language or "english",
        set_name=set_name,
        product_type=product_type,
        notes=row.get("notes", ""),
    )
    description_html = build_description_html(short_description)
    tags = build_tags(language or "english", product_type, row.get("notes", ""))
    categories = build_categories(language or "english", product_type, set_name)

    return {
        "draft_status": "ready" if not errors else "error",
        "errors": " | ".join(errors),
        "warnings": " | ".join(warnings),
        "review_status": "",
        "sku": sku,
        "product_name": input_product_name,
        "language": language,
        "product_type": product_type,
        "quantity": row.get("quantity", ""),
        "price_override": row.get("price_override", ""),
        "title_override": row.get("title_override", ""),
        "tcgplayer_product_id": row.get("tcgplayer_product_id", ""),
        "notes": row.get("notes", ""),
        "lookup_status": lookup_status,
        "final_title": final_title,
        "target_price": target_price,
        "price_source": price_source,
        "market_price": market_price,
        "set_name": set_name,
        "market_title": signal_title,
        "image_url": clean_text(first_signal_row.get("imageUrl")),
        "short_description": short_description,
        "description_html": description_html,
        "tags": ", ".join(tags),
        "categories": ", ".join(categories),
        "url_slug": build_url_slug(final_title, sku),
        "visibility": "hidden",
        "stock_quantity": str(quantity) if quantity is not None else "",
        "existing_store_product_id": existing["product_id"] if existing else "",
        "existing_store_variant_id": existing["variant_id"] if existing else "",
    }


def build_drafts(
    intake_csv: Path,
    signal_csv: Path,
    squarespace_export: Path,
    created_csv: Path,
) -> List[dict[str, str]]:
    signal_by_product_id = load_signal_by_product_id(signal_csv)
    existing_by_sku = load_existing_store_mapping(squarespace_export)
    for sku, created in load_existing_store_mapping(created_csv).items():
        existing_by_sku.setdefault(sku, created)

    drafts: List[dict[str, str]] = []
    seen_skus: set[str] = set()
    for intake_row in read_csv_rows(intake_csv):
        drafts.append(
            draft_row(
                intake_row=intake_row,
                signal_by_product_id=signal_by_product_id,
                existing_by_sku=existing_by_sku,
                seen_skus=seen_skus,
            )
        )
    return drafts


def main() -> int:
    args = parse_args()
    intake_csv = Path(args.intake_csv)
    output_csv = Path(args.output_csv)
    if not intake_csv.exists():
        raise SystemExit(f"Intake CSV not found: {intake_csv}")

    drafts = build_drafts(
        intake_csv=intake_csv,
        signal_csv=Path(args.signal_csv),
        squarespace_export=Path(args.squarespace_export),
        created_csv=Path(args.created_csv),
    )
    write_csv(output_csv, DRAFT_FIELDS, drafts)
    ready = sum(1 for row in drafts if row["draft_status"] == "ready")
    errors = len(drafts) - ready
    print(f"Wrote {len(drafts)} draft rows to {output_csv}")
    print(f"Ready: {ready}")
    print(f"Errors: {errors}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
