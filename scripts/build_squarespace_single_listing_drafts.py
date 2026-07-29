#!/usr/bin/env python3
"""
Build reviewable Squarespace singles listing drafts from local Pokemon data.

The workflow is intentionally file-based:
- intake rows live in `data/new_singles_intake.csv`
- this script enriches them from local metadata + market snapshots
- the result is a review/approval CSV that can be checked before any API writes
"""

from __future__ import annotations

import argparse
import csv
import html
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
EXTRACTED_DIR = DATA_DIR / "extracted"
OUTPUT_DIR = REPO_ROOT / "output"
DEFAULT_INTAKE_CSV = DATA_DIR / "new_singles_intake.csv"
DEFAULT_MARKET_CSV = DATA_DIR / "market_prices_latest.csv"
DEFAULT_EXPORT_CSV = REPO_ROOT / "products_Apr-09_04-31-18PM.csv"
DEFAULT_CREATED_CSV = OUTPUT_DIR / "squarespace_created_single_listings.csv"
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "squarespace_single_listing_drafts.csv"

INTAKE_FIELDS = [
    "sku",
    "product_id",
    "subtype",
    "language",
    "condition",
    "quantity",
    "price_override",
    "title_override",
    "notes",
]

DRAFT_FIELDS = [
    "draft_status",
    "errors",
    "warnings",
    "review_status",
    "sku",
    "product_id",
    "subtype",
    "language",
    "condition",
    "quantity",
    "price_override",
    "title_override",
    "notes",
    "final_title",
    "target_price",
    "price_source",
    "market_price",
    "set_name",
    "set_code",
    "card_number",
    "rarity",
    "subtype",
    "card_type",
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


@dataclass(frozen=True)
class DatasetPaths:
    language: str
    signal_csv: Path
    groups_csv: Path


DATASETS = {
    "english": DatasetPaths(
        language="english",
        signal_csv=EXTRACTED_DIR / "pokemon_product_signal_snapshot.csv",
        groups_csv=EXTRACTED_DIR / "pokemon_groups.csv",
    ),
    "japanese": DatasetPaths(
        language="japanese",
        signal_csv=EXTRACTED_DIR / "pokemon_jp_product_signal_snapshot.csv",
        groups_csv=EXTRACTED_DIR / "pokemon_jp_groups.csv",
    ),
}

LANGUAGE_ALIASES = {
    "en": "english",
    "eng": "english",
    "english": "english",
    "jp": "japanese",
    "ja": "japanese",
    "jpn": "japanese",
    "japanese": "japanese",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build reviewable Squarespace singles listing drafts from local metadata."
    )
    parser.add_argument("--intake-csv", default=str(DEFAULT_INTAKE_CSV))
    parser.add_argument("--market-csv", default=str(DEFAULT_MARKET_CSV))
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


def parse_decimal(value: str | None) -> Optional[Decimal]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def money_str(value: Decimal | None) -> str:
    if value is None:
        return ""
    return f"{value.quantize(Decimal('0.01'))}"


def normalize_language(value: str | None) -> str:
    key = str(value or "").strip().lower()
    return LANGUAGE_ALIASES.get(key, "")


def normalize_subtype(value: str | None) -> str:
    return clean_text(value)


def slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    return re.sub(r"-{2,}", "-", text).strip("-")


def clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_condition(value: str | None) -> str:
    text = clean_text(value).lower()
    if not text:
        return "Near Mint"
    aliases = {
        "nm": "Near Mint",
        "near mint": "Near Mint",
        "mint": "Near Mint",
        "lp": "Lightly Played",
        "lightly played": "Lightly Played",
        "mp": "Moderately Played",
        "moderately played": "Moderately Played",
        "hp": "Heavily Played",
        "heavily played": "Heavily Played",
        "damaged": "Damaged",
    }
    return aliases.get(text, text.title())


def condition_slug(condition: str) -> str:
    mapping = {
        "Near Mint": "near-mint",
        "Lightly Played": "lightly-played",
        "Moderately Played": "moderately-played",
        "Heavily Played": "heavily-played",
        "Damaged": "damaged",
    }
    return mapping.get(condition, slugify(condition))


def parse_quantity(value: str | None) -> Optional[int]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        quantity = int(raw)
    except ValueError:
        return None
    return quantity if quantity >= 0 else None


def split_csv_list(value: str | None) -> List[str]:
    if not value:
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def subtype_key(value: str | None) -> str:
    return normalize_subtype(value).casefold()


def build_variant_sku(product_id: str, subtype: str | None) -> str:
    normalized_subtype = normalize_subtype(subtype)
    if not normalized_subtype or normalized_subtype.casefold() == "normal":
        return product_id
    return f"{product_id}-{slugify(normalized_subtype)}"


def load_market_by_sku(path: Path) -> Dict[str, dict[str, str]]:
    rows = {}
    if not path.exists():
        return rows
    for row in read_csv_rows(path):
        sku = clean_text(row.get("sku"))
        if sku:
            rows[sku] = row
    return rows


def load_signal_by_product_id(path: Path) -> Dict[str, List[dict[str, str]]]:
    rows: Dict[str, List[dict[str, str]]] = {}
    for row in read_csv_rows(path):
        product_id = clean_text(row.get("productId"))
        if not product_id:
            continue
        rows.setdefault(product_id, []).append(row)
    return rows


def load_groups_by_id(path: Path) -> Dict[str, dict[str, str]]:
    rows = {}
    for row in read_csv_rows(path):
        group_id = clean_text(row.get("groupId"))
        if group_id:
            rows[group_id] = row
    return rows


def load_existing_store_mapping(path: Path) -> Dict[str, dict[str, str]]:
    mapping: Dict[str, dict[str, str]] = {}
    if not path.exists():
        return mapping
    for row in read_csv_rows(path):
        sku = clean_text(row.get("SKU"))
        if not sku:
            sku = clean_text(row.get("sku"))
        if not sku:
            continue
        mapping[sku] = {
            "product_id": clean_text(
                row.get("Product ID [Non Editable]") or row.get("product_id")
            ),
            "variant_id": clean_text(
                row.get("Variant ID [Non Editable]") or row.get("variant_id")
            ),
        }
    return mapping


def canonical_product_id(intake_row: dict[str, str]) -> str:
    product_id = clean_text(intake_row.get("product_id"))
    if product_id:
        return product_id
    sku = clean_text(intake_row.get("sku"))
    return sku if sku.isdigit() else ""


def resolve_signal_row(
    *,
    product_id: str,
    requested_subtype: str,
    requested_sku: str,
    signal_rows: List[dict[str, str]],
) -> tuple[dict[str, str], str, List[str]]:
    warnings: List[str] = []
    if not signal_rows:
        return {}, requested_subtype, warnings

    subtype_matches = {
        subtype_key(row.get("subTypeName")): row
        for row in signal_rows
    }
    if requested_subtype:
        match = subtype_matches.get(subtype_key(requested_subtype))
        if match is not None:
            return match, normalize_subtype(match.get("subTypeName")), warnings
        return {}, requested_subtype, warnings

    if len(signal_rows) == 1:
        only_row = signal_rows[0]
        return only_row, normalize_subtype(only_row.get("subTypeName")), warnings

    if requested_sku:
        sku_matches = {
            build_variant_sku(product_id, row.get("subTypeName")): row
            for row in signal_rows
        }
        match = sku_matches.get(requested_sku)
        if match is not None:
            inferred_subtype = normalize_subtype(match.get("subTypeName"))
            warnings.append(f"inferred subtype from sku: {inferred_subtype}")
            return match, inferred_subtype, warnings

    preferred_defaults = [
        "normal",
        "unlimited holofoil",
        "unlimited",
        "holofoil",
    ]
    for preferred in preferred_defaults:
        match = subtype_matches.get(preferred)
        if match is not None:
            inferred_subtype = normalize_subtype(match.get("subTypeName"))
            warnings.append(f"multiple variants found; defaulted subtype to {inferred_subtype}")
            return match, inferred_subtype, warnings

    return {}, requested_subtype, warnings


def build_title(
    product_name: str,
    number: str,
    group_name: str,
    set_code: str,
    language: str,
    subtype: str,
) -> str:
    title = clean_text(product_name)
    normalized_subtype = normalize_subtype(subtype)
    if normalized_subtype and normalized_subtype.casefold() != "normal" and normalized_subtype.casefold() not in title.casefold():
        title = f"{title} ({normalized_subtype})"
    if number and number.lower() not in title.lower():
        title = f"{title} - {number}"
    set_label = clean_text(group_name)
    if set_code and set_code.lower() not in set_label.lower():
        set_label = f"{set_label} ({set_code})"
    if set_label and set_label.lower() not in title.lower():
        title = f"{title} - {set_label}"
    if language == "japanese" and "japanese" not in title.lower():
        title = f"{title} Japanese"
    return title


def build_short_description(
    product_name: str,
    group_name: str,
    number: str,
    rarity: str,
    language: str,
    condition: str,
    subtype: str,
) -> str:
    parts = [
        f"{product_name} from {group_name}.",
        f"Language: {language.title()}.",
        f"Condition: {condition}.",
    ]
    normalized_subtype = normalize_subtype(subtype)
    if normalized_subtype:
        parts.append(f"Variant: {normalized_subtype}.")
    if number:
        parts.append(f"Card number: {number}.")
    if rarity:
        parts.append(f"Rarity: {rarity}.")
    parts.append("Images represent the card version and may not be the exact copy shipped.")
    return " ".join(parts)


def build_description_html(short_description: str, language: str, condition: str) -> str:
    escaped_description = html.escape(short_description)
    escaped_language = html.escape(language.title())
    escaped_condition = html.escape(condition)
    return (
        f"<p style=\"white-space:pre-wrap;\" data-rte-preserve-empty=\"true\">"
        f"<em><span>Language: {escaped_language}</span><br /></em>"
        f"<span>Condition: {escaped_condition}</span><br />"
        f"<span>{escaped_description}</span>"
        f"</p>"
    )


def build_categories(
    language: str,
    group_name: str,
    rarity: str,
    subtype: str,
    card_type: str,
    condition: str,
) -> List[str]:
    categories = [f"/singles/{language}"]

    if language == "japanese":
        set_name = group_name.split(":", 1)[-1].strip() if ":" in group_name else group_name
        set_slug = slugify(set_name)
        if set_slug:
            categories.append(f"/singles/japanese/{set_slug}")
    else:
        if ":" in group_name:
            prefix, name = [part.strip() for part in group_name.split(":", 1)]
            prefix_slug = slugify(prefix)
            name_slug = slugify(name)
            if prefix_slug and name_slug:
                categories.append(f"/singles/{prefix_slug}/{name_slug}")
        else:
            name_slug = slugify(group_name)
            if name_slug:
                categories.append(f"/singles/english/{name_slug}")

    if card_type:
        categories.append(f"/singles/card-type/{slugify(card_type)}")
    if rarity:
        categories.append(f"/singles/rarity/{slugify(rarity)}")
    if subtype:
        categories.append(f"/singles/printing/{slugify(subtype)}")
    if condition:
        categories.append(f"/singles/condition/{condition_slug(condition)}")

    unique_categories: List[str] = []
    for category in categories:
        if category and category not in unique_categories:
            unique_categories.append(category)
    return unique_categories


def build_tags(notes: str | None = None) -> List[str]:
    tags = ["Singles"]
    if clean_text(notes):
        tags.append("Singles Intake")
    return tags


def resolve_price(
    intake_row: dict[str, str],
    market_row: Optional[dict[str, str]],
    signal_row: dict[str, str],
) -> tuple[str, str, str]:
    price_override = parse_decimal(intake_row.get("price_override"))
    if price_override is not None:
        market_price = parse_decimal((market_row or {}).get("market_price"))
        return money_str(price_override), "price_override", money_str(market_price)

    target_price = parse_decimal((market_row or {}).get("target_price"))
    if target_price is not None:
        market_price = parse_decimal((market_row or {}).get("market_price"))
        return money_str(target_price), "market_prices_latest.target_price", money_str(market_price)

    market_price = parse_decimal((market_row or {}).get("market_price"))
    if market_price is not None:
        return money_str(market_price), "market_prices_latest.market_price", money_str(market_price)

    signal_price = parse_decimal(signal_row.get("latest_price"))
    if signal_price is not None:
        return money_str(signal_price), "product_signal_snapshot.latest_price", money_str(signal_price)

    return "", "", ""


def build_url_slug(title: str, sku: str) -> str:
    title_slug = slugify(title)
    sku_slug = slugify(sku)
    if sku_slug and sku_slug not in title_slug:
        return slugify(f"{title_slug}-{sku_slug}")
    return title_slug


def draft_row(
    intake_row: dict[str, str],
    dataset: DatasetPaths,
    signal_by_product_id: Dict[str, List[dict[str, str]]],
    groups_by_id: Dict[str, dict[str, str]],
    market_by_sku: Dict[str, dict[str, str]],
    existing_by_sku: Dict[str, dict[str, str]],
    seen_skus: set[str],
) -> dict[str, str]:
    row = {field: clean_text(intake_row.get(field)) for field in INTAKE_FIELDS}
    errors: List[str] = []
    warnings: List[str] = []

    requested_sku = row["sku"]

    product_id = canonical_product_id(row)
    if not product_id:
        errors.append("missing product_id and sku is not numeric")
    row["product_id"] = product_id
    requested_subtype = normalize_subtype(row.get("subtype"))
    row["subtype"] = requested_subtype

    quantity = parse_quantity(row.get("quantity"))
    if quantity is None:
        errors.append("quantity must be a non-negative integer")

    signal_candidates = signal_by_product_id.get(product_id, []) if product_id else []
    signal_row, resolved_subtype, subtype_warnings = resolve_signal_row(
        product_id=product_id,
        requested_subtype=requested_subtype,
        requested_sku=requested_sku,
        signal_rows=signal_candidates,
    )
    warnings.extend(subtype_warnings)
    row["subtype"] = resolved_subtype or requested_subtype

    if not signal_row:
        if signal_candidates:
            available_subtypes = ", ".join(
                sorted(normalize_subtype(candidate.get("subTypeName")) for candidate in signal_candidates)
            )
            errors.append(f"multiple market variants found; set subtype explicitly ({available_subtypes})")
        else:
            errors.append(f"product metadata not found for product_id {product_id or '<blank>'}")
        signal_row = {}

    sku = build_variant_sku(product_id, row["subtype"]) if product_id else requested_sku
    if requested_sku and sku and requested_sku != sku:
        warnings.append(f"canonical sku normalized to {sku}")
    row["sku"] = sku
    if not sku:
        errors.append("missing sku")
    elif sku in seen_skus:
        errors.append("duplicate sku in intake")
    else:
        seen_skus.add(sku)

    existing = existing_by_sku.get(sku)
    if existing:
        errors.append("sku already exists in Squarespace/export records")

    group_row = groups_by_id.get(clean_text(signal_row.get("groupId")))
    group_name = clean_text(signal_row.get("groupName") or (group_row or {}).get("name"))
    set_code = clean_text((group_row or {}).get("abbreviation"))
    product_name = clean_text(signal_row.get("productName"))
    number = clean_text(signal_row.get("number"))
    rarity = clean_text(signal_row.get("rarity"))
    subtype = normalize_subtype(signal_row.get("subTypeName") or row["subtype"])
    card_type = clean_text(signal_row.get("productClass"))
    image_url = clean_text(signal_row.get("imageUrl"))
    product_kind = clean_text(signal_row.get("productKind"))
    if product_kind and product_kind != "card":
        warnings.append(f"product_kind={product_kind}")

    market_row = market_by_sku.get(sku) or market_by_sku.get(product_id)
    target_price, price_source, market_price = resolve_price(row, market_row, signal_row)
    if not target_price:
        errors.append("unable to derive target price from intake, market csv, or signal snapshot")

    condition = normalize_condition(row.get("condition"))
    row["condition"] = condition
    final_title = clean_text(row.get("title_override")) or build_title(
        product_name=product_name,
        number=number,
        group_name=group_name,
        set_code=set_code,
        language=dataset.language,
        subtype=subtype,
    )
    if not final_title:
        errors.append("unable to derive final title")

    short_description = build_short_description(
        product_name=product_name or final_title,
        group_name=group_name or "Unknown Set",
        number=number,
        rarity=rarity,
        language=dataset.language,
        condition=condition,
        subtype=subtype,
    )
    description_html = build_description_html(short_description, dataset.language, condition)
    tags = build_tags(row.get("notes"))
    categories = build_categories(
        language=dataset.language,
        group_name=group_name,
        rarity=rarity,
        subtype=subtype,
        card_type=card_type,
        condition=condition,
    )

    output = {
        "draft_status": "ready" if not errors else "error",
        "errors": " | ".join(errors),
        "warnings": " | ".join(warnings),
        "review_status": "",
        "sku": sku,
        "product_id": product_id,
        "subtype": subtype,
        "language": dataset.language,
        "condition": condition,
        "quantity": row.get("quantity", ""),
        "price_override": row.get("price_override", ""),
        "title_override": row.get("title_override", ""),
        "notes": row.get("notes", ""),
        "final_title": final_title,
        "target_price": target_price,
        "price_source": price_source,
        "market_price": market_price,
        "set_name": group_name,
        "set_code": set_code,
        "card_number": number,
        "rarity": rarity,
        "subtype": subtype,
        "card_type": card_type,
        "image_url": image_url,
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
    return output


def build_drafts(
    intake_csv: Path,
    market_csv: Path,
    squarespace_export: Path,
    created_csv: Path,
) -> List[dict[str, str]]:
    market_by_sku = load_market_by_sku(market_csv)
    existing_by_sku = load_existing_store_mapping(squarespace_export)
    for sku, created in load_existing_store_mapping(created_csv).items():
        existing_by_sku.setdefault(sku, created)

    datasets_cache = {
        language: (
            load_signal_by_product_id(paths.signal_csv),
            load_groups_by_id(paths.groups_csv),
        )
        for language, paths in DATASETS.items()
    }

    drafts: List[dict[str, str]] = []
    seen_skus: set[str] = set()
    for intake_row in read_csv_rows(intake_csv):
        normalized_language = normalize_language(intake_row.get("language"))
        if not normalized_language:
            row = {field: clean_text(intake_row.get(field)) for field in DRAFT_FIELDS}
            row.update(
                {
                    "draft_status": "error",
                    "errors": "unsupported language; use english or japanese",
                    "warnings": "",
                    "review_status": "",
                    "product_id": canonical_product_id(intake_row),
                }
            )
            drafts.append(row)
            continue

        dataset = DATASETS[normalized_language]
        signal_by_product_id, groups_by_id = datasets_cache[normalized_language]
        drafts.append(
            draft_row(
                intake_row=intake_row,
                dataset=dataset,
                signal_by_product_id=signal_by_product_id,
                groups_by_id=groups_by_id,
                market_by_sku=market_by_sku,
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
        market_csv=Path(args.market_csv),
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
