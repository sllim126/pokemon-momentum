#!/usr/bin/env python3
"""
Convert a collection-builder CSV export into the singles intake CSV format.

The source CSV is expected to include card-level metadata such as set name,
card number, variant, and language. This script resolves those rows against
the local Pokemon signal snapshots so Squarespace listing drafts can still be
built from the repo's canonical data rather than manual TCGplayer lookups.
"""

from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from scripts.build_squarespace_single_listing_drafts import (
    DATASETS,
    DEFAULT_INTAKE_CSV,
    INTAKE_FIELDS,
    DatasetPaths,
    build_variant_sku,
    clean_text,
    normalize_condition,
    normalize_language,
    normalize_subtype,
    read_csv_rows,
    write_csv,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_CSV = REPO_ROOT / "data" / "collection_builder_export.csv"
SOURCE_REQUIRED_FIELDS = [
    "Name",
    "Set Name",
    "Card Number",
    "Variant",
    "Language",
]

VARIANT_ALIASES = {
    "normal": "Normal",
    "holo": "Holofoil",
    "holofoil": "Holofoil",
    "reverse holo": "Reverse Holofoil",
    "reverse holofoil": "Reverse Holofoil",
    "unlimited": "Unlimited",
    "unlimited holo": "Unlimited Holofoil",
    "unlimited holofoil": "Unlimited Holofoil",
    "cosmos holo": "Cosmos Holofoil",
    "cosmos holofoil": "Cosmos Holofoil",
    "detective pikachu stamp": "Detective Pikachu Stamp",
    "expansion stamp": "Expansion Stamp",
    "master ball reverse holo": "Master Ball Reverse Holofoil",
    "master ball reverse holofoil": "Master Ball Reverse Holofoil",
    "poke ball reverse holo": "Poke Ball Reverse Holofoil",
    "poke ball reverse holofoil": "Poke Ball Reverse Holofoil",
}

SET_ALIAS_MAP = {
    "sm black star promos": [
        "sm promos",
    ],
    "swsh black star promos": [
        "swsh sword shield promo cards",
        "swsh sword and shield promo cards",
        "sword shield promo cards",
        "sword and shield promo cards",
    ],
    "wizards black star promos": [
        "wotc promo",
        "wizards black star promos",
    ],
    "scarlet and violet black star promos": [
        "scarlet and violet promo cards",
        "sv scarlet and violet promo cards",
        "svp scarlet and violet promo cards",
    ],
    "scarlet violet black star promos": [
        "scarlet violet promo cards",
        "sv scarlet violet promo cards",
        "svp scarlet violet promo cards",
    ],
    "mega evolution": [
        "mega evolution",
        "me01 mega evolution",
    ],
    "mega evolution black star promos": [
        "me mega evolution promo",
        "mega evolution promo",
    ],
    "pokemon tcg classic blastoise": [
        "trading card game classic",
        "pokemon tcg classic",
    ],
    "celebations classic collection": [
        "celebrations classic collection",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a collection-builder CSV into new_singles_intake.csv."
    )
    parser.add_argument("--source-csv", default=str(DEFAULT_SOURCE_CSV))
    parser.add_argument("--output-csv", default=str(DEFAULT_INTAKE_CSV))
    return parser.parse_args()


def normalize_lookup_text(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[\'`]+", "", text)
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text).strip().lower()
    return re.sub(r"\s+", " ", text)


def normalize_card_number(value: str | None) -> str:
    text = clean_text(value).upper().replace(" ", "").replace("_", "/")
    if not text:
        return ""

    def strip_leading_zeroes(match: re.Match[str]) -> str:
        digits = match.group(0)
        return str(int(digits)) if digits.isdigit() else digits

    return re.sub(r"\d+", strip_leading_zeroes, text)


def numbers_match(source_number: str, row_number: str) -> bool:
    left = normalize_card_number(source_number)
    right = normalize_card_number(row_number)
    if not left or not right:
        return False
    if left == right:
        return True
    left_prefix = left.split("/", 1)[0]
    right_prefix = right.split("/", 1)[0]
    return bool(left_prefix and right_prefix and left_prefix == right_prefix)


def normalize_variant_label(value: str | None) -> str:
    key = normalize_lookup_text(value)
    if key == "unlimited shadowless":
        return "Unlimited"
    if key in VARIANT_ALIASES:
        return VARIANT_ALIASES[key]
    return normalize_subtype(value)


def set_aliases(value: str | None) -> set[str]:
    text = clean_text(value)
    aliases: set[str] = set()
    normalized = normalize_lookup_text(text)
    if normalized:
        aliases.add(normalized)
        for alias in SET_ALIAS_MAP.get(normalized, []):
            expanded = normalize_lookup_text(alias)
            if expanded:
                aliases.add(expanded)

    if ":" in text:
        parts = [part.strip() for part in text.split(":") if part.strip()]
        for start in range(1, len(parts)):
            alias = normalize_lookup_text(" ".join(parts[start:]))
            if alias:
                aliases.add(alias)

    if " - " in text:
        parts = [part.strip() for part in text.split(" - ") if part.strip()]
        if len(parts) > 1:
            alias = normalize_lookup_text(" ".join(parts[1:]))
            if alias:
                aliases.add(alias)

    tokenized = normalized.split()
    if tokenized and re.fullmatch(r"(?:sv|swsh|sm|xy|bw|dp|hgss|me|svp)\d*[a-z]*", tokenized[0]):
        alias = " ".join(tokenized[1:])
        if alias:
            aliases.add(alias)
    if tokenized and tokenized[0] == "ex":
        alias = " ".join(tokenized[1:])
        if alias:
            aliases.add(alias)

    return aliases


def product_aliases(value: str | None) -> set[str]:
    text = clean_text(value)
    aliases = set()
    normalized = normalize_lookup_text(text)
    if normalized:
        aliases.add(normalized)

    if " - " in text:
        prefix = clean_text(text.split(" - ", 1)[0])
        alias = normalize_lookup_text(prefix)
        if alias:
            aliases.add(alias)

    if "(" in text:
        prefix = clean_text(text.split("(", 1)[0].rstrip("- "))
        alias = normalize_lookup_text(prefix)
        if alias:
            aliases.add(alias)

    return aliases


def row_matches_variant(source_variant: str, row: dict[str, str]) -> bool:
    target_variant = normalize_variant_label(source_variant).casefold()
    row_variant = normalize_variant_label(row.get("subTypeName")).casefold()
    product_name = normalize_lookup_text(row.get("productName"))

    if row_variant == target_variant:
        return True

    special_product_patterns = {
        "master ball reverse holofoil": ["master ball pattern"],
        "poke ball reverse holofoil": ["poke ball pattern"],
        "expansion stamp": ["stamped"],
        "detective pikachu stamp": ["detective pikachu stamped"],
        "iono stamp": ["regional championships", "stamp"],
    }
    for pattern in special_product_patterns.get(target_variant, []):
        if pattern in product_name:
            return True

    if target_variant == "cosmos holofoil" and row_variant == "holofoil":
        return True

    return False


def row_matches_set(target_set_aliases: set[str], row: dict[str, str], source_variant: str) -> bool:
    row_set_aliases = set_aliases(row.get("groupName"))
    if target_set_aliases & row_set_aliases:
        return True

    product_name = normalize_lookup_text(row.get("productName"))
    if "mega evolution" in target_set_aliases and "mega evolution" in product_name:
        return True
    if normalize_lookup_text(source_variant) == "iono stamp" and "regional championships" in product_name:
        return True

    for alias in target_set_aliases:
        if any(alias in row_alias or row_alias in alias for row_alias in row_set_aliases):
            return True
    return False


def prefer_candidate_rows(source_variant: str, matches: Sequence[dict[str, str]]) -> List[dict[str, str]]:
    target_variant = normalize_lookup_text(source_variant)
    if target_variant == "cosmos holofoil":
        non_stamped = [
            row for row in matches if "stamped" not in normalize_lookup_text(row.get("productName"))
        ]
        if non_stamped:
            return non_stamped
    if target_variant == "iono stamp":
        regional = [
            row for row in matches if "regional championships" in normalize_lookup_text(row.get("productName"))
        ]
        if regional:
            return regional
    return list(matches)


def load_signal_rows(dataset: DatasetPaths) -> List[dict[str, str]]:
    return read_csv_rows(dataset.signal_csv)


def find_candidate_rows(
    *,
    source_name: str,
    source_set_name: str,
    source_card_number: str,
    source_variant: str,
    signal_rows: Sequence[dict[str, str]],
) -> List[dict[str, str]]:
    target_name = normalize_lookup_text(source_name)
    target_set_aliases = set_aliases(source_set_name)

    exact_matches: List[dict[str, str]] = []
    loose_matches: List[dict[str, str]] = []
    for row in signal_rows:
        if not row_matches_variant(source_variant, row):
            continue
        if not numbers_match(source_card_number, row.get("number") or ""):
            continue
        if target_name not in product_aliases(row.get("productName")):
            continue

        if row_matches_set(target_set_aliases, row, source_variant):
            exact_matches.append(row)
            continue

    return prefer_candidate_rows(source_variant, exact_matches or loose_matches)


def convert_collection_rows(
    source_rows: Sequence[dict[str, str]],
    *,
    datasets: Dict[str, DatasetPaths] | None = None,
    default_condition: str = "Near Mint",
) -> tuple[List[dict[str, str]], List[str]]:
    datasets = datasets or DATASETS
    signal_rows_by_language = {
        language: load_signal_rows(dataset) for language, dataset in datasets.items()
    }

    grouped: dict[tuple[str, str, str, str], dict[str, object]] = {}
    errors: List[str] = []

    for index, row in enumerate(source_rows, start=2):
        source_name = clean_text(row.get("Name"))
        source_set_name = clean_text(row.get("Set Name"))
        source_card_number = clean_text(row.get("Card Number"))
        source_variant = clean_text(row.get("Variant"))
        source_language = normalize_language(row.get("Language"))
        source_notes = clean_text(row.get("Notes"))
        condition = normalize_condition(default_condition)

        if not source_language or source_language not in datasets:
            errors.append(
                f"row {index}: unsupported language '{clean_text(row.get('Language')) or '<blank>'}'"
            )
            continue
        if not source_name or not source_set_name or not source_card_number or not source_variant:
            errors.append(f"row {index}: missing one of Name, Set Name, Card Number, or Variant")
            continue

        matches = find_candidate_rows(
            source_name=source_name,
            source_set_name=source_set_name,
            source_card_number=source_card_number,
            source_variant=source_variant,
            signal_rows=signal_rows_by_language[source_language],
        )
        if not matches:
            errors.append(
                f"row {index}: no dataset match for {source_name} | {source_set_name} | {source_card_number} | {source_variant} | {source_language}"
            )
            continue
        if len(matches) > 1:
            match_sets = ", ".join(sorted({clean_text(match.get('groupName')) for match in matches}))
            errors.append(
                f"row {index}: multiple dataset matches for {source_name} | {source_card_number} | {source_variant} ({match_sets})"
            )
            continue

        match = matches[0]
        product_id = clean_text(match.get("productId"))
        subtype = normalize_variant_label(match.get("subTypeName"))
        sku = build_variant_sku(product_id, subtype)
        group_key = (sku, product_id, subtype, source_language)
        bucket = grouped.setdefault(
            group_key,
            {
                "sku": sku,
                "product_id": product_id,
                "subtype": subtype,
                "language": source_language,
                "condition": condition,
                "quantity": 0,
                "price_override": "",
                "title_override": "",
                "notes": [],
                "_position": clean_text(row.get("Position")),
            },
        )
        bucket["quantity"] = int(bucket["quantity"]) + 1
        if source_notes and source_notes not in bucket["notes"]:
            cast_notes = bucket["notes"]
            assert isinstance(cast_notes, list)
            cast_notes.append(source_notes)

    def sort_key(item: dict[str, object]) -> tuple[int, str]:
        raw_position = clean_text(item.get("_position"))
        if raw_position.isdigit():
            return (int(raw_position), str(item.get("sku") or ""))
        return (10**9, str(item.get("sku") or ""))

    rows: List[dict[str, str]] = []
    for item in sorted(grouped.values(), key=sort_key):
        notes = " | ".join(item["notes"]) if item["notes"] else ""
        rows.append(
            {
                "sku": str(item["sku"]),
                "product_id": str(item["product_id"]),
                "subtype": str(item["subtype"]),
                "language": str(item["language"]),
                "condition": str(item["condition"]),
                "quantity": str(item["quantity"]),
                "price_override": "",
                "title_override": "",
                "notes": notes,
            }
        )

    return rows, errors


def read_source_csv(path: Path) -> List[dict[str, str]]:
    return read_csv_rows(path)


def validate_source_columns(fieldnames: Iterable[str] | None) -> None:
    field_set = {clean_text(name) for name in fieldnames or []}
    missing = [field for field in SOURCE_REQUIRED_FIELDS if field not in field_set]
    if missing:
        raise ValueError(
            f"CSV does not look like a collection-builder export. Missing columns: {', '.join(missing)}"
        )


def convert_collection_csv(
    source_csv: Path,
    output_csv: Path,
    *,
    datasets: Dict[str, DatasetPaths] | None = None,
) -> tuple[List[dict[str, str]], List[str]]:
    with source_csv.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        validate_source_columns(reader.fieldnames)
        rows, errors = convert_collection_rows(list(reader), datasets=datasets)
    if errors:
        return rows, errors
    write_csv(output_csv, INTAKE_FIELDS, rows)
    return rows, errors


def main() -> int:
    args = parse_args()
    rows, errors = convert_collection_csv(
        Path(args.source_csv),
        Path(args.output_csv),
    )
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        print(f"Converted rows before failure: {len(rows)}")
        return 1

    print(f"Wrote intake rows: {len(rows)}")
    print(f"Output CSV: {args.output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
