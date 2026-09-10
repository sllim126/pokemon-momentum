#!/usr/bin/env python3
"""
Build a standardized Squarespace import CSV for all sealed/store inventory rows.

This is meant for inventory cleanup, not listing creation. It preserves the
existing store export as the source of truth for prices, stock, visibility,
images, and richer legacy descriptions, while standardizing categories and tags
to the newer sealed-inventory scheme.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "output"
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "standardized_sealed_inventory_import.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a standardized Squarespace import CSV for sealed inventory."
    )
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV))
    return parser.parse_args()


def clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def detect_delimiter(path: Path) -> str:
    sample = path.read_text(encoding="utf-8")[:4096]
    return "\t" if "\t" in sample else ","


def read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    delimiter = detect_delimiter(path)
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def write_csv(path: Path, fieldnames: Iterable[str], rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def is_single(row: dict[str, str]) -> bool:
    sku = clean_text(row.get("SKU"))
    tags = clean_text(row.get("Tags"))
    description = clean_text(row.get("Description"))
    return (
        sku.isdigit()
        or "Singles" in tags
        or "Condition:" in description
        or "Images represent the card version" in description
    )


def is_accessory_or_printify(row: dict[str, str]) -> bool:
    title = clean_text(row.get("Title")).lower()
    sku = clean_text(row.get("SKU")).lower()
    handle = clean_text(row.get("Product URL")).lower()
    categories = clean_text(row.get("Categories")).lower()
    tags = clean_text(row.get("Tags")).lower()
    description = clean_text(row.get("Description")).lower()
    accessory_markers = [
        "desk mat",
        "play mat",
        "display stand",
        "whatnot",
        "live streams",
        "streaming mat",
    ]
    return any(
        marker in text
        for marker in accessory_markers
        for text in [title, sku, handle, categories, tags, description]
    )


def is_japanese(row: dict[str, str]) -> bool:
    title = clean_text(row.get("Title")).lower()
    tags = clean_text(row.get("Tags")).lower()
    sku = clean_text(row.get("SKU")).upper()
    return "japanese" in title or "japanese" in tags or sku.startswith("JP-") or sku.endswith("-JP-BB")


def infer_set_tag(row: dict[str, str]) -> str:
    title = clean_text(row.get("Title"))
    sku = clean_text(row.get("SKU")).upper()
    lower = title.lower()
    checks = [
        ("perfect order", "Perfect Order"),
        ("chaos rising", "Chaos Rising"),
        ("pitch black", "Pitch Black"),
        ("ascended heroes", "Ascended Heroes"),
        ("crown zenith", "Crown Zenith"),
        ("surging sparks", "Surging Sparks"),
        ("destined rivals", "Destined Rivals"),
        ("journey together", "Journey Together"),
        ("prismatic evolutions", "Prismatic Evolutions"),
        ("sv 151", "SV 151"),
        ("pokemon 151", "SV 151"),
        ("scarlet & violet 151", "SV 151"),
        ("black bolt", "Black Bolt"),
        ("white flare", "White Flare"),
        ("phantasmal flames", "Phantasmal Flames"),
        ("mega brave", "Mega Brave"),
        ("mega symphonia", "Mega Symphonia"),
        ("inferno x", "Inferno X"),
        ("nihil zero", "Nihil Zero"),
        ("ninja spinner", "Ninja Spinner"),
        ("abyss eye", "Abyss Eye"),
        ("battle partners", "Battle Partners"),
        ("crimson haze", "Crimson Haze"),
        ("future flash", "Future Flash"),
        ("ancient roar", "Ancient Roar"),
        ("cyber judge", "Cyber Judge"),
        ("night wanderer", "Night Wanderer"),
        ("paradise dragona", "Paradise Dragona"),
        ("heat wave arena", "Heat Wave Arena"),
        ("start deck 100", "Start Deck 100"),
    ]
    for needle, label in checks:
        if needle in lower:
            return label
    if "SV-151" in sku or "151" in sku:
        return "SV 151"
    return ""


def infer_category_set_slug(set_tag: str) -> str:
    slug_map = {
        "Perfect Order": "/mega-evolution/perfect-order",
        "Chaos Rising": "/mega-evolution/chaos-rising",
        "Pitch Black": "/mega-evolution/pitch-black",
        "Ascended Heroes": "/mega-evolution/ascended-heroes",
        "Crown Zenith": "/sword-shield/crown-zenith",
        "Black Bolt": "/scarlet-violet/black-bolt",
        "SV 151": "/scarlet-violet/sv-151",
    }
    return slug_map.get(set_tag, "")


def standardized_categories(row: dict[str, str]) -> str:
    title = clean_text(row.get("Title"))
    lower = title.lower()
    set_tag = infer_set_tag(row)
    categories: list[str] = []

    if "booster box" in lower:
        categories.extend(["/booster-box", "/booster-box/japanese" if is_japanese(row) else "/booster-box/english"])
    elif "booster bundle" in lower:
        categories.append("/booster-bundle")
    elif "elite trainer box" in lower:
        categories.append("/elite-trainer-box")
    elif "sleeved booster" in lower or "booster pack" in lower or "2-pack blister" in lower:
        if is_japanese(row):
            categories.append("/other/other")
        else:
            categories.extend(["/booster-pack", "/booster-pack/english"])
    elif any(token in lower for token in ["premium collection", "ex box", "collection", "v-union"]):
        categories.append("/other/collection-boxes")
    elif "tin" in lower:
        categories.append("/other/tins")
    elif any(token in lower for token in ["bulk", "display stand", "desk mat", "start deck"]):
        categories.append("/other/other")
    else:
        categories.append("/other/other")

    set_slug = infer_category_set_slug(set_tag)
    if set_slug and set_slug not in categories:
        categories.append(set_slug)

    unique: list[str] = []
    for category in categories:
        if category and category not in unique:
            unique.append(category)
    return ", ".join(unique)


def standardized_tags(row: dict[str, str]) -> str:
    title = clean_text(row.get("Title"))
    lower = title.lower()
    set_tag = infer_set_tag(row)
    tags: list[str] = ["Japanese" if is_japanese(row) else "English"]

    if set_tag:
        tags.append(set_tag)
    if "pokemon center" in lower:
        tags.append("Pokemon Center")
    if "costco" in lower:
        tags.append("Costco")
    if "poke ball tin" in lower:
        tags.append("Poke Ball Tin")
    if "art set" in lower:
        tags.append("Art Set")
    if "bulk" in lower:
        tags.append("Bulk")
    if "display stand" in lower:
        tags.append("Display")
    if "desk mat" in lower or "play mat" in lower:
        tags.append("Playmat")
    if "random design" in clean_text(row.get("Description")).lower():
        tags.append("Random Design")

    unique: list[str] = []
    for tag in tags:
        tag = clean_text(tag)
        if tag and tag not in unique:
            unique.append(tag)
    return ", ".join(unique)


def build_output_row(row: dict[str, str], fieldnames: list[str]) -> dict[str, str]:
    output = {field: row.get(field, "") for field in fieldnames}
    output["Categories"] = standardized_categories(row)
    output["Tags"] = standardized_tags(row)
    return output


def main() -> int:
    args = parse_args()
    input_csv = Path(args.input_csv)
    output_csv = Path(args.output_csv)
    if not input_csv.exists():
        raise SystemExit(f"Input CSV not found: {input_csv}")

    fieldnames, rows = read_rows(input_csv)
    sealed_rows = [
        row for row in rows if not is_single(row) and not is_accessory_or_printify(row)
    ]
    output_rows = [build_output_row(row, fieldnames) for row in sealed_rows]
    write_csv(output_csv, fieldnames, output_rows)
    print(f"Wrote {len(output_rows)} rows to {output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
