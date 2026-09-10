#!/usr/bin/env python3
"""
Build an inventory cleanup report from a Squarespace product export.

This focuses on practical cleanup tasks:
- missing categories
- missing tags
- hidden items that may need review
- rough recommended categories/tags for sealed inventory
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "output"
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "inventory_cleanup_report.csv"

REPORT_FIELDS = [
    "sku",
    "title",
    "inventory_type",
    "visible",
    "stock",
    "current_categories",
    "current_tags",
    "issue_flags",
    "recommended_categories",
    "recommended_tags",
    "priority",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an inventory cleanup report.")
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV))
    return parser.parse_args()


def clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def detect_delimiter(path: Path) -> str:
    sample = path.read_text(encoding="utf-8")[:4096]
    return "\t" if "\t" in sample else ","


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter=detect_delimiter(path)))


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


def is_japanese(row: dict[str, str]) -> bool:
    title = clean_text(row.get("Title")).lower()
    tags = clean_text(row.get("Tags")).lower()
    sku = clean_text(row.get("SKU")).upper()
    return "japanese" in title or "japanese" in tags or sku.startswith("JP-")


def recommend_sealed_categories(row: dict[str, str]) -> str:
    title = clean_text(row.get("Title"))
    sku = clean_text(row.get("SKU")).upper()
    lower = title.lower()
    categories: list[str] = []

    if "booster box" in lower:
        categories.extend(["/booster-box", "/booster-box/japanese" if is_japanese(row) else "/booster-box/english"])
    elif "booster bundle" in lower:
        categories.append("/booster-bundle")
    elif "elite trainer box" in lower:
        categories.append("/elite-trainer-box")
    elif "2-pack blister" in lower or "sleeved booster" in lower:
        categories.extend(["/booster-pack", "/booster-pack/english"])
    elif "tin" in lower:
        categories.append("/other/tins")
    elif any(token in lower for token in ["collection", "premium collection", "ex box", "v-union"]):
        categories.append("/other/collection-boxes")
    else:
        categories.append("/other/other")

    set_map = {
        "perfect order": "/mega-evolution/perfect-order",
        "chaos rising": "/mega-evolution/chaos-rising",
        "pitch black": "/mega-evolution/pitch-black",
        "ascended heroes": "/mega-evolution/ascended-heroes",
        "crown zenith": "/sword-shield/crown-zenith",
        "151": "/scarlet-violet/sv-151",
        "black bolt": "/scarlet-violet/black-bolt",
    }
    for token, category in set_map.items():
        if token in lower or token in sku.lower():
            categories.append(category)
            break

    seen: list[str] = []
    for category in categories:
        if category not in seen:
            seen.append(category)
    return ", ".join(seen)


def recommend_sealed_tags(row: dict[str, str]) -> str:
    title = clean_text(row.get("Title"))
    lower = title.lower()
    tags: list[str] = ["Japanese" if is_japanese(row) else "English"]

    if "pokemon center" in lower:
        tags.append("Pokemon Center")
    if "costco" in lower:
        tags.append("Costco")
    if "poke ball tin" in lower:
        tags.append("Poke Ball Tin")
    if "perfect order" in lower:
        tags.append("Perfect Order")
    elif "chaos rising" in lower:
        tags.append("Chaos Rising")
    elif "pitch black" in lower:
        tags.append("Pitch Black")
    elif "ascended heroes" in lower:
        tags.append("Ascended Heroes")
    elif "crown zenith" in lower:
        tags.append("Crown Zenith")
    elif "black bolt" in lower:
        tags.append("Black Bolt")
    elif "151" in lower:
        tags.append("SV 151")

    seen: list[str] = []
    for tag in tags:
        if tag not in seen:
            seen.append(tag)
    return ", ".join(seen)


def build_issue_flags(row: dict[str, str], inventory_type: str) -> list[str]:
    flags: list[str] = []
    if not clean_text(row.get("Categories")):
        flags.append("missing_categories")
    if not clean_text(row.get("Tags")):
        flags.append("missing_tags")
    if clean_text(row.get("Visible")).lower() == "no":
        flags.append("hidden")
    if inventory_type == "sealed":
        title = clean_text(row.get("Title")).lower()
        if "pokemon center elite trainer box" in title and "Pokemon Center" not in clean_text(row.get("Tags")):
            flags.append("missing_pokemon_center_tag")
    return flags


def priority_for(flags: list[str], inventory_type: str, stock: str) -> str:
    if inventory_type == "sealed" and stock not in {"", "0", "No", "no"} and "missing_categories" in flags:
        return "high"
    if inventory_type == "sealed" and "missing_tags" in flags:
        return "medium"
    if "hidden" in flags:
        return "review"
    return "low"


def build_report(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    report: list[dict[str, str]] = []
    for row in rows:
        inventory_type = "single" if is_single(row) else "sealed"
        issue_flags = build_issue_flags(row, inventory_type)
        if not issue_flags:
            continue

        current_stock = clean_text(row.get("Stock"))
        report.append(
            {
                "sku": clean_text(row.get("SKU")),
                "title": clean_text(row.get("Title")),
                "inventory_type": inventory_type,
                "visible": clean_text(row.get("Visible")),
                "stock": current_stock,
                "current_categories": clean_text(row.get("Categories")),
                "current_tags": clean_text(row.get("Tags")),
                "issue_flags": ", ".join(issue_flags),
                "recommended_categories": recommend_sealed_categories(row) if inventory_type == "sealed" else "",
                "recommended_tags": recommend_sealed_tags(row) if inventory_type == "sealed" else "",
                "priority": priority_for(issue_flags, inventory_type, current_stock),
            }
        )
    return report


def main() -> int:
    args = parse_args()
    input_csv = Path(args.input_csv)
    output_csv = Path(args.output_csv)
    if not input_csv.exists():
        raise SystemExit(f"Input CSV not found: {input_csv}")
    rows = read_rows(input_csv)
    report = build_report(rows)
    write_csv(output_csv, REPORT_FIELDS, report)
    print(f"Wrote {len(report)} rows to {output_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
