#!/usr/bin/env python3
"""
Build a market price CSV for Japanese booster boxes from the latest local signal snapshot.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXPORT_CSV = REPO_ROOT / "products_Apr-09_04-31-18PM.csv"
DEFAULT_SIGNAL_CSV = REPO_ROOT / "data" / "extracted" / "pokemon_jp_product_signal_snapshot.csv"
DEFAULT_OUTPUT_CSV = REPO_ROOT / "data" / "market_prices_latest.csv"

SKU_NAME_OVERRIDES = {
    "JP-151-BOX": "Pokemon Card 151 Booster Box",
    "JP-SHINY-BOX": "Shiny Treasure ex High Class Booster Box",
    "JP-TR-BB": "Glory of Team Rocket Booster Box",
    "JP-TF-BB": "Terastal Fest ex Booster Box",
    "MYZ-JP-BB": "Nihil Zero Booster Box",
    "JP-WIFO-BOX": "Wild Force Booster Box",
}

EXCLUDED_SKUS = {
    "JP-MPTB-BOX",
}


def normalize_name(value: str) -> str:
    value = value.lower()
    value = value.replace("pokemon ", "")
    value = value.replace(" - japanese", "")
    value = value.replace(" japanese", "")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def load_market_prices(signal_csv: Path) -> dict[str, tuple[str, str]]:
    market_by_name: dict[str, tuple[str, str]] = {}
    with signal_csv.open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row["productKind"] != "sealed" or row["productClass"] != "sealed_booster_box":
                continue
            market_by_name[normalize_name(row["productName"])] = (
                row["productName"],
                row["latest_price"],
            )
    return market_by_name


def iter_store_rows(export_csv: Path):
    with export_csv.open(newline="") as handle:
        for row in csv.DictReader(handle):
            if "/booster-box/japanese" not in row["Categories"]:
                continue
            yield row


def main() -> int:
    market_by_name = load_market_prices(DEFAULT_SIGNAL_CSV)
    output_rows: list[dict[str, str]] = []
    unmatched: list[tuple[str, str]] = []
    seen_skus: set[str] = set()

    for row in iter_store_rows(DEFAULT_EXPORT_CSV):
        sku = row["SKU"].strip()
        title = row["Title"].strip()
        if not sku or sku in EXCLUDED_SKUS:
            continue
        if sku in seen_skus:
            raise SystemExit(f"Duplicate SKU in Squarespace export: {sku}")
        seen_skus.add(sku)

        lookup_name = SKU_NAME_OVERRIDES.get(sku, title)
        market_row = market_by_name.get(normalize_name(lookup_name))
        if market_row is None:
            unmatched.append((sku, title))
            continue

        market_title, market_price = market_row
        output_rows.append(
            {
                "sku": sku,
                "market_price": market_price,
                "title": title,
                "market_title": market_title,
            }
        )

    DEFAULT_OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with DEFAULT_OUTPUT_CSV.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["sku", "market_price", "title", "market_title"])
        writer.writeheader()
        writer.writerows(sorted(output_rows, key=lambda row: row["sku"]))

    print(f"Wrote {len(output_rows)} rows to {DEFAULT_OUTPUT_CSV}")
    if unmatched:
        print("Unmatched store rows:")
        for sku, title in unmatched:
            print(f"  {sku}: {title}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
