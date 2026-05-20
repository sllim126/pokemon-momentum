#!/usr/bin/env python3
"""
Audit visible store SKUs and classify pricing automation coverage.
"""

from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
EXPORT_CSV = REPO_ROOT / "products_Apr-09_04-31-18PM.csv"
RULES_CSV = REPO_ROOT / "data" / "store_price_rules.csv"
MARKET_CSV = REPO_ROOT / "data" / "market_prices_latest.csv"
EXPECTATIONS_CSV = REPO_ROOT / "data" / "store_price_audit_expectations.csv"


def load_rule_modes(path: Path) -> dict[str, str]:
    modes: dict[str, str] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            sku = str(row.get("sku") or "").strip()
            if sku:
                modes[sku] = str(row.get("pricing_mode") or "").strip()
    return modes


def load_market_skus(path: Path) -> set[str]:
    skus: set[str] = set()
    if not path.exists():
        return skus
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            sku = str(row.get("sku") or "").strip()
            if sku:
                skus.add(sku)
    return skus


def load_expectations(path: Path) -> dict[str, dict[str, str]]:
    expectations: dict[str, dict[str, str]] = {}
    if not path.exists():
        return expectations
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            sku = str(row.get("sku") or "").strip()
            if sku:
                expectations[sku] = {
                    "classification": str(row.get("classification") or "").strip(),
                    "note": str(row.get("note") or "").strip(),
                }
    return expectations


def iter_visible_export_rows(path: Path):
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            sku = str(row.get("SKU") or "").strip()
            if not sku:
                continue
            if str(row.get("Visible") or "").strip().lower() != "yes":
                continue
            yield {
                "sku": sku,
                "title": str(row.get("Title") or "").strip(),
                "price": str(row.get("Price") or "").strip(),
                "stock": str(row.get("Stock") or "").strip(),
                "categories": str(row.get("Categories") or "").strip(),
            }


def classify_sku(
    sku: str,
    rule_modes: dict[str, str],
    market_skus: set[str],
    expectations: dict[str, dict[str, str]],
) -> tuple[str, str]:
    rule_mode = rule_modes.get(sku, "")
    expected = expectations.get(sku)

    if sku in market_skus:
        return "automated", "Included in generated pricing targets"
    if rule_mode == "manual":
        return "manual_expected", "Manual pricing rule"
    if expected is not None:
        return expected["classification"], expected["note"]
    if sku in rule_modes:
        return "missing_unexpected", "Has pricing rule but is missing from generated targets"
    return "missing_unexpected", "Visible SKU has no pricing rule or expectation"


def build_audit_rows() -> list[dict[str, str]]:
    rule_modes = load_rule_modes(RULES_CSV)
    market_skus = load_market_skus(MARKET_CSV)
    expectations = load_expectations(EXPECTATIONS_CSV)

    rows: list[dict[str, str]] = []
    for row in iter_visible_export_rows(EXPORT_CSV):
        classification, note = classify_sku(row["sku"], rule_modes, market_skus, expectations)
        rows.append(
            {
                **row,
                "classification": classification,
                "note": note,
            }
        )
    rows.sort(key=lambda item: (item["classification"], item["sku"]))
    return rows


def main() -> int:
    rows = build_audit_rows()
    counts = Counter(row["classification"] for row in rows)

    print(f"Visible SKUs audited: {len(rows)}")
    for name in ["automated", "manual_expected", "hold_expected", "missing_unexpected"]:
        print(f"{name}: {counts.get(name, 0)}")

    for name in ["missing_unexpected", "hold_expected", "manual_expected"]:
        matching = [row for row in rows if row["classification"] == name]
        if not matching:
            continue
        print(f"\n{name}:")
        for row in matching:
            print(
                f"  {row['sku']} | {row['price']} | stock={row['stock']} | {row['title']} | {row['note']}"
            )
    return 0 if counts.get("missing_unexpected", 0) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
