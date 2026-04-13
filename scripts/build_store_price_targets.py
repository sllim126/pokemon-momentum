#!/usr/bin/env python3
"""
Build one daily target-price CSV for the store from local market snapshots.
"""

from __future__ import annotations

import csv
import os
import re
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXPORT_CSV = REPO_ROOT / "products_Apr-09_04-31-18PM.csv"
JP_SIGNAL_CSV = REPO_ROOT / "data" / "extracted" / "pokemon_jp_product_signal_snapshot.csv"
EN_SIGNAL_CSV = REPO_ROOT / "data" / "extracted" / "pokemon_product_signal_snapshot.csv"
RULES_CSV = REPO_ROOT / "data" / "store_price_rules.csv"
OUTPUT_CSV = REPO_ROOT / "data" / "market_prices_latest.csv"
SUPPLIER_QUOTES_CSV = REPO_ROOT / "data" / "supplier_quotes.csv"

DEFAULT_JPY_PER_USD = Decimal(os.getenv("SUPPLIER_FLOOR_JPY_PER_USD", "145"))
DEFAULT_IMPORT_DUTY_PCT = Decimal(os.getenv("SUPPLIER_FLOOR_IMPORT_DUTY_PCT", "10"))
DEFAULT_INBOUND_SHIPPING_MODE = os.getenv("SUPPLIER_FLOOR_INBOUND_MODE", "order-estimate").strip().lower()
DEFAULT_ORDER_SHIPPING_JPY = Decimal(os.getenv("SUPPLIER_FLOOR_ORDER_SHIPPING_JPY", "13800"))
DEFAULT_ORDER_BOX_COUNT = Decimal(os.getenv("SUPPLIER_FLOOR_ORDER_BOX_COUNT", "11"))
DEFAULT_INBOUND_SHIPPING_USD = Decimal(os.getenv("SUPPLIER_FLOOR_INBOUND_SHIPPING_USD", "5.50"))
DEFAULT_HANDLING_COST_USD = Decimal(os.getenv("SUPPLIER_FLOOR_HANDLING_COST_USD", "0.75"))
DEFAULT_OUTBOUND_SHIPPING_USD = Decimal(os.getenv("SUPPLIER_FLOOR_OUTBOUND_SHIPPING_USD", "6.25"))
DEFAULT_SHIPPING_CREDIT_USD = Decimal(os.getenv("SUPPLIER_FLOOR_SHIPPING_CREDIT_USD", "0"))
DEFAULT_DISBURSEMENT_FEE_USD = Decimal(os.getenv("SUPPLIER_FLOOR_DISBURSEMENT_FEE_USD", "15"))
DEFAULT_PLATFORM_FEE_PCT = Decimal(os.getenv("SUPPLIER_FLOOR_PLATFORM_FEE_PCT", "10"))
DEFAULT_PAYMENT_FEE_PCT = Decimal(os.getenv("SUPPLIER_FLOOR_PAYMENT_FEE_PCT", "0"))
DEFAULT_PAYMENT_FEE_FIXED = Decimal(os.getenv("SUPPLIER_FLOOR_PAYMENT_FEE_FIXED_USD", "0.30"))
DEFAULT_TARGET_MARGIN_PCT = Decimal(os.getenv("SUPPLIER_FLOOR_TARGET_MARGIN_PCT", "15"))
DEFAULT_JP_BOOSTER_BOX_JPY_PER_USD = Decimal(os.getenv("JP_BOOSTER_BOX_JPY_PER_USD", "159"))
DEFAULT_JP_BOOSTER_BOX_ORDER_SHIPPING_JPY = Decimal(os.getenv("JP_BOOSTER_BOX_ORDER_SHIPPING_JPY", "13800"))
DEFAULT_JP_BOOSTER_BOX_ORDER_COUNT = Decimal(os.getenv("JP_BOOSTER_BOX_ORDER_COUNT", "10"))
DEFAULT_JP_BOOSTER_BOX_MARKUP_PCT = Decimal(os.getenv("JP_BOOSTER_BOX_MARKUP_PCT", "20"))


def parse_decimal(value: str) -> Decimal | None:
    """Return a Decimal for CSV/env input or None when the field is blank/invalid.

    Expected input:
    - strings from CSV cells or environment variables
    - blank strings when a value is intentionally omitted

    Expected output:
    - Decimal when parsing succeeds
    - None when the caller should treat the field as "not provided"
    """
    value = (value or "").strip()
    if not value:
        return None
    try:
        return Decimal(value)
    except Exception:
        return None


def normalize_name(value: str) -> str:
    """Normalize product titles for fuzzy name-based joins between rule rows and market rows.

    Expected input:
    - a human-facing product title such as "Pokemon Mega Dream Ex Booster Box - Japanese"

    Expected output:
    - lowercased ASCII-ish token string suitable for dictionary lookup
    - examples: "mega dream ex booster box", "white flare booster box"
    """
    value = value.lower()
    value = value.replace("pokemon ", "")
    value = value.replace(" - japanese", "")
    value = value.replace(" - english", "")
    value = value.replace(" japanese", "")
    value = value.replace(" english", "")
    value = value.replace("&", "and")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def round_price(value: Decimal, cents: int = 2) -> Decimal:
    """Round a Decimal money value to the requested number of cents."""
    if cents <= 0:
        return value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    quant = Decimal("1").scaleb(-cents)
    return value.quantize(quant, rounding=ROUND_HALF_UP)


def round_down_to_ending(value: Decimal, ending: Decimal) -> Decimal:
    """Snap a price downward to a merchandising ending such as .99.

    Expected behavior:
    - never rounds upward above the requested value
    - keeps a target already ending in `.99` unchanged
    - used for both market targets and profit-floor targets
    """
    whole = value.to_integral_value(rounding=ROUND_FLOOR)
    candidate = whole + ending
    if candidate > value:
        candidate -= Decimal("1")
    return round_price(candidate, 2)


def load_latest_supplier_quotes(path: Path) -> dict[str, dict[str, str]]:
    """Load the newest saved supplier quote row for each SKU.

    Source file:
    - `data/supplier_quotes.csv`

    Expected output:
    - dict keyed by SKU
    - each value is the most recent quote row based on `(quote_date, quote_id, item_name_raw)`

    Why this exists:
    - the pricing builder only wants one active supplier cost per SKU
    - historical quotes remain in the CSV for audit/debug purposes
    """
    latest_by_sku: dict[str, dict[str, str]] = {}
    if not path.exists():
        return latest_by_sku
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            sku = (row.get("sku") or "").strip()
            if not sku:
                continue
            sort_key = (
                (row.get("quote_date") or "").strip(),
                (row.get("quote_id") or "").strip(),
                (row.get("item_name_raw") or "").strip().lower(),
            )
            existing = latest_by_sku.get(sku)
            existing_key = (
                (existing.get("quote_date") or "").strip(),
                (existing.get("quote_id") or "").strip(),
                (existing.get("item_name_raw") or "").strip().lower(),
            ) if existing else None
            if existing is None or sort_key >= existing_key:
                latest_by_sku[sku] = row
    return latest_by_sku


def compute_profit_floor(cost_jpy: Decimal) -> Decimal | None:
    """Convert a supplier JPY quote into a minimum viable USD listing price.

    Expected input:
    - `cost_jpy`: supplier unit cost for one SKU from the latest quote row

    Expected output:
    - Decimal target ending in `.99` when the current env assumptions produce a valid floor
    - None when required inputs are invalid, e.g. zero FX rate or non-positive cost

    Calculation summary:
    - convert supplier JPY to USD
    - add import duty, inbound shipping, handling, and disbursement
    - add outbound shipping and fixed payment fee
    - solve for the listing price that preserves the configured target margin

    Debug note:
    - if this number is much higher than market, the generated CSV will mark
      `target_source=profit_floor`, which tells the operator why the final target jumped.
    """
    if cost_jpy <= 0 or DEFAULT_JPY_PER_USD <= 0:
        return None
    supplier_cost_usd = cost_jpy / DEFAULT_JPY_PER_USD
    import_cost_usd = supplier_cost_usd * (DEFAULT_IMPORT_DUTY_PCT / Decimal("100"))
    inbound_shipping_usd = DEFAULT_INBOUND_SHIPPING_USD
    if (
        DEFAULT_INBOUND_SHIPPING_MODE == "order-estimate"
        and DEFAULT_ORDER_SHIPPING_JPY > 0
        and DEFAULT_ORDER_BOX_COUNT > 0
        and DEFAULT_JPY_PER_USD > 0
    ):
        inbound_shipping_usd = (DEFAULT_ORDER_SHIPPING_JPY / DEFAULT_ORDER_BOX_COUNT) / DEFAULT_JPY_PER_USD

    landed_cost_usd = (
        supplier_cost_usd
        + import_cost_usd
        + inbound_shipping_usd
        + DEFAULT_HANDLING_COST_USD
        + DEFAULT_DISBURSEMENT_FEE_USD
    )
    # The daily store target should protect a minimum viable margin, not just
    # mirror the US secondary market. This floor uses the same supplier-side
    # assumptions surfaced on the dashboard so auto-pricing does not undercut
    # a SKU below a survivable listing price.
    fixed_costs = landed_cost_usd + DEFAULT_OUTBOUND_SHIPPING_USD + DEFAULT_PAYMENT_FEE_FIXED - DEFAULT_SHIPPING_CREDIT_USD
    fee_rate = (DEFAULT_PLATFORM_FEE_PCT + DEFAULT_PAYMENT_FEE_PCT) / Decimal("100")
    margin_rate = DEFAULT_TARGET_MARGIN_PCT / Decimal("100")
    denominator = Decimal("1") - fee_rate - margin_rate
    if denominator <= 0:
        return None
    required_price = fixed_costs / denominator
    return round_down_to_ending(required_price, Decimal("0.99"))


def is_jp_booster_box_rule(rule: dict[str, str]) -> bool:
    """Return whether a pricing rule should use the JP landed-cost markup logic.

    Expected input:
    - one row from `store_price_rules.csv`

    Expected output:
    - True for Japanese booster-box rows that should reflect current supplier costs
    - False for everything else so the legacy pricing logic stays unchanged
    """
    market_source = (rule.get("market_source") or "").strip().lower()
    note = (rule.get("note") or "").strip().lower()
    lookup_value = (rule.get("lookup_value") or "").strip().lower()
    return (
        market_source == "jp"
        and ("booster box" in note or "booster box" in lookup_value)
    )


def compute_jp_booster_box_floor(cost_jpy: Decimal) -> Decimal | None:
    """Price JP booster boxes at 20% above landed cost from the latest supplier quote.

    Expected input:
    - `cost_jpy`: latest quoted supplier cost for one Japanese booster box

    Expected output:
    - Decimal target ending in `.99`
    - None when the quote or FX/shipping assumptions are invalid

    Landed-cost model:
    - convert supplier JPY to USD
    - add 10% import duty
    - allocate `13,800 JPY` shipping across `10` mixed boxes
    - add handling and disbursement
    - multiply by `1.20`
    """
    if cost_jpy <= 0 or DEFAULT_JP_BOOSTER_BOX_JPY_PER_USD <= 0 or DEFAULT_JP_BOOSTER_BOX_ORDER_COUNT <= 0:
        return None
    supplier_cost_usd = cost_jpy / DEFAULT_JP_BOOSTER_BOX_JPY_PER_USD
    import_cost_usd = supplier_cost_usd * (DEFAULT_IMPORT_DUTY_PCT / Decimal("100"))
    inbound_shipping_usd = (DEFAULT_JP_BOOSTER_BOX_ORDER_SHIPPING_JPY / DEFAULT_JP_BOOSTER_BOX_ORDER_COUNT) / DEFAULT_JP_BOOSTER_BOX_JPY_PER_USD
    landed_cost_usd = (
        supplier_cost_usd
        + import_cost_usd
        + inbound_shipping_usd
        + DEFAULT_HANDLING_COST_USD
        + DEFAULT_DISBURSEMENT_FEE_USD
    )
    target = landed_cost_usd * (Decimal("1") + (DEFAULT_JP_BOOSTER_BOX_MARKUP_PCT / Decimal("100")))
    return round_down_to_ending(target, Decimal("0.99"))


def compute_target_price(market_price: Decimal, pricing_mode: str, min_price: Decimal | None) -> Decimal:
    """Compute the market-aware target before any supplier-profit protection is applied.

    Expected input:
    - `market_price`: latest market price from the English or Japanese signal snapshot
    - `pricing_mode`: rule mode from `store_price_rules.csv`
    - `min_price`: optional hard floor from the rule row

    Expected output:
    - Decimal target price suitable for writing to `market_prices_latest.csv`

    Current supported modes:
    - `market`
    - `market_minus_5_pct_99`
    - `market_minus_5_pct_99_with_floor`
    """
    if pricing_mode == "market":
        target = round_price(market_price, 2)
    elif pricing_mode in {"market_minus_5_pct_99", "market_minus_5_pct_99_with_floor"}:
        target = round_down_to_ending(market_price * Decimal("0.95"), Decimal("0.99"))
    else:
        raise ValueError(f"Unsupported pricing_mode: {pricing_mode}")

    if min_price is not None and target < min_price:
        target = min_price
    return round_price(target, 2)


def load_signal_rows(path: Path) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    """Load market snapshot rows keyed by both product id and normalized name.

    Expected output:
    - tuple of `(by_id, by_name)` dictionaries

    Why both are needed:
    - English sealed often uses stable TCGplayer product ids
    - some Japanese rules still join by name because that data is easier to maintain
    """
    by_id: dict[str, dict[str, str]] = {}
    by_name: dict[str, dict[str, str]] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            by_id[row["productId"]] = row
            by_name[normalize_name(row["productName"])] = row
    return by_id, by_name


def load_store_rows(path: Path) -> dict[str, dict[str, str]]:
    """Load Squarespace export rows keyed by SKU.

    Expected output:
    - dict where each value is the raw export row

    Important:
    - this file is the source of truth for whether a SKU currently exists in the store export
    - if a rule SKU is missing here, `build_target_rows` records it as unmatched
    """
    rows: dict[str, dict[str, str]] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            sku = (row.get("SKU") or "").strip()
            if sku:
                rows[sku] = row
    return rows


def build_target_rows(export_csv: Path = DEFAULT_EXPORT_CSV) -> tuple[list[dict[str, str]], list[str]]:
    """Build the complete store pricing feed consumed by the Squarespace sync job.

    Inputs:
    - Squarespace export CSV for live store SKUs/titles
    - English and Japanese market snapshot CSVs
    - `store_price_rules.csv` describing how each SKU should be priced
    - latest supplier quote per SKU, when available

    Expected output:
    - `output_rows`: list of CSV-ready dict rows with:
      `sku`, `market_price`, `target_price`, `profit_floor_price`,
      `supplier_cost_jpy`, `target_source`, `title`, `market_title`,
      `pricing_mode`, `market_source`
    - `unmatched`: human-readable reasons for skipped rows

    Final target rule:
    - start with the market-aware target from the pricing rule
    - if a supplier quote exists and the computed profit floor is higher, override with that floor
    - rows marked `pricing_mode=manual` are intentionally omitted
    """
    store_rows = load_store_rows(export_csv)
    jp_by_id, jp_by_name = load_signal_rows(JP_SIGNAL_CSV)
    en_by_id, en_by_name = load_signal_rows(EN_SIGNAL_CSV)
    latest_supplier_quotes = load_latest_supplier_quotes(SUPPLIER_QUOTES_CSV)
    signal_maps = {
        "jp": (jp_by_id, jp_by_name),
        "en": (en_by_id, en_by_name),
    }

    output_rows: list[dict[str, str]] = []
    unmatched: list[str] = []

    with RULES_CSV.open(newline="") as handle:
        for rule in csv.DictReader(handle):
            sku = (rule.get("sku") or "").strip()
            if not sku:
                continue

            store_row = store_rows.get(sku)
            if store_row is None:
                unmatched.append(f"{sku}: missing from Squarespace export")
                continue

            pricing_mode = (rule.get("pricing_mode") or "").strip()
            if pricing_mode == "manual":
                continue

            source = (rule.get("market_source") or "").strip()
            lookup_type = (rule.get("lookup_type") or "").strip()
            lookup_value = (rule.get("lookup_value") or "").strip()
            min_price = parse_decimal(rule.get("min_price") or "")
            by_id, by_name = signal_maps[source]

            market_row = by_id.get(lookup_value) if lookup_type == "product_id" else by_name.get(normalize_name(lookup_value))
            if market_row is None:
                unmatched.append(f"{sku}: no market match for {lookup_value}")
                continue

            market_price = parse_decimal(market_row.get("latest_price") or "")
            if market_price is None:
                unmatched.append(f"{sku}: missing latest_price in market data")
                continue

            target_price = compute_target_price(market_price, pricing_mode, min_price)
            supplier_quote = latest_supplier_quotes.get(sku)
            supplier_cost_jpy = parse_decimal((supplier_quote or {}).get("cost_jpy") or "")
            if supplier_cost_jpy is not None and is_jp_booster_box_rule(rule):
                profit_floor_price = compute_jp_booster_box_floor(supplier_cost_jpy)
                target_source = "jp_landed_markup"
            else:
                profit_floor_price = compute_profit_floor(supplier_cost_jpy) if supplier_cost_jpy is not None else None
                target_source = "profit_floor"
            final_target_price = target_price
            # Prefer the higher of the market-aware target and the supplier-cost
            # floor so automation protects margin by default.
            if profit_floor_price is not None and profit_floor_price > final_target_price:
                final_target_price = profit_floor_price
            output_rows.append(
                {
                    "sku": sku,
                    "market_price": str(round_price(market_price, 2)),
                    "target_price": str(final_target_price),
                    "title": store_row.get("Title") or "",
                    "market_title": market_row.get("productName") or "",
                    "pricing_mode": pricing_mode,
                    "market_source": source,
                    "profit_floor_price": str(profit_floor_price) if profit_floor_price is not None else "",
                    "supplier_cost_jpy": str(supplier_cost_jpy) if supplier_cost_jpy is not None else "",
                    "target_source": target_source if profit_floor_price is not None and profit_floor_price > target_price else "market",
                }
            )

    return output_rows, unmatched


def main() -> int:
    """CLI entrypoint that writes `data/market_prices_latest.csv`.

    Expected side effects:
    - creates/overwrites the output CSV
    - prints a short summary plus any unmatched rule rows
    - exits with code `0` on success
    """
    output_rows, unmatched = build_target_rows(DEFAULT_EXPORT_CSV)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sku",
                "market_price",
                "target_price",
                "profit_floor_price",
                "supplier_cost_jpy",
                "target_source",
                "title",
                "market_title",
                "pricing_mode",
                "market_source",
            ],
        )
        writer.writeheader()
        writer.writerows(sorted(output_rows, key=lambda row: row["sku"]))

    print(f"Wrote {len(output_rows)} rows to {OUTPUT_CSV}")
    if unmatched:
        print("Skipped or unmatched rules:")
        for row in unmatched:
            print(f"  {row}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
