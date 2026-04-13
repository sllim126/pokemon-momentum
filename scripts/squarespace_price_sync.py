#!/usr/bin/env python3
"""
Sync Squarespace product variant prices from a market price CSV.

Expected market CSV columns (header names are case-insensitive):
  - sku (required)
  - market_price (required)
Optional:
  - title

The script maps SKUs to Squarespace product + variant IDs using a Squarespace
product export CSV (like products_Apr-09_04-31-18PM.csv).
"""

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests


@dataclass
class VariantMapping:
    sku: str
    product_id: str
    variant_id: str
    title: str
    current_price: Optional[Decimal]
    current_sale_price: Optional[Decimal]


@dataclass
class MarketRow:
    sku: str
    market_price: Decimal
    target_price: Optional[Decimal]


def load_local_dotenv(env_path: Path) -> None:
    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _parse_decimal(value: str) -> Optional[Decimal]:
    if value is None:
        return None
    value = value.strip()
    if value == "" or value.lower() == "null":
        return None
    try:
        return Decimal(value)
    except Exception:
        return None


def load_squarespace_export(path: str) -> Dict[str, VariantMapping]:
    mapping: Dict[str, VariantMapping] = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sku = (row.get("SKU") or "").strip()
            if not sku:
                continue
            mapping[sku] = VariantMapping(
                sku=sku,
                product_id=row.get("Product ID [Non Editable]") or "",
                variant_id=row.get("Variant ID [Non Editable]") or "",
                title=row.get("Title") or "",
                current_price=_parse_decimal(row.get("Price") or ""),
                current_sale_price=_parse_decimal(row.get("Sale Price") or ""),
            )
    return mapping


def load_market_prices(path: str) -> Dict[str, MarketRow]:
    prices: Dict[str, MarketRow] = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        # Normalize fieldnames
        field_map = {name.lower(): name for name in (reader.fieldnames or [])}
        sku_field = field_map.get("sku")
        price_field = field_map.get("market_price")
        target_field = field_map.get("target_price")
        if not sku_field or not price_field:
            raise ValueError("Market CSV must contain 'sku' and 'market_price' columns")
        for row in reader:
            sku = (row.get(sku_field) or "").strip()
            if not sku:
                continue
            price = _parse_decimal(row.get(price_field) or "")
            if price is None:
                continue
            target_price = _parse_decimal(row.get(target_field) or "") if target_field else None
            prices[sku] = MarketRow(sku=sku, market_price=price, target_price=target_price)
    return prices


def round_price(value: Decimal, cents: int) -> Decimal:
    if cents <= 0:
        return value.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    quant = Decimal("1").scaleb(-cents)
    return value.quantize(quant, rounding=ROUND_HALF_UP)


def round_price_with_ending(value: Decimal, cents: int, price_ending: str) -> Decimal:
    normalized_ending = price_ending.strip()
    if not normalized_ending:
        return round_price(value, cents)

    ending = _parse_decimal(normalized_ending)
    if ending is None or ending < 0 or ending >= 1:
        raise ValueError("price_ending must be a decimal between 0 and 1, such as 0.99")

    whole = value.to_integral_value(rounding=ROUND_FLOOR)
    candidate = whole + ending
    if candidate > value:
        candidate -= Decimal("1")
    return round_price(candidate, cents)


def compute_new_price(
    market_price: Decimal,
    markup_pct: Decimal,
    min_price: Optional[Decimal],
    max_price: Optional[Decimal],
    round_cents: int,
    price_ending: str,
) -> Decimal:
    candidate = market_price * (Decimal("1") + markup_pct / Decimal("100"))
    candidate = round_price_with_ending(candidate, round_cents, price_ending)
    if min_price is not None and candidate < min_price:
        candidate = min_price
    if max_price is not None and candidate > max_price:
        candidate = max_price
    return round_price(candidate, round_cents)


def should_update(
    current: Optional[Decimal],
    new: Decimal,
    min_abs: Decimal,
    min_pct: Decimal,
) -> bool:
    if current is None:
        return True
    diff = abs(new - current)
    if diff < min_abs:
        return False
    if current > 0:
        pct = (diff / current) * Decimal("100")
        if pct < min_pct:
            return False
    return True


def build_update_payload(new_price: Decimal, currency: str, disable_sale: bool) -> dict:
    pricing_value = {
        "basePrice": {"currency": currency, "value": str(new_price)},
    }
    if disable_sale:
        pricing_value["onSale"] = False
    return {"pricing": pricing_value}


def post_update(
    base_url: str,
    api_version: str,
    api_key: str,
    user_agent: str,
    product_id: str,
    variant_id: str,
    payload: dict,
    timeout: int,
) -> requests.Response:
    url = f"{base_url}/{api_version}/commerce/products/{product_id}/variants/{variant_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": user_agent,
        "Content-Type": "application/json",
    }
    return requests.post(url, headers=headers, json=payload, timeout=timeout)


def write_mapping_csv(path: str, mapping: Iterable[VariantMapping]) -> None:
    fieldnames = [
        "sku",
        "product_id",
        "variant_id",
        "title",
        "current_price",
        "current_sale_price",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for m in mapping:
            writer.writerow(
                {
                    "sku": m.sku,
                    "product_id": m.product_id,
                    "variant_id": m.variant_id,
                    "title": m.title,
                    "current_price": str(m.current_price) if m.current_price is not None else "",
                    "current_sale_price": str(m.current_sale_price)
                    if m.current_sale_price is not None
                    else "",
                }
            )


def main() -> int:
    load_local_dotenv(Path(__file__).resolve().parents[1] / ".env")

    parser = argparse.ArgumentParser(description="Sync Squarespace prices from market CSV")
    parser.add_argument(
        "--market-csv",
        default=os.getenv("SQUARESPACE_MARKET_CSV"),
        help="Market prices CSV with sku + market_price",
    )
    parser.add_argument(
        "--squarespace-export",
        default=os.getenv(
            "SQUARESPACE_EXPORT_CSV",
            "/opt/pokemon-momentum/products_Apr-09_04-31-18PM.csv",
        ),
        help="Squarespace product export CSV",
    )
    parser.add_argument("--currency", default="USD", help="Currency code for pricing updates")
    parser.add_argument(
        "--markup-pct",
        type=Decimal,
        default=Decimal(os.getenv("SQUARESPACE_MARKUP_PCT", "0")),
    )
    parser.add_argument(
        "--discount-pct",
        type=Decimal,
        default=Decimal(os.getenv("SQUARESPACE_DISCOUNT_PCT", "0")),
        help="Positive percentage to price below market, e.g. 5 means market * 0.95",
    )
    parser.add_argument("--min-price", type=Decimal, default=None)
    parser.add_argument("--max-price", type=Decimal, default=None)
    parser.add_argument("--round-cents", type=int, default=2)
    parser.add_argument(
        "--price-ending",
        default=os.getenv("SQUARESPACE_PRICE_ENDING", ""),
        help="Snap prices downward to this fractional ending, e.g. 0.99",
    )
    parser.add_argument(
        "--min-abs-change",
        type=Decimal,
        default=Decimal(os.getenv("SQUARESPACE_MIN_ABS_CHANGE", "1.00")),
    )
    parser.add_argument(
        "--min-pct-change",
        type=Decimal,
        default=Decimal(os.getenv("SQUARESPACE_MIN_PCT_CHANGE", "1.0")),
    )
    parser.add_argument(
        "--disable-sale",
        action="store_true",
        default=os.getenv("SQUARESPACE_DISABLE_SALE", "").strip().lower() in {"1", "true", "yes"},
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=float(os.getenv("SQUARESPACE_SLEEP_SECONDS", "0.25")),
    )
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--log-jsonl",
        default=os.getenv(
            "SQUARESPACE_SYNC_LOG",
            "/opt/pokemon-momentum/output/squarespace_price_sync_log.jsonl",
        ),
        help="Append-only JSONL log",
    )
    parser.add_argument(
        "--mapping-csv",
        default=os.getenv(
            "SQUARESPACE_MAPPING_CSV",
            "/opt/pokemon-momentum/output/squarespace_product_mapping.csv",
        ),
        help="Write SKU -> product/variant mapping",
    )
    parser.add_argument("--base-url", default="https://api.squarespace.com")
    args = parser.parse_args()

    if not args.market_csv:
        print("Missing --market-csv or SQUARESPACE_MARKET_CSV.", file=sys.stderr)
        return 2

    api_key = os.getenv("SQUARESPACE_API_KEY")
    api_version = os.getenv("SQUARESPACE_PRODUCTS_API_VERSION", "v2")
    user_agent = os.getenv("SQUARESPACE_USER_AGENT", "pokemon-momentum/price-sync")
    effective_markup_pct = args.markup_pct - args.discount_pct

    if not api_key and not args.dry_run:
        print("Missing SQUARESPACE_API_KEY env var.", file=sys.stderr)
        return 2

    export_map = load_squarespace_export(args.squarespace_export)
    Path(args.mapping_csv).parent.mkdir(parents=True, exist_ok=True)
    write_mapping_csv(args.mapping_csv, export_map.values())

    market_prices = load_market_prices(args.market_csv)

    updates: List[Tuple[VariantMapping, Decimal]] = []
    for sku, market_row in market_prices.items():
        mapping = export_map.get(sku)
        if not mapping:
            continue
        if market_row.target_price is not None:
            new_price = market_row.target_price
        else:
            new_price = compute_new_price(
                market_price=market_row.market_price,
                markup_pct=effective_markup_pct,
                min_price=args.min_price,
                max_price=args.max_price,
                round_cents=args.round_cents,
                price_ending=args.price_ending,
            )
        if should_update(mapping.current_price, new_price, args.min_abs_change, args.min_pct_change):
            updates.append((mapping, new_price))

    print(f"Matched SKUs: {len([s for s in market_prices if s in export_map])}")
    print(f"Proposed updates: {len(updates)}")
    print(f"Effective markup pct: {effective_markup_pct}")

    if args.dry_run:
        for mapping, new_price in updates:
            print(
                f"DRY RUN: {mapping.sku} | {mapping.title} | {mapping.current_price} -> {new_price}"
            )
        return 0

    Path(args.log_jsonl).parent.mkdir(parents=True, exist_ok=True)
    with open(args.log_jsonl, "a", newline="") as logf:
        for mapping, new_price in updates:
            payload = build_update_payload(new_price, args.currency, args.disable_sale)
            try:
                resp = post_update(
                    base_url=args.base_url,
                    api_version=api_version,
                    api_key=api_key,
                    user_agent=user_agent,
                    product_id=mapping.product_id,
                    variant_id=mapping.variant_id,
                    payload=payload,
                    timeout=args.timeout,
                )
                ok = resp.status_code // 100 == 2
                record = {
                    "sku": mapping.sku,
                    "title": mapping.title,
                    "product_id": mapping.product_id,
                    "variant_id": mapping.variant_id,
                    "old_price": str(mapping.current_price),
                    "new_price": str(new_price),
                    "status_code": resp.status_code,
                    "response": resp.text[:500],
                    "ok": ok,
                }
                logf.write(json.dumps(record) + "\n")
                if not ok:
                    print(f"FAILED {mapping.sku}: {resp.status_code}")
                else:
                    print(f"UPDATED {mapping.sku}: {mapping.current_price} -> {new_price}")
            except requests.RequestException as exc:
                record = {
                    "sku": mapping.sku,
                    "title": mapping.title,
                    "product_id": mapping.product_id,
                    "variant_id": mapping.variant_id,
                    "old_price": str(mapping.current_price),
                    "new_price": str(new_price),
                    "error": str(exc),
                    "ok": False,
                }
                logf.write(json.dumps(record) + "\n")
                print(f"ERROR {mapping.sku}: {exc}")
            time.sleep(args.sleep_seconds)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
