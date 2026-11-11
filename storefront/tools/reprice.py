#!/usr/bin/env python3
"""Set store prices at or near today's market, never below break-even (owner, 2026-11-11).

For each visible product that has a price rule and a market price:

    target = the product's rule in data/store_price_rules.csv, applied to today's market price
             ("market": the market price; "market_minus_5_pct_99": 5% under, ending in .99)
    floor  = the lowest price that doesn't lose money on a poke6s.com sale (see break_even)
    new    = the higher of the two

Products without a rule ("manual"), a market price or a cost are left alone and listed, so a
missing cost can never lead to a loss-making price.

The market price is the same one the product page shows: the average over the product's
TCGplayer IDs in data/storefront/market_reference.json (storefront/app/market.py, summary()),
which the 03:00 job refreshes.

Costs are private and never in this repository. They come from a CSV given with --costs
(the owner keeps it in the Poke6s books folder):

    sku,unit_cost,ship_label,packing,source_item,note

With --hide-above-market, a product whose break-even is above market is hidden instead of
listed above market (owner, 2026-11-11). It still gets its break-even price. Hidden products
that could sell at or under market again are listed; show them with --show SKU --apply.

Dry run by default: prints the table and changes nothing. --apply writes everything in one
transaction. Standard library only, so it runs on the host:

    python3 storefront/tools/reprice.py --costs /path/to/store_sku_costs.csv --hide-above-market
    python3 storefront/tools/reprice.py --costs /path/to/store_sku_costs.csv --hide-above-market --apply

The output shows prices only, never costs, so it is safe in a log.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Stripe, US cards: 2.9% + 30 cents a charge, plus 0.5% for Stripe Tax (tax_mode "stripe").
CARD_RATE = 0.029
TAX_RATE = 0.005
CARD_FIXED = 0.30


def money(value: float) -> str:
    return f"${value:,.2f}"


def ending_99_down(value: float) -> float:
    """Highest price ending in .99 at or below value (12.40 -> 11.99, 12.99 -> 12.99)."""
    candidate = math.floor(value) + 0.99
    if candidate > value + 1e-9:
        candidate -= 1
    return round(candidate, 2)


def ending_99_up(value: float) -> float:
    """Lowest price ending in .99 at or above value (12.40 -> 12.99)."""
    candidate = math.floor(value) + 0.99
    if candidate < value - 1e-9:
        candidate += 1
    return round(candidate, 2)


def rule_target(market: float, mode: str) -> float | None:
    """Same modes as scripts/build_store_price_targets.py, compute_target_price()."""
    if mode == "market":
        return round(market, 2)
    if mode in {"market_minus_5_pct_99", "market_minus_5_pct_99_with_floor"}:
        return ending_99_down(market * 0.95)
    return None  # "manual" or unknown: leave the price alone


def break_even(cost: dict, free_shipping_over: float) -> tuple[float, float]:
    """(floor when the buyer pays shipping, floor when shipping is free).

    Under the free-shipping threshold the buyer pays a flat shipping charge ($10 or $20) that
    covers the label and box, so the item only has to cover its cost and Stripe's fees. At or
    over the threshold shipping is free, so the item also has to cover the label and packing.
    """
    keep = 1 - CARD_RATE - TAX_RATE
    paid_shipping = (cost["unit_cost"] + CARD_FIXED) / keep
    free_shipping = (cost["unit_cost"] + cost["ship_label"] + cost["packing"] + CARD_FIXED) / keep
    return paid_shipping, free_shipping


def new_price(target: float, cost: dict, free_shipping_over: float) -> tuple[float, bool]:
    """(price, raised_to_floor). Floors are rounded up to a .99 ending."""
    paid, free = break_even(cost, free_shipping_over)
    price, raised = target, False
    if price < paid:
        price, raised = ending_99_up(paid), True
    if price >= free_shipping_over and price < free:
        price, raised = ending_99_up(free), True
    return price, raised


def load_costs(path: Path) -> dict[str, dict]:
    costs = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            sku = (row.get("sku") or "").strip()
            if not sku or not (row.get("unit_cost") or "").strip():
                continue
            costs[sku] = {
                "unit_cost": float(row["unit_cost"]),
                "ship_label": float(row.get("ship_label") or 0),
                "packing": float(row.get("packing") or 0),
            }
    return costs


def market_price(reference: dict, tcgplayer_ids: str) -> float | None:
    products = reference.get("products") or {}
    prices = [float(products[i]["price"]) for i in (tcgplayer_ids or "").split("|")
              if i.strip() in products and products[i.strip()].get("price")]
    return round(sum(prices) / len(prices), 2) if prices else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--costs", type=Path, required=True, help="private sku,unit_cost,... CSV")
    parser.add_argument("--db", type=Path, default=ROOT / "data/storefront/store.db")
    parser.add_argument("--market", type=Path, default=ROOT / "data/storefront/market_reference.json")
    parser.add_argument("--rules", type=Path, default=ROOT / "data/store_price_rules.csv")
    parser.add_argument("--free-shipping-over", type=float, default=150.0)
    parser.add_argument("--hide-above-market", action="store_true",
                        help="hide products whose break-even is above market instead of listing "
                             "them above market (owner, 2026-11-11)")
    parser.add_argument("--show", nargs="+", default=[], metavar="SKU",
                        help="show hidden products again (with --apply)")
    parser.add_argument("--apply", action="store_true", help="write the changes")
    args = parser.parse_args()

    costs = load_costs(args.costs)
    reference = json.loads(args.market.read_text(encoding="utf-8"))
    with args.rules.open(encoding="utf-8", newline="") as handle:
        rules = {r["sku"].strip(): r["pricing_mode"].strip() for r in csv.DictReader(handle)}

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT id, sku, title, price_cents, tcgplayer_product_ids, visible, stock, track_stock "
        "FROM products ORDER BY title").fetchall()

    print(f"Market prices from {reference.get('latest_date')}. "
          f"{'APPLYING' if args.apply else 'Dry run: nothing changes'}.\n")
    print(f"{'SKU':20} {'now':>9} {'market':>9} {'rule':>9} {'new':>9} {'vs mkt':>7}  note")
    changes, hide, skipped, could_show = [], [], [], []
    for row in rows:
        sku, now, visible = row["sku"], row["price_cents"] / 100, bool(row["visible"])
        mode = rules.get(sku, "")
        market = market_price(reference, row["tcgplayer_product_ids"])
        target = rule_target(market, mode) if market is not None else None
        reason = ("no price rule" if not mode else "manual price" if target is None and market
                  else "no market price" if market is None else "no cost on file"
                  if sku not in costs else "")
        if reason:
            if visible:
                skipped.append((sku, row["title"], now, reason))
            continue
        price, raised = new_price(target, costs[sku], args.free_shipping_over)
        if not visible:
            # Hidden products are only reported: worth showing again once they can sell at or
            # under market without a loss (and are in stock).
            if not raised and (row["stock"] > 0 or not row["track_stock"]):
                could_show.append((sku, row["title"], price, market))
            continue
        note = "raised to break-even" if raised else ""
        if raised and args.hide_above_market:
            hide.append((row["id"], sku))
            note = "HIDE: break-even is above market"
        vs = f"{(price / market - 1) * 100:+.0f}%"
        print(f"{sku[:20]:20} {money(now):>9} {money(market):>9} {money(target):>9} "
              f"{money(price):>9} {vs:>7}  {note}")
        # A hidden product still gets its break-even price, so showing it again can't lose money.
        if round(price * 100) != row["price_cents"]:
            changes.append((row["id"], sku, round(price * 100)))

    print(f"\n{len(changes)} price(s) change; {len(hide)} product(s) to hide.")
    if skipped:
        print("\nLeft alone:")
        for sku, title, now, reason in skipped:
            print(f"  {sku[:20]:20} {money(now):>9}  {reason}: {title[:50]}")
    if could_show:
        print("\nHidden, but could sell at or under market now (show with --show SKU --apply):")
        for sku, title, price, market in could_show:
            print(f"  {sku[:20]:20} {money(price):>9} (market {money(market)})  {title[:45]}")

    known = {row["sku"] for row in rows}
    unknown = [sku for sku in args.show if sku not in known]
    if unknown:
        print(f"\nUnknown SKU(s) for --show: {', '.join(unknown)}")
        return 1
    if args.apply:
        with con:
            for product_id, _, cents in changes:
                con.execute("UPDATE products SET price_cents = ?, compare_at_cents = NULL, "
                            "updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?",
                            (cents, product_id))
            for product_id, _ in hide:
                con.execute("UPDATE products SET visible = 0, "
                            "updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?",
                            (product_id,))
            for sku in args.show:
                con.execute("UPDATE products SET visible = 1, "
                            "updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE sku = ?",
                            (sku,))
        print(f"Applied: {len(changes)} price(s), {len(hide)} hidden, {len(args.show)} shown.")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
