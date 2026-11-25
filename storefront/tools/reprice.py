#!/usr/bin/env python3
"""Set store prices at or near today's market, never below break-even (owner, 2026-11-11).

For each visible product that has a price rule and a market price:

    target = the product's rule in data/store_price_rules.csv, applied to today's market price
             ("market": the market price; "market_minus_5_pct_99": 5% under, ending in .99)
    floor  = the lowest price that doesn't lose money on a poke6s.com sale (see break_even), or
             the product's own minimum (min_price in the rules file), whichever is higher
    new    = the higher of target and floor

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

Run unattended every night (owner, 2026-11-25: "fine with automatic repricing, so long as it
doesn't dip below my profitability amount"): scripts/pipeline/run_store_reprice_host.sh, after
the 03:00 market reference and the 03:15 backup. With --max-market-age-days the run changes
nothing when the market prices are older than that, so a failed refresh can't reprice on old data.

Dry run by default: prints the table and changes nothing. --apply writes everything in one
transaction and logs each change in product_changes (the admin's product history, marked
"repricing"). Standard library only, so it runs on the host:

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
from datetime import date
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


def new_price(target: float, cost: dict, free_shipping_over: float,
              min_price: float | None = None) -> tuple[float, bool]:
    """(price, raised_to_floor). Break-even floors are rounded up to a .99 ending; the owner's
    min_price is used as written."""
    paid, free = break_even(cost, free_shipping_over)
    price, raised = target, False
    if min_price is not None and price < min_price:
        price, raised = min_price, True
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


def load_rules(path: Path) -> dict[str, str]:
    """sku -> pricing mode from data/store_price_rules.csv."""
    with path.open(encoding="utf-8", newline="") as handle:
        return {r["sku"].strip(): r["pricing_mode"].strip() for r in csv.DictReader(handle)}


def load_min_prices(path: Path) -> dict[str, float]:
    """sku -> the owner's minimum price (the rules file's min_price column, in dollars), for the
    products that have one. Repricing never goes under it, whatever the market does."""
    floors = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            raw = (row.get("min_price") or "").strip().lstrip("$")
            if raw:
                floors[row["sku"].strip()] = float(raw)   # a typo fails loudly rather than being skipped
    return floors


def load_products(con: sqlite3.Connection) -> list[dict]:
    return [dict(r) for r in con.execute(
        "SELECT id, sku, title, price_cents, compare_at_cents, tcgplayer_product_ids, visible, stock, track_stock "
        "FROM products ORDER BY title")]


def plan(products: list[dict], costs: dict, reference: dict, rules: dict, *,
         free_shipping_over: float = 150.0, hide_above_market: bool = False,
         min_prices: dict | None = None) -> dict:
    """Work out every product's new price without changing anything.

    Returns lists of dicts (prices in cents):
      priced      visible products the rules price: id, sku, title, now, market, target, new,
                  raised (lifted to break-even), hide (with hide_above_market), change (new != now)
      skipped     visible products left alone, with the reason (no rule, manual, no market price,
                  no cost on file)
      could_show  hidden, in-stock products that could sell at or under market without a loss
    Shared by this script and the admin's Pricing page (storefront/admin), so both agree.
    """
    priced, skipped, could_show = [], [], []
    for row in products:
        sku, visible = row["sku"], bool(row["visible"])
        mode = rules.get(sku, "")
        market = market_price(reference, row["tcgplayer_product_ids"])
        target = rule_target(market, mode) if market is not None else None
        reason = ("no price rule" if not mode else "manual price" if target is None and market
                  else "no market price" if market is None else "no cost on file"
                  if sku not in costs else "")
        base = {"id": row["id"], "sku": sku, "title": row["title"], "now": row["price_cents"], "mode": mode}
        if reason:
            if visible:
                skipped.append({**base, "reason": reason})
            continue
        price, raised = new_price(target, costs[sku], free_shipping_over, (min_prices or {}).get(sku))
        new = round(price * 100)
        if not visible:
            # Hidden products are only reported: worth showing again once they can sell at or
            # under market without a loss (and are in stock).
            if not raised and (row["stock"] > 0 or not row["track_stock"]):
                could_show.append({**base, "new": new, "market": round(market * 100)})
            continue
        # A product whose break-even is above market still gets its break-even price, so
        # showing it again later can't lose money.
        priced.append({**base, "market": round(market * 100), "target": round(target * 100), "new": new,
                       "raised": raised, "hide": raised and hide_above_market, "change": new != row["price_cents"],
                       "vs_market": round((price / market - 1) * 100)})
    return {"priced": priced, "skipped": skipped, "could_show": could_show,
            "market_date": reference.get("latest_date")}


def apply(con: sqlite3.Connection, changes: list[dict], *, hide: list[dict] = (), show: list[dict] = (),
          market_date: str | None = None) -> int:
    """Write planned prices (and hides/shows) in one transaction, each logged in product_changes
    as "repricing". `changes` are plan()["priced"] entries. A product whose price moved since the
    plan was made (someone changed it in the admin) is skipped. Returns how many prices changed."""
    # Logged in product_changes (migration 005) so the admin's product history shows them as
    # "repricing". An older database without the table just isn't logged.
    logged = con.execute("SELECT 1 FROM sqlite_master WHERE name = 'product_changes'").fetchone()
    rule_note = f"market rule, market prices from {market_date}"
    stamp = "strftime('%Y-%m-%dT%H:%M:%SZ','now')"

    def log_change(product_id, field, old, new, reason=rule_note):
        if logged and old != new:
            con.execute("INSERT INTO product_changes (product_id, field, old_value, new_value, reason, source, at) "
                        f"VALUES (?, ?, ?, ?, ?, 'reprice', {stamp})",
                        (product_id, field, None if old is None else str(old), None if new is None else str(new),
                         reason))

    changed = 0
    con.execute("BEGIN IMMEDIATE")
    try:
        for item in changes:
            row = con.execute("SELECT price_cents, compare_at_cents FROM products WHERE id = ?", (item["id"],)).fetchone()
            if row is None or row[0] != item["now"]:
                continue
            log_change(item["id"], "price", item["now"], item["new"])
            log_change(item["id"], "compare_at", row[1], None)
            con.execute(f"UPDATE products SET price_cents = ?, compare_at_cents = NULL, updated_at = {stamp} "
                        "WHERE id = ?", (item["new"], item["id"]))
            changed += 1
        for item in hide:
            log_change(item["id"], "visible", 1, 0, "break-even is above market")
            con.execute(f"UPDATE products SET visible = 0, updated_at = {stamp} WHERE id = ?", (item["id"],))
        for item in show:
            log_change(item["id"], "visible", 0, 1, "shown with --show")
            con.execute(f"UPDATE products SET visible = 1, updated_at = {stamp} WHERE id = ?", (item["id"],))
        con.commit()
    except BaseException:
        con.rollback()
        raise
    return changed


def market_age_days(latest_date: str | None, today: date | None = None) -> int | None:
    """Days since the market reference's latest_date (YYYY-MM-DD), or None if it has none."""
    try:
        return ((today or date.today()) - date.fromisoformat(str(latest_date)[:10])).days
    except ValueError:
        return None


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
    parser.add_argument("--max-market-age-days", type=int, default=None,
                        help="change nothing if the market prices are older than this (the nightly run uses 2)")
    args = parser.parse_args()

    reference = json.loads(args.market.read_text(encoding="utf-8"))
    if args.max_market_age_days is not None:
        age = market_age_days(reference.get("latest_date"))
        if age is None or age > args.max_market_age_days:
            print(f"Market prices are from {reference.get('latest_date') or 'an unknown date'}, older than "
                  f"{args.max_market_age_days} day(s): nothing changed.")
            return 2
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    products = load_products(con)
    result = plan(products, load_costs(args.costs), reference, load_rules(args.rules),
                  free_shipping_over=args.free_shipping_over, hide_above_market=args.hide_above_market,
                  min_prices=load_min_prices(args.rules))

    print(f"Market prices from {result['market_date']}. "
          f"{'APPLYING' if args.apply else 'Dry run: nothing changes'}.\n")
    print(f"{'SKU':20} {'now':>9} {'market':>9} {'rule':>9} {'new':>9} {'vs mkt':>7}  note")
    for item in result["priced"]:
        note = "HIDE: break-even is above market" if item["hide"] else "raised to floor" if item["raised"] else ""
        print(f"{item['sku'][:20]:20} {money(item['now'] / 100):>9} {money(item['market'] / 100):>9} "
              f"{money(item['target'] / 100):>9} {money(item['new'] / 100):>9} {item['vs_market']:>+6d}%  {note}")
    changes = [item for item in result["priced"] if item["change"]]
    hide = [item for item in result["priced"] if item["hide"]]
    print(f"\n{len(changes)} price(s) change; {len(hide)} product(s) to hide.")
    if result["skipped"]:
        print("\nLeft alone:")
        for item in result["skipped"]:
            print(f"  {item['sku'][:20]:20} {money(item['now'] / 100):>9}  {item['reason']}: {item['title'][:50]}")
    if result["could_show"]:
        print("\nHidden, but could sell at or under market now (show with --show SKU --apply):")
        for item in result["could_show"]:
            print(f"  {item['sku'][:20]:20} {money(item['new'] / 100):>9} (market {money(item['market'] / 100)})  "
                  f"{item['title'][:45]}")

    by_sku = {row["sku"]: row for row in products}
    unknown = [sku for sku in args.show if sku not in by_sku]
    if unknown:
        print(f"\nUnknown SKU(s) for --show: {', '.join(unknown)}")
        return 1
    if args.apply:
        con.isolation_level = None   # apply() runs its own BEGIN ... COMMIT
        changed = apply(con, changes, hide=hide, show=[by_sku[sku] for sku in args.show if not by_sku[sku]["visible"]],
                        market_date=result["market_date"])
        print(f"Applied: {changed} price(s), {len(hide)} hidden, {len(args.show)} shown.")
        # One line per change for the nightly job's Discord summary (notify_discord.py, store-reprice).
        for item in changes:
            note = " (at its floor)" if item["raised"] else ""
            print(f"Changed: {item['sku']} {money(item['now'] / 100)} -> {money(item['new'] / 100)}{note}")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
