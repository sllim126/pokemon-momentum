"""The admin's Best buys page (`/supplier-best-buys`, templates/supplier_best_buys.html).

The owner's question (2026-11-25): "an accurate profit for each system, and where I should source
it from", ranked for turnover ("A standard order would be 2-4 boxes of each one that is selling
quickly. Time is money, and I'm trying to grow, not just hold it").

For each box it works out every route: two ways to buy it and three places to sell it.
- Buy: import from the newest supplier quote (the landed cost from supplier_profit.landed_cost,
  so the same assumptions as Supplier Profitability), or buy it on TCGplayer at the market price
  plus sales tax (Utah, 7.45%) and any shipping. "If it's cheaper to buy from TCGplayer, then I
  should do that, and save on tariffs" (owner, 2026-11-25).
- Sell: the own site at the store price (the buyer pays the store's flat shipping, nothing over
  the free-shipping line), eBay at the target price, TCGplayer at the market price; the fees and
  income tax are supplier_profit.channel_result's.
The best route is the one with the most profit after tax; "best elsewhere" is the best one
that sells on eBay or TCGplayer instead, which turn stock over faster than the own site. How fast a box sells comes from the
owner: TCGplayer's product page shows sales per day and how many are listed, which the owner
types in on this page (a small ledger, data/storefront/tcgplayer_sales.csv, newest row per SKU
counts). TCGCSV's prices carry no sales counts, so there's no other source.

The suggested order follows the owner's rule (2-4 of each fast seller), smaller when the market
is falling, and never a box that loses money on every route ("never sell at a loss").
Everything is per box. Nothing on the page changes the store.
"""

from __future__ import annotations

import csv
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from . import supplier_profit
from .supplier_profit import Assumptions, money, number

# Buying on TCGplayer: Utah sales tax on the order (no resale certificate assumed) and shipping
# per box (many sellers ship free over a threshold, so 0 by default). Both typed on the page.
BUY_DEFAULTS = {"tcg_buy_tax_pct": 7.45, "tcg_buy_shipping_usd": 0.0}

SALES_FIELDS = ["entered_at", "sku", "sold_per_day", "listed", "trend_3m_pct"]

# Sold per day on TCGplayer -> speed, and the owner's standard order for it (2-4 of fast sellers).
FAST_PER_DAY = 4.0
STEADY_PER_DAY = 2.0
ORDER_BY_SPEED = {"Fast": 4, "Steady": 2, "Slow": 1}
# A market down this much (30-day market move, or the 3-month trend the owner typed) halves the order.
FALLING_PCT = -15.0

SORTS = {"speed": "Fastest sellers first", "profit": "Profit $ (best route)", "roi": "Return on cost %",
         "saving": "Import saving $", "name": "Name"}
SOURCES = {"import": "Import", "tcgplayer": "TCGplayer"}


@dataclass
class BuyAssumptions:
    tcg_buy_tax_pct: float
    tcg_buy_shipping_usd: float


def read_buy_assumptions(query: Mapping[str, str]) -> BuyAssumptions:
    return BuyAssumptions(**{name: number(query.get(name), default) for name, default in BUY_DEFAULTS.items()})


# --- the owner's TCGplayer sales numbers ----------------------------------------------------------

def latest_sales(path: Path) -> dict[str, dict]:
    """The newest sales row per SKU (rows are appended, never edited). A missing file is {}."""
    if not path.is_file():
        return {}
    newest: dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            sku = str(row.get("sku") or "").strip().upper()
            if sku and (sku not in newest or str(row.get("entered_at") or "") >= newest[sku]["entered_at"]):
                newest[sku] = {
                    "entered_at": str(row.get("entered_at") or ""),
                    "sold_per_day": number(row.get("sold_per_day"), None),
                    "listed": number(row.get("listed"), None),
                    "trend_3m_pct": number(row.get("trend_3m_pct"), None),
                }
    return newest


def add_sales(path: Path, sku: str, sold_per_day: str, listed: str, trend_3m_pct: str, known_skus: set[str],
              now: datetime | None = None) -> None:
    """Append one SKU's numbers from TCGplayer's page. Raises ValueError on bad input."""
    sku = sku.strip().upper()
    if sku not in known_skus:
        raise ValueError("Pick a product from the list.")
    sold = number(sold_per_day, None)
    if sold is None or sold < 0:
        raise ValueError("Sold per day is a number, 0 or more (TCGplayer's “sold per day” or total sold ÷ days).")
    count = number(listed, None)
    if listed.strip() and (count is None or count < 0):
        raise ValueError("Listed is a whole number, or leave it empty.")
    trend = number(trend_3m_pct, None)
    if trend_3m_pct.strip() and trend is None:
        raise ValueError("The 3-month trend is a percentage like -24, or leave it empty.")
    new_file = not path.is_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SALES_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow({"entered_at": (now or datetime.now(UTC)).strftime("%Y-%m-%dT%H:%M:%SZ"), "sku": sku,
                         "sold_per_day": f"{sold:g}", "listed": "" if count is None else f"{count:g}",
                         "trend_3m_pct": "" if trend is None else f"{trend:g}"})


# --- the routes ------------------------------------------------------------------------------------

def store_products(conn) -> dict[str, dict]:
    """Sealed store products by SKU: title, price in dollars, stock, shipping class, TCGplayer IDs."""
    return {r["sku"].upper(): {"title": r["title"], "price": r["price_cents"] / 100 if r["price_cents"] else None,
                               "stock": r["stock"], "ship_class": r["ship_class"],
                               "tcgplayer_ids": r["tcgplayer_product_ids"] or ""}
            for r in conn.execute("SELECT sku, title, price_cents, stock, ship_class, tcgplayer_product_ids "
                                  "FROM products WHERE kind = 'sealed'")}


def buyer_shipping(price: float | None, ship_class: str, ship_rates_cents: dict, free_over_cents: int) -> float:
    """What a buyer pays the own site to ship one box: the class's flat rate, nothing over the
    free-shipping line (storefront/app/orders.py does the same at checkout)."""
    if price is None:
        return 0.0
    if free_over_cents and price * 100 >= free_over_cents:
        return 0.0
    cents = ship_rates_cents.get(ship_class, max(ship_rates_cents.values()) if ship_rates_cents else 0)
    return cents / 100


def short_title(title: str) -> str:
    """The title without the words every row shares ("Pokemon", "Booster Box", "Japanese")."""
    short = re.sub(r"^Pok[eé]mon\s+", "", title.strip(), flags=re.I)
    short = re.sub(r"\s*-?\s*Japanese$", "", short, flags=re.I)
    short = re.sub(r"\s+Booster Box$", "", short, flags=re.I)
    return short or title


def speed(sold_per_day: float | None) -> str:
    if sold_per_day is None:
        return ""
    if sold_per_day >= FAST_PER_DAY:
        return "Fast"
    return "Steady" if sold_per_day >= STEADY_PER_DAY else "Slow"


def suggested_order(speed_label: str, best_profit: float | None, falling: bool, stock: int) -> tuple[int, str]:
    """(boxes to order, why). The owner's standard order is 2-4 of each fast seller; a falling
    market halves it; a box that loses money on every route is never suggested."""
    if best_profit is None:
        return 0, "No price to judge"
    if best_profit <= 0:
        return 0, "Loses money on every route"
    if not speed_label:
        return 0, "Add its TCGplayer sales"
    base = ORDER_BY_SPEED[speed_label]
    why = f"{speed_label} seller"
    if falling:
        base = max(1, base // 2)
        why += ", market falling"
    qty = max(0, base - max(0, stock))
    if qty < base:
        why += f", {stock} in stock"
    return qty, why


def build(quotes: list[dict], market_rows: dict[str, dict], store: dict[str, dict], sales: dict[str, dict],
          market_summary, a: Assumptions, buy: BuyAssumptions, ship_rates_cents: dict,
          free_over_cents: int) -> list[dict]:
    """One row per box that has a supplier quote or sales numbers (and a price to judge by)."""
    quote_by_sku = {str(q.get("sku") or "").upper(): q for q in quotes}
    rows = []
    for sku in sorted(set(quote_by_sku) | set(sales)):
        product, market_row, quote, sold = store.get(sku), market_rows.get(sku, {}), quote_by_sku.get(sku), sales.get(sku)
        if product is None and not market_row:
            continue
        market_price = number(market_row.get("market_price"), None)
        summary = market_summary(product["tcgplayer_ids"]) if product and product["tcgplayer_ids"] else None
        if summary is not None:
            market_price = summary.price   # today's market, fresher than the nightly targets file
        target_price = number(market_row.get("target_price"), None)
        site_price = product["price"] if product else None

        costs = {}
        cost_jpy = number(quote.get("cost_jpy"), None) if quote else None
        if cost_jpy and cost_jpy > 0:
            costs["import"] = supplier_profit.landed_cost(cost_jpy / a.jpy_per_usd, a)
        if market_price:
            costs["tcgplayer"] = market_price * (1 + buy.tcg_buy_tax_pct / 100) + buy.tcg_buy_shipping_usd
        if not costs:
            continue

        sells = {"site": (site_price, buyer_shipping(site_price, product["ship_class"] if product else "",
                                                     ship_rates_cents, free_over_cents)),
                 "ebay": (target_price, 0.0), "tcgplayer": (market_price, 0.0)}
        routes = []
        for source, cost in costs.items():
            for key, (price, credit) in sells.items():
                if not price:
                    continue
                fixed = cost + a.outbound_shipping_usd - credit
                result = supplier_profit.channel_result(a.channels[key], price, fixed, a)
                routes.append({"source": source, "source_name": SOURCES[source], "channel": key,
                               "channel_name": a.channels[key].name, "price": money(price), "cost": money(cost),
                               "buyer_shipping": money(credit), "profit": result["profit_after_tax"],
                               "margin_pct": result["margin_after_tax_pct"],
                               "roi_pct": (round(result["profit_after_tax"] / cost * 100, 1)
                                           if result["profit_after_tax"] is not None and cost > 0 else None)})
        routes.sort(key=lambda r: -(r["profit"] if r["profit"] is not None else -10_000.0))
        best = routes[0] if routes else None

        sold_per_day = sold["sold_per_day"] if sold else None
        listed = sold["listed"] if sold else None
        trend_3m = sold["trend_3m_pct"] if sold else None
        roc_30d = summary.roc_30d_pct if summary else None
        falling = any(t is not None and t <= FALLING_PCT for t in (roc_30d, trend_3m))
        label = speed(sold_per_day)
        stock = product["stock"] if product else 0
        qty, why = suggested_order(label, best["profit"] if best else None, falling, stock)
        # Which source to buy from, judged on the best channel (the cheaper one to land, when the
        # best route is the same either way).
        saving = (money(costs["tcgplayer"] - costs["import"]) if {"import", "tcgplayer"} <= set(costs) else None)
        rows.append({
            "sku": sku,
            "title": (product or {}).get("title") or market_row.get("title") or sku,
            "short_title": short_title((product or {}).get("title") or market_row.get("title") or sku),
            "supplier": str((quote or {}).get("supplier_name") or ""),
            "quote_date": str((quote or {}).get("quote_date") or ""),
            "import_cost": money(costs.get("import")),
            "tcg_cost": money(costs.get("tcgplayer")),
            "saving": saving,
            "market_price": money(market_price),
            "site_price": money(site_price),
            "routes": routes,
            "best": best,
            # The best route that doesn't sell on the own site: the site's price is usually the
            # highest but it sells slowest, so this says whether a quick marketplace sale still pays.
            "elsewhere": next((r for r in routes if r["channel"] != "site"), None),
            "sold_per_day": sold_per_day,
            "listed": listed,
            "days_of_supply": (round(listed / sold_per_day) if listed is not None and sold_per_day else None),
            "sales_date": (sold or {}).get("entered_at", "")[:10],
            "speed": label,
            "roc_30d": roc_30d,
            "trend_3m": trend_3m,
            "falling": falling,
            "stock": stock,
            "order_qty": qty,
            "order_why": why,
        })
    return rows


def sort_rows(rows: list[dict], sort: str) -> list[dict]:
    """Fastest sellers first by default (unknown speed last), then the most profit."""
    def profit(row):
        return row["best"]["profit"] if row["best"] and row["best"]["profit"] is not None else -10_000.0
    if sort == "name":
        return sorted(rows, key=lambda r: r["title"].lower())
    if sort == "profit":
        return sorted(rows, key=lambda r: -profit(r))
    if sort == "roi":
        return sorted(rows, key=lambda r: -(r["best"]["roi_pct"] if r["best"] and r["best"]["roi_pct"] is not None
                                            else -10_000.0))
    if sort == "saving":
        return sorted(rows, key=lambda r: -(r["saving"] if r["saving"] is not None else -10_000.0))
    return sorted(rows, key=lambda r: (-(r["sold_per_day"] if r["sold_per_day"] is not None else -1.0), -profit(r)))


def order_plan(rows: list[dict]) -> dict:
    """The suggested order added up by source, at each box's best route's buying cost."""
    plan = {key: {"name": name, "boxes": 0, "cost": 0.0, "profit": 0.0} for key, name in SOURCES.items()}
    for row in rows:
        if row["order_qty"] and row["best"]:
            part = plan[row["best"]["source"]]
            part["boxes"] += row["order_qty"]
            part["cost"] += row["order_qty"] * row["best"]["cost"]
            part["profit"] += row["order_qty"] * row["best"]["profit"]
    for part in plan.values():
        part["cost"], part["profit"] = money(part["cost"]), money(part["profit"])
    return plan
