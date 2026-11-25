"""The admin's Supplier Profitability page (`/supplier-profitability`, templates/supplier_profitability.html).

Moved from market.poke6s.com on 2026-11-21 (docs/market_inventory.md, option B, the owner
tools); the market address now forwards here. The arithmetic is the market page's, unchanged:
for each SKU's newest supplier quote (Japanese boxes, priced in yen), what the box costs landed,
what it must sell for to break even and to make the target margin on each channel (own site,
eBay, TCGplayer), and whether today's reference price clears that ("Buy", "Thin margin",
"Pass").

Inputs, all read fresh on every page load, never written:
- data/storefront/supplier_quotes.csv: the quote ledger that Supplier Pricing (supplier_quotes.py,
  "Enter a quote") appends to. The newest row per SKU counts; rows with no SKU are counted as "unmatched".
- data/market_prices_latest.csv: each SKU's market and target price (scripts/build_store_price_targets.py).
- the store's own products table: the poke6s.com price, for the "Store Price" reference.
- the JPY-per-USD rate from Frankfurter (free, no key; the market page used the same), cached for
  an hour. If it can't be reached the page says so and uses DEFAULT_JPY_PER_USD or the typed rate.

Everything the owner can change (exchange rate, duty, shipping, fees, tax, target margin) comes
from the page's form as query parameters, so a page can be bookmarked with its assumptions.
Unlike the market page, a field left at 0 stays 0 (the market treated 0 as "use the default"
for some fields); a field left empty uses the default.

Every result is per unit (one box), not per order. It's an owner tool, so it's admin-only like the
Pricing page and never reaches the public store.
"""

from __future__ import annotations

import csv
import json
import logging
import threading
import time
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger("storefront.admin.supplier_profit")

DEFAULT_JPY_PER_USD = 159.0
FX_URL = "https://api.frankfurter.dev/v2/rates?base=USD&quotes=JPY"
FX_CACHE_SECONDS = 3600
FX_RETRY_SECONDS = 600

# The page's starting assumptions (the market page's defaults). Shipping and fees in USD unless
# the name says JPY; percentages as percent (10 = 10%).
DEFAULTS = {
    "import_duty_pct": 10.0,
    # "order-estimate": order_shipping_jpy / order_box_count per box; "manual": inbound_shipping_usd.
    "inbound_shipping_mode": "order-estimate",
    "order_shipping_jpy": 13800.0,
    "order_box_count": 11.0,
    "inbound_shipping_usd": 5.50,
    "handling_cost_usd": 0.75,
    # Charged once per supplier order, so each box carries its share (owner, 2026-10-27,
    # confirmed 2026-11-09). Without a box count, one box carries the whole fee.
    "disbursement_fee_usd": 15.00,
    "outbound_shipping_usd": 7.25,
    "shipping_credit_usd": 0.0,
    "income_tax_pct": 25.0,
    "target_margin_pct": 15.0,
}

# Channel key -> name, which saved price it is judged against by default, and its fees.
# reference: "store" (the poke6s.com price), "market", "target" (market_prices_latest.csv) or "none".
CHANNELS = {
    "site": {"name": "Own Site", "reference": "store", "platform_fee_pct": 0.0, "payment_fee_pct": 2.9,
             "payment_fee_fixed": 0.30},
    "ebay": {"name": "eBay", "reference": "target", "platform_fee_pct": 13.25, "payment_fee_pct": 0.0,
             "payment_fee_fixed": 0.30},
    "tcgplayer": {"name": "TCGplayer", "reference": "market", "platform_fee_pct": 10.25, "payment_fee_pct": 2.5,
                  "payment_fee_fixed": 0.30},
}
REFERENCES = {"store": "Store price", "market": "Market price", "target": "Target price", "none": "No reference"}
DECISIONS = ("Buy", "Thin margin", "Pass")
_DECISION_ORDER = {"Buy": 0, "Thin margin": 1, "Pass": 2, "No ref price": 3}


def number(value, default: float | None) -> float | None:
    """A form or CSV value as a float; `default` when empty or unreadable."""
    text = str(value if value is not None else "").strip().replace(",", "")
    if not text:
        return default
    try:
        number = float(text)
    except ValueError:
        return default
    return number if number == number and abs(number) != float("inf") else default


def money(value: float | None) -> float | None:
    """Dollars rounded to cents for display; None stays None."""
    return None if value is None else round(value + 1e-9, 2)


@dataclass
class Channel:
    key: str
    name: str
    reference: str
    platform_fee_pct: float
    payment_fee_pct: float
    payment_fee_fixed: float


@dataclass
class Assumptions:
    jpy_per_usd: float
    import_duty_pct: float
    inbound_shipping_mode: str
    order_shipping_jpy: float
    order_box_count: float
    inbound_shipping_usd: float
    handling_cost_usd: float
    disbursement_fee_usd: float
    outbound_shipping_usd: float
    shipping_credit_usd: float
    income_tax_pct: float
    target_margin_pct: float
    channels: dict[str, Channel] = field(default_factory=dict)

    @property
    def inbound_per_box_usd(self) -> float:
        """Inbound shipping for one box: the whole order's yen split over its boxes, or the typed
        per-box amount (manual mode, or when the order figures are missing)."""
        if (self.inbound_shipping_mode == "order-estimate" and self.order_shipping_jpy > 0
                and self.order_box_count > 0 and self.jpy_per_usd > 0):
            return self.order_shipping_jpy / self.order_box_count / self.jpy_per_usd
        return self.inbound_shipping_usd

    @property
    def disbursement_per_box_usd(self) -> float:
        if self.order_box_count > 0:
            return self.disbursement_fee_usd / self.order_box_count
        return self.disbursement_fee_usd


def read_assumptions(query: Mapping[str, str], live_rate: float | None) -> Assumptions:
    """The page's assumptions from its query string. Empty fields take DEFAULTS (and the live
    exchange rate, or DEFAULT_JPY_PER_USD without one)."""
    values = {name: number(query.get(name), default) for name, default in DEFAULTS.items()
              if name != "inbound_shipping_mode"}
    mode = str(query.get("inbound_shipping_mode") or DEFAULTS["inbound_shipping_mode"])
    rate = number(query.get("jpy_per_usd"), None) or live_rate or DEFAULT_JPY_PER_USD
    channels = {}
    for key, base in CHANNELS.items():
        reference = str(query.get(f"{key}_reference") or base["reference"])
        channels[key] = Channel(
            key=key, name=base["name"], reference=reference if reference in REFERENCES else base["reference"],
            platform_fee_pct=number(query.get(f"{key}_platform_fee_pct"), base["platform_fee_pct"]),
            payment_fee_pct=number(query.get(f"{key}_payment_fee_pct"), base["payment_fee_pct"]),
            payment_fee_fixed=number(query.get(f"{key}_payment_fee_fixed"), base["payment_fee_fixed"]),
        )
    return Assumptions(jpy_per_usd=rate if rate > 0 else DEFAULT_JPY_PER_USD,
                       inbound_shipping_mode=mode if mode in ("order-estimate", "manual") else "order-estimate",
                       channels=channels, **values)


def channel_result(channel: Channel, reference_price: float | None, fixed_costs: float,
                   a: Assumptions) -> dict:
    """Break-even and target-margin prices on one channel, and the verdict at its reference price.

    Fees are a share of the sale price plus a fixed fee per sale, so the price that leaves
    `fixed_costs` (landed cost plus outbound shipping) is fixed / (1 - fee share), and the one
    that also leaves the target margin is fixed / (1 - fee share - margin). Income tax only
    applies to a profit, never to a loss.
    """
    fee_rate = max(0.0, (channel.platform_fee_pct + channel.payment_fee_pct) / 100)
    tax_rate = max(0.0, a.income_tax_pct / 100)
    margin_rate = max(0.0, a.target_margin_pct / 100)
    costs = fixed_costs + channel.payment_fee_fixed
    break_even = costs / (1 - fee_rate) if 1 - fee_rate > 0 else None
    for_margin = costs / (1 - fee_rate - margin_rate) if 1 - fee_rate - margin_rate > 0 else None
    profit = margin = None
    if reference_price is None or reference_price <= 0:
        decision = "No ref price"
        reference_price = None
    else:
        before_tax = reference_price * (1 - fee_rate) - costs
        profit = before_tax if before_tax <= 0 else before_tax * (1 - tax_rate)
        margin = profit / reference_price * 100
        if for_margin is not None and reference_price >= for_margin:
            decision = "Buy"
        elif break_even is not None and reference_price >= break_even:
            decision = "Thin margin"
        else:
            decision = "Pass"
    headroom = reference_price - for_margin if reference_price is not None and for_margin is not None else None
    return {"name": channel.name, "reference": REFERENCES[channel.reference], "reference_price": money(reference_price),
            "break_even": money(break_even), "for_margin": money(for_margin), "profit_after_tax": money(profit),
            "margin_after_tax_pct": None if margin is None else round(margin, 1), "headroom": headroom,
            "decision": decision}


def _rank(result: dict) -> tuple:
    headroom = result["headroom"]
    return (_DECISION_ORDER.get(result["decision"], 9), -(headroom if headroom is not None else -10_000.0))


def landed_cost(cost_usd: float, a: Assumptions) -> float:
    """One box delivered to us: supplier price, import duty on it, inbound shipping, packaging
    and handling, and its share of the order's disbursement fee."""
    return (cost_usd + cost_usd * a.import_duty_pct / 100 + a.inbound_per_box_usd + a.handling_cost_usd
            + a.disbursement_per_box_usd)


def latest_quotes(path: Path) -> tuple[list[dict], int]:
    """(the newest quote row per SKU, how many rows have no SKU yet). Newest = latest quote_date,
    then quote_id. A missing file is no quotes."""
    newest: dict[str, dict] = {}
    unmatched = 0
    if not path.is_file():
        return [], 0
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            # Upper case: the old market page saved some SKUs in lower case ("jp-bp-box"), and
            # store SKUs are upper case, so "jp-bp-box" and "JP-BP-BOX" are one product.
            sku = str(row.get("sku") or "").strip().upper()
            if not sku:
                unmatched += 1
                continue
            key = (str(row.get("quote_date") or ""), str(row.get("quote_id") or ""),
                   str(row.get("item_name_raw") or "").lower())
            if sku not in newest or key >= newest[sku][0]:
                newest[sku] = (key, {**row, "sku": sku})
    return [row for _key, row in sorted(newest.values(), key=lambda kv: str(kv[1].get("sku")))], unmatched


def market_targets(path: Path) -> dict[str, dict]:
    """market_prices_latest.csv by SKU ({} when it's missing)."""
    if not path.is_file():
        return {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return {str(row.get("sku") or "").strip(): row for row in csv.DictReader(handle) if row.get("sku")}


def store_prices(conn) -> dict[str, dict]:
    """Each store product's title and price in dollars, by SKU."""
    return {sku: {"title": title, "price": cents / 100}
            for sku, title, cents in conn.execute("SELECT sku, title, price_cents FROM products") if sku}


def build(quotes: list[dict], unmatched: int, market: dict[str, dict], store: dict[str, dict],
          a: Assumptions) -> dict:
    """The page's table rows and summary, best buys first."""
    rows = []
    for quote in quotes:
        sku = str(quote.get("sku") or "").strip()
        cost_jpy = number(quote.get("cost_jpy"), None)
        if not sku or cost_jpy is None or cost_jpy <= 0:
            continue
        store_row, market_row = store.get(sku, {}), market.get(sku, {})
        cost_usd = cost_jpy / a.jpy_per_usd
        landed = landed_cost(cost_usd, a)
        fixed = landed + a.outbound_shipping_usd - a.shipping_credit_usd
        prices = {"store": store_row.get("price"), "market": number(market_row.get("market_price"), None),
                  "target": number(market_row.get("target_price"), None), "none": None}
        channels = {key: channel_result(ch, prices[ch.reference], fixed, a) for key, ch in a.channels.items()}
        best_key = min(channels, key=lambda key: _rank(channels[key]))
        floors = [c["for_margin"] for c in channels.values() if c["for_margin"] is not None]
        best = channels[best_key]
        market_price = prices["market"]
        rows.append({
            "sku": sku,
            "title": store_row.get("title") or market_row.get("title") or quote.get("item_name_raw") or sku,
            "supplier_item": str(quote.get("item_name_raw") or ""),
            "supplier": str(quote.get("supplier_name") or ""),
            "quote_date": str(quote.get("quote_date") or ""),
            "cost_jpy": round(cost_jpy),
            "cost_usd": money(cost_usd),
            "inbound_usd": money(a.inbound_per_box_usd),
            "landed": money(landed),
            "floor": money(min(floors)) if floors else None,
            "channels": channels,
            "best": best["name"],
            "best_key": best_key,
            "decision": best["decision"],
            # The best channel's numbers, for the Verdict cell and the sorts (SORTS).
            "best_profit": best["profit_after_tax"],
            "best_margin_pct": best["margin_after_tax_pct"],
            # Return on cost: profit after tax per dollar the box costs landed, the "where should
            # the money go" number (a cheap box with a fair margin can beat a dear one).
            "roi_pct": (round(best["profit_after_tax"] / landed * 100, 1)
                        if best["profit_after_tax"] is not None and landed > 0 else None),
            "market_price": money(market_price),
            # How far the market price sits above the landed cost, before any fees.
            "market_gap_pct": (round((market_price - landed) / landed * 100, 1)
                               if market_price and landed > 0 else None),
            "_rank": _rank(best),
        })
    rows.sort(key=lambda row: (row["_rank"], row["sku"]))
    profits = [row["channels"][key]["profit_after_tax"] for row in rows for key in row["channels"]
               if row["channels"][key]["name"] == row["best"] and row["channels"][key]["profit_after_tax"] is not None]
    return {
        "rows": rows,
        "counts": {decision: sum(1 for row in rows if row["decision"] == decision) for decision in DECISIONS},
        "avg_best_profit": money(sum(profits) / len(profits)) if profits else None,
        "unmatched": unmatched,
    }


# The table's sort orders: key -> label in the Sort menu. Biggest first, except "landed" and
# "name". Rows missing the number (no reference price, no market price) always sort last.
# "ch:<channel>" sorts by one channel's margin %; the channel columns' headers link to those.
SORTS = {
    "best": "Best buys first",
    "margin": "Margin % (best channel)",
    "profit": "Profit $ (best channel)",
    "roi": "Return on cost %",
    "market_gap": "Market above landed %",
    "landed": "Cheapest landed",
    "newest": "Newest quote",
    "name": "Name",
}


def _sort_value(row: dict, sort: str):
    """The value a row sorts on for one sort key ("ch:<channel>" sorts by that channel's margin); None sorts last."""
    if sort.startswith("ch:"):
        channel = row["channels"].get(sort[3:])
        return channel["margin_after_tax_pct"] if channel else None
    field_for = {"margin": "best_margin_pct", "profit": "best_profit", "roi": "roi_pct",
                 "market_gap": "market_gap_pct", "landed": "landed", "newest": "quote_date", "name": "title"}
    value = row.get(field_for.get(sort, ""))
    return value.lower() if isinstance(value, str) and sort == "name" else value


def sort_rows(rows: list[dict], sort: str) -> list[dict]:
    """The rows in the chosen order (SORTS, or "ch:<channel>"); "best" or an unknown key keeps
    build()'s order (verdict, then dollars above the target-margin price)."""
    if sort == "best" or (sort not in SORTS and not sort.startswith("ch:")):
        return list(rows)
    ascending = sort in ("landed", "name")
    present = [row for row in rows if _sort_value(row, sort) is not None]
    missing = [row for row in rows if _sort_value(row, sort) is None]
    present.sort(key=lambda row: _sort_value(row, sort), reverse=not ascending)
    return present + missing


def judged_on(rows: list[dict], channel: str) -> list[dict]:
    """The rows re-judged on one channel only (the "Channel" filter): verdict, profit, margin and
    return on cost come from that channel instead of the best one. Any other value: unchanged."""
    out = []
    for row in rows:
        result = row["channels"].get(channel)
        if result is None:
            return list(rows)
        profit = result["profit_after_tax"]
        out.append({**row, "best": result["name"], "best_key": channel, "decision": result["decision"],
                    "best_profit": profit, "best_margin_pct": result["margin_after_tax_pct"],
                    "roi_pct": round(profit / row["landed"] * 100, 1) if profit is not None and row["landed"] else None,
                    "_rank": _rank(result)})
    out.sort(key=lambda row: (row["_rank"], row["sku"]))
    return out


def visible(rows: list[dict], decision: str, needle: str, min_margin: float | None = None) -> list[dict]:
    """The rows the page's filter keeps: one decision (or all), a SKU/title/supplier-name match,
    and (when given) at least `min_margin` % after tax on the row's best (or chosen) channel."""
    needle = needle.strip().lower()
    return [row for row in rows if (decision not in DECISIONS or row["decision"] == decision)
            and (not needle or needle in f"{row['sku']} {row['title']} {row['supplier_item']}".lower())
            and (min_margin is None or (row["best_margin_pct"] is not None and row["best_margin_pct"] >= min_margin))]


def quick_quote(query: Mapping[str, str], a: Assumptions) -> dict | None:
    """The Quick Calculator: one typed quote (USD or JPY) against typed own-site and eBay prices,
    with the same assumptions. Nothing is saved. None until a price is entered."""
    price = number(query.get("q_price"), None)
    if price is None or price <= 0:
        return None
    in_yen = query.get("q_currency") == "jpy"
    cost_usd = price / a.jpy_per_usd if in_yen else price
    landed = landed_cost(cost_usd, a)
    fixed = landed + a.outbound_shipping_usd - a.shipping_credit_usd
    return {
        "item": str(query.get("q_item") or "").strip(), "vendor": str(query.get("q_vendor") or "").strip(),
        "price_label": f"{round(price):,} JPY" if in_yen else f"${price:,.2f}",
        "landed": money(landed), "fixed": money(fixed),
        "site": channel_result(a.channels["site"], number(query.get("q_site_price"), None), fixed, a),
        "ebay": channel_result(a.channels["ebay"], number(query.get("q_ebay_price"), None), fixed, a),
    }


class FxRate:
    """The latest JPY-per-USD rate, fetched at most once an hour. `fetch` returns the raw JSON
    text (tests pass a fake). A failed fetch keeps the last good rate, marked stale."""

    def __init__(self, fetch: Callable[[], str] | None = None):
        self._fetch = fetch or self._download
        self._lock = threading.Lock()
        self._value: dict | None = None
        self._at = 0.0
        self._failed_at = -FX_RETRY_SECONDS

    @staticmethod
    def _download() -> str:
        request = urllib.request.Request(FX_URL, headers={"User-Agent": "poke6s-admin"})
        with urllib.request.urlopen(request, timeout=8) as response:  # noqa: S310 (fixed https URL)
            return response.read().decode("utf-8")

    def get(self) -> dict | None:
        """{"rate", "date", "stale"} or None when it has never been fetched successfully."""
        with self._lock:
            if self._value and time.monotonic() - self._at < FX_CACHE_SECONDS:
                return self._value
            # After a failure, wait FX_RETRY_SECONDS before trying again, so an offline
            # Frankfurter doesn't add an 8-second wait to every page load.
            if time.monotonic() - self._failed_at < FX_RETRY_SECONDS:
                return self._value
            try:
                payload = json.loads(self._fetch())
                rows = payload if isinstance(payload, list) else []
                row = next(r for r in rows if str(r.get("base")).upper() == "USD"
                           and str(r.get("quote")).upper() == "JPY")
                rate = float(row["rate"])
                if rate <= 0:
                    raise ValueError("rate is not positive")
                self._value = {"rate": round(rate, 2), "date": str(row.get("date") or ""), "stale": False}
                self._at = time.monotonic()
            except Exception as exc:  # noqa: BLE001 (network, JSON or shape: all mean "no fresh rate")
                log.warning("JPY/USD rate not refreshed: %s", exc)
                self._failed_at = time.monotonic()
                if self._value:
                    self._value = {**self._value, "stale": True}
            return self._value
