"""Reports for the books (Phase 3, step 7): one month of poke6s.com sales, in the shapes the
owner's bookkeeping files already use.

The owner's choice (2026-11-20): poke6s.com orders are NOT entered in QuickBooks one by one.
QuickBooks sees only Stripe's payouts in the bank feed, like TCGplayer's. The detail lives in
two downloads from this page:

- **Sales** (`sales_csv`): one row per order paid in the month, in the Utah sales worksheet's
  columns (date, channel, buyer, items, sale_amount, tax_charged, how_paid,
  delivery_city_state, utah_taxable, source, notes), so the rows paste straight in.
  sale_amount is the order before tax (items - discount + shipping); a fully refunded order
  shows 0.00 and "no (refunded)", the way the worksheet already records refunded orders.
- **Stripe money** (`money_rows`): every charge and refund in the month with Stripe's fee and
  what landed in the balance, so the payouts in the bank feed can be grossed up into sales,
  tax collected, shipping and fees. Fees come from Stripe (the admin key may read charges, not
  payouts), so this one needs the admin's Stripe key and a connection.

The page also shows a Utah sales tax summary (month and year to date, from the store's own
records) and the month's "Sold on TCGplayer" stock changes, to check against TCGplayer's
Sales Summary.

Months are shop months (Utah time). Test-mode orders are left out everywhere.
"""

from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

SHOP_TZ = ZoneInfo("America/Denver")   # same zone as notify.MOUNTAIN
MONTH = re.compile(r"^(20\d\d)-(0[1-9]|1[0-2])$")
# Orders that took money (pending/expired/cancelled never did).
MONEY_STATUSES = ("paid", "shipped", "partially_refunded", "refunded")
WORKSHEET_COLUMNS = ["date", "channel", "buyer", "items", "sale_amount", "tax_charged", "how_paid",
                     "delivery_city_state", "utah_taxable", "source", "notes"]
MONEY_COLUMNS = ["date", "order", "type", "gross", "tax", "shipping", "stripe_fee", "net",
                 "expected_payout_date", "stripe_id"]


def parse_month(text: str | None, today: date | None = None) -> str:
    """'2026-11' as given, or the current shop month when it's missing or malformed."""
    if text and MONTH.match(text):
        return text
    return (today or datetime.now(SHOP_TZ).date()).strftime("%Y-%m")


def month_bounds(month: str) -> tuple[str, str]:
    """A shop month as UTC timestamps in the database's format: [start, end)."""
    year, mon = int(month[:4]), int(month[5:])
    start = datetime(year, mon, 1, tzinfo=SHOP_TZ)
    end = datetime(year + (mon == 12), mon % 12 + 1, 1, tzinfo=SHOP_TZ)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return start.astimezone(UTC).strftime(fmt), end.astimezone(UTC).strftime(fmt)


def year_bounds(month: str) -> tuple[str, str]:
    """From Jan 1 of the month's year to the end of the month (year to date)."""
    return month_bounds(month[:4] + "-01")[0], month_bounds(month)[1]


def shop_date(iso: str | None) -> str:
    """'2026-11-18T07:54:52Z' -> '2026-11-18' in shop time ('' when missing)."""
    if not iso:
        return ""
    return datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).astimezone(SHOP_TZ).date().isoformat()


def unix_date(seconds: int | None) -> str:
    """A Stripe timestamp as a shop date."""
    return datetime.fromtimestamp(seconds, UTC).astimezone(SHOP_TZ).date().isoformat() if seconds else ""


def dollars(cents: int | None) -> str:
    return f"{(cents or 0) / 100:.2f}"


def _address(order: dict) -> dict:
    try:
        return (json.loads(order.get("shipping_json") or "{}") or {}).get("address") or {}
    except ValueError:
        return {}


def _orders(conn: sqlite3.Connection, start: str, end: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM orders WHERE mode = 'live' AND paid_at >= ? AND paid_at < ? AND status IN "
        f"({','.join('?' * len(MONEY_STATUSES))}) ORDER BY paid_at", (start, end, *MONEY_STATUSES)).fetchall()
    orders = [dict(row) for row in rows]
    for order in orders:
        order["items"] = [dict(r) for r in conn.execute(
            "SELECT sku, title, quantity, unit_price_cents FROM order_items WHERE order_id = ? ORDER BY id",
            (order["id"],))]
        address = _address(order)
        order["state"] = (address.get("state") or "").upper()
        city = (address.get("city") or "").strip().title()
        order["city_state"] = ", ".join(part for part in (city, order["state"]) if part)
        order["before_tax_cents"] = order["total_cents"] - (order["tax_cents"] or 0)
    return orders


def sales(conn: sqlite3.Connection, month: str) -> list[dict]:
    """The month's paid live orders, oldest first, with items, ship-to state and amounts."""
    return _orders(conn, *month_bounds(month))


def worksheet_row(order: dict) -> dict:
    """One order as a Utah sales worksheet row (see the module docstring)."""
    refunded = order["refunded_cents"] or 0
    full_refund = refunded >= order["total_cents"]
    items = "; ".join(f"{i['quantity']}x {i['title']}" if i["quantity"] > 1 else i["title"]
                      for i in order["items"])
    notes = [f"items {dollars(order['subtotal_cents'])}, shipping {dollars(order['shipping_cents'])}"]
    if order["discount_cents"]:
        notes.append(f"discount {dollars(order['discount_cents'])}")
    if refunded and not full_refund:
        notes.append(f"partly refunded {dollars(refunded)}: check the tax on it")
    if full_refund:
        utah = "no (refunded)"
    else:
        utah = "yes" if order["state"] == "UT" else "no"
    return {
        "date": shop_date(order["paid_at"]),
        "channel": "poke6s.com",
        "buyer": order["name"] or "",
        "items": items,
        "sale_amount": "0.00" if full_refund else dollars(order["before_tax_cents"]),
        "tax_charged": "0" if full_refund else dollars(order["tax_cents"]),
        "how_paid": "Stripe",
        "delivery_city_state": "Fully refunded" if full_refund else order["city_state"],
        "utah_taxable": utah,
        "source": f"poke6s.com order {order['number']}",
        "notes": "; ".join(notes),
    }


def sales_csv(conn: sqlite3.Connection, month: str) -> str:
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=WORKSHEET_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for order in sales(conn, month):
        writer.writerow(worksheet_row(order))
    return out.getvalue()


def utah_summary(conn: sqlite3.Connection, start: str, end: str) -> dict:
    """Totals for the sales tax return over [start, end), from the store's records. Fully
    refunded orders are left out; partly refunded ones are counted and listed for a check."""
    summary = {"orders": 0, "utah_orders": 0, "utah_sales_cents": 0, "utah_tax_cents": 0,
               "other_sales_cents": 0, "other_tax_cents": 0, "refunded_orders": 0, "partial": []}
    for order in _orders(conn, start, end):
        refunded = order["refunded_cents"] or 0
        if refunded >= order["total_cents"]:
            summary["refunded_orders"] += 1
            continue
        if refunded:
            summary["partial"].append(order["number"])
        summary["orders"] += 1
        if order["state"] == "UT":
            summary["utah_orders"] += 1
            summary["utah_sales_cents"] += order["before_tax_cents"]
            summary["utah_tax_cents"] += order["tax_cents"] or 0
        else:
            summary["other_sales_cents"] += order["before_tax_cents"]
            summary["other_tax_cents"] += order["tax_cents"] or 0
    return summary


def tcgplayer_sales(conn: sqlite3.Connection, month: str) -> list[dict]:
    """The month's "Sold on TCGplayer" stock changes (product page, reason 'tcgplayer_sale')."""
    start, end = month_bounds(month)
    rows = conn.execute(
        "SELECT m.at, m.change, m.reference, p.id AS product_id, p.sku, p.title FROM stock_movements m "
        "JOIN products p ON p.id = m.product_id WHERE m.reason = 'tcgplayer_sale' AND m.at >= ? AND m.at < ? "
        "ORDER BY m.at", (start, end)).fetchall()
    # The reference reads "Sold on TCGplayer" or "Sold on TCGplayer: <owner's note>"; keep the note.
    return [{**dict(row), "date": shop_date(row["at"]), "units": -row["change"],
             "note": (row["reference"] or "").partition(": ")[2]} for row in rows]


def money_rows(conn: sqlite3.Connection, client, month: str) -> list[dict]:
    """Every Stripe charge and refund dated in the month (shop time), with fees and net.

    Charges come from the month's orders; refunds can be on older orders, so every live order
    paid before the month's end that has a refund is checked too. Raises StripeError (from the
    client) when Stripe can't be reached or refuses."""
    start, end = month_bounds(month)
    candidates = conn.execute(
        "SELECT * FROM orders WHERE mode = 'live' AND stripe_payment_intent IS NOT NULL AND paid_at < ? "
        "AND (paid_at >= ? OR refunded_cents > 0) ORDER BY paid_at", (end, start)).fetchall()
    rows: list[dict] = []
    for order in map(dict, candidates):
        for charge in client.list_charges(order["stripe_payment_intent"]):
            if charge.get("status") != "succeeded":
                continue   # declined attempts moved no money
            charge_day = unix_date(charge.get("created"))
            if charge_day[:7] == month:
                txn = charge.get("balance_transaction") or {}
                fee = txn.get("fee", 0) if isinstance(txn, dict) else 0
                rows.append({
                    "date": charge_day, "order": order["number"], "type": "sale",
                    "gross": dollars(charge["amount"]), "tax": dollars(order["tax_cents"]),
                    "shipping": dollars(order["shipping_cents"]), "stripe_fee": dollars(fee),
                    "net": dollars(charge["amount"] - fee),
                    "expected_payout_date": unix_date(txn.get("available_on")) if isinstance(txn, dict) else "",
                    "stripe_id": charge.get("id", ""),
                })
            for refund in ((charge.get("refunds") or {}).get("data") or []):
                if refund.get("status") not in ("succeeded", "pending"):
                    continue
                refund_day = unix_date(refund.get("created"))
                if refund_day[:7] != month:
                    continue
                rows.append({
                    "date": refund_day, "order": order["number"], "type": "refund",
                    "gross": dollars(-refund["amount"]), "tax": "", "shipping": "",
                    # Stripe keeps the original fee on a refund and charges nothing more.
                    "stripe_fee": "0.00", "net": dollars(-refund["amount"]),
                    "expected_payout_date": "", "stripe_id": refund.get("id", ""),
                })
    rows.sort(key=lambda row: (row["date"], row["order"], row["type"] != "sale"))
    return rows


def money_totals(rows: list[dict]) -> dict:
    """Sums of the money rows, in cents."""
    def cents(value: str) -> int:
        return round(float(value) * 100) if value else 0
    sales_rows = [r for r in rows if r["type"] == "sale"]
    refund_rows = [r for r in rows if r["type"] == "refund"]
    return {
        "sales": sum(cents(r["gross"]) for r in sales_rows),
        "tax": sum(cents(r["tax"]) for r in sales_rows),
        "shipping": sum(cents(r["shipping"]) for r in sales_rows),
        "fees": sum(cents(r["stripe_fee"]) for r in rows),
        "refunds": sum(cents(r["gross"]) for r in refund_rows),
        "net": sum(cents(r["net"]) for r in rows),
    }


def money_csv(rows: list[dict]) -> str:
    """The money rows plus a TOTAL line (the gross-up for the month's Stripe payouts)."""
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=MONEY_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    totals = money_totals(rows)
    writer.writerow({"date": "TOTAL", "type": f"sales {dollars(totals['sales'])}, refunds {dollars(totals['refunds'])}",
                     "gross": dollars(totals["sales"] + totals["refunds"]), "tax": dollars(totals["tax"]),
                     "shipping": dollars(totals["shipping"]), "stripe_fee": dollars(totals["fees"]),
                     "net": dollars(totals["net"])})
    return out.getvalue()
