"""Refunds and returns from the admin's order page (Phase 3, step 4).

One form does both halves of a return (plan D8: a returned box is inspected first, then
refunded):
- **Refund** any amount up to what's left on the order, through Stripe, with the admin's own
  restricted key (`STOREFRONT_ADMIN_STRIPE_KEY`: Charges and Refunds write; the public store's
  key can't refund). The order is updated straight away (orders.record_refund, the same code the
  charge.refunded webhook runs, which then finds nothing new), the customer gets a refund email,
  and a dated note records what was done.
- **Put back** returned units: a 'restock' stock movement per product, referenced
  "Return <order number>", so the form never offers more than the order sold minus what was
  already put back.
Either half can be used alone ($0 refund with units put back = a return with no money back yet).

Safety: the amount is checked against the order (never more than paid minus refunded), the
owner must tick a confirmation, and the Stripe request carries an idempotency key built from
the order and its refunded total, so a double-clicked button can't refund twice. Test-mode
orders aren't refunded here: the key is a live one.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime

from ..app import notify
from ..app import orders as store_orders
from ..app.orders import now_iso
from ..app.product_changes import clean_reason, parse_money
from ..app.settings import Settings
from ..app.stripe_api import StripeError

REFUNDABLE = ("paid", "shipped", "partially_refunded")
# Stripe's refund reasons (shown to the customer's bank); "" sends none.
REASONS = {"requested_by_customer": "Customer asked (return or cancel)", "duplicate": "Charged twice",
           "fraudulent": "Fraud", "": "Other"}


def remaining_cents(order: dict) -> int:
    return max(0, int(order["total_cents"]) - int(order["refunded_cents"] or 0))


def can_refund(order: dict) -> str:
    """'' when money can go back on this order, otherwise why not (shown on the page)."""
    if order["status"] == "disputed":
        return "This payment is disputed: the bank decides, so answer the dispute in Stripe instead."
    if order["status"] not in REFUNDABLE:
        return ""   # not a sale (pending, expired, cancelled) or already fully refunded: no form
    if order["mode"] != "live":
        return "Test order: refund it in Stripe's test dashboard (the admin's key is a live one)."
    if not order.get("stripe_payment_intent"):
        return "No Stripe payment on this order."
    return ""


def returnable(conn: sqlite3.Connection, order: dict) -> list[dict]:
    """The order's stock-counted lines with how many units can still be put back."""
    lines = []
    for item in order["items"]:
        if not item.get("product_id"):
            continue
        product = conn.execute("SELECT track_stock FROM products WHERE id = ?", (item["product_id"],)).fetchone()
        if product is None or not product["track_stock"]:
            continue
        back = conn.execute("SELECT coalesce(sum(change), 0) FROM stock_movements WHERE product_id = ? "
                            "AND reason = 'restock' AND reference = ?",
                            (item["product_id"], return_reference(order["number"]))).fetchone()[0]
        lines.append({"product_id": item["product_id"], "sku": item["sku"], "title": item["title"],
                      "sold": item["quantity"], "back": int(back), "can": max(0, item["quantity"] - int(back))})
    return lines


def return_reference(number: str) -> str:
    return f"Return {number}"


def refund(conn: sqlite3.Connection, settings: Settings, client, number: str, amount_text: str, reason: str,
           put_back: dict[int, int], note: str, confirmed: bool, now: datetime | None = None) -> dict:
    """Refund and/or put back stock on one order. Returns {"refunded": cents, "put_back": units,
    "email_sent": bool | None}. Raises ValueError with a message for the owner."""
    order = store_orders.get_order(conn, number)
    if order is None:
        raise ValueError(f"No order {number}")
    amount = parse_money(amount_text) if amount_text.strip() else 0
    if amount is None:
        raise ValueError("The refund amount should look like 138.99")
    if reason not in REASONS:
        raise ValueError("Pick a reason")
    lines = {line["product_id"]: line for line in returnable(conn, order)}
    units = {pid: qty for pid, qty in put_back.items() if qty}
    for pid, qty in units.items():
        if pid not in lines or not 0 < qty <= lines[pid]["can"]:
            raise ValueError("That's more units than this order can put back")
    if not amount and not units:
        raise ValueError("Type an amount to refund, or choose units to put back (or both)")
    if amount:
        blocked = can_refund(order) or ("" if order["status"] in REFUNDABLE else "This order can't be refunded.")
        if blocked:
            raise ValueError(blocked)
        if amount > remaining_cents(order):
            raise ValueError(f"At most {notify.money(remaining_cents(order))} is left to refund on this order")
    if not confirmed:
        raise ValueError("Tick the box to confirm" + (f" the {notify.money(amount)} refund" if amount else ""))
    note = clean_reason(note)

    email_sent = None
    if amount:
        params = {"payment_intent": order["stripe_payment_intent"], "amount": amount,
                  "metadata": {"order": number, "source": "poke6s admin"}}
        if reason:
            params["reason"] = reason
        try:
            # Same order, same refunded-so-far, same amount -> same key: a repeated click is
            # answered with the first refund instead of a second one.
            client.create_refund(params, f"refund-{number}-{order['refunded_cents']}-{amount}")
        except StripeError as exc:
            raise ValueError(f"Stripe refused the refund: {exc}") from None
        # Record it now (the webhook repeats it later and finds nothing new).
        order = store_orders.record_refund(conn, settings, {
            "payment_intent": order["stripe_payment_intent"], "amount": order["total_cents"],
            "amount_refunded": int(order["refunded_cents"] or 0) + amount}) or order
        if order.get("email"):
            email_sent = notify.refund_email(settings, order, store_orders.status_url(settings.base_url, order), amount)

    if units:
        at = now_iso(now)
        conn.execute("BEGIN IMMEDIATE")
        try:
            for pid, qty in units.items():
                conn.execute("UPDATE products SET stock = stock + ?, updated_at = ? WHERE id = ?", (qty, at, pid))
                conn.execute("INSERT INTO stock_movements (product_id, change, reason, reference, at) "
                             "VALUES (?, ?, 'restock', ?, ?)", (pid, qty, return_reference(number), at))
            conn.commit()
        except BaseException:
            conn.rollback()
            raise

    parts = []
    if amount:
        parts.append(f"Refunded {notify.money(amount)} ({REASONS[reason].lower()})")
    if units:
        parts.append("put back " + ", ".join(f"{qty}x {lines[pid]['sku']}" for pid, qty in units.items()))
    text = "; ".join(parts) + "."
    store_orders.add_note(conn, number, text[0].upper() + text[1:] + (f" {note}" if note else ""), now)
    return {"refunded": amount, "put_back": sum(units.values()), "email_sent": email_sent}
