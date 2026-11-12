"""Cart pricing, checkout, and the order life cycle. Plan: docs/storefront_plan.md "Checkout
and orders".

    cart (browser) --POST /cart/quote--> build_quote()      prices always from the database
    cart (browser) --POST /checkout----> start_checkout()   pending order, stock reserved,
                                                            Stripe Checkout Session created
    Stripe webhook / reconcile() ------> mark_paid()         reservation becomes a sale
                                      -> release()           checkout expired: stock back
                                      -> record_refund(), record_dispute()
    tools/ship_order.py ---------------> mark_shipped()      shipped email with tracking

Rules kept here:
- The browser sends only SKUs and quantities; prices, stock and shipping come from the
  database, so a changed price in the browser can't change what is charged.
- Stock never goes below zero: reservations use a conditional UPDATE, and products.stock has a
  CHECK (stock >= 0). Every stock change is a stock_movements row (reservation, release, sale),
  so stock always equals the sum of its movements.
- Every step is safe to repeat: a webhook delivered twice, or a webhook and the reconcile job
  seeing the same payment, changes the order once.
- The connection passed in must be a writer (db.connect(..., writer=True)): autocommit, with
  explicit BEGIN IMMEDIATE ... COMMIT around each multi-row change.
- Untracked products (track_stock = 0, Squarespace "Unlimited") take no stock.

Order numbers: P6-2026-0001 for live orders, P6T-2026-0001 for Stripe test mode, so test
orders never use up or mix with real numbers.
"""

from __future__ import annotations

import json
import logging
import secrets
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from urllib.parse import quote as urlquote

from . import notify
from .settings import Settings
from .stripe_api import StripeError

log = logging.getLogger("storefront.orders")

MAX_LINES = 50
MAX_QTY = 10                 # per line, when the product has no max_per_order of its own
# Shipping classes from smallest to largest; the cart ships at its largest class's rate.
SHIP_CLASS_ORDER = ["single", "accessory", "small-sealed", "large-sealed"]
SHIP_CLASS_NAMES = {"single": "single cards", "accessory": "accessories",
                    "small-sealed": "small sealed", "large-sealed": "large sealed"}
GENERAL_GOODS_TAX_CODE = "txcd_99999999"   # Stripe Tax: general tangible goods


def now_iso(now: datetime | None = None) -> str:
    return (now or datetime.now(UTC)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse(iso: str) -> datetime:
    return datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


# --- the cart -----------------------------------------------------------------------------

class CartError(ValueError):
    pass


def parse_cart(payload) -> list[tuple[str, int]]:
    """[{"sku": "...", "qty": 2}, ...] -> [(sku, qty)], merged by SKU, order kept.

    Raises CartError for anything that isn't a sensible cart, so a bad request gets a 400.
    """
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or len(items) > MAX_LINES:
        raise CartError("The cart is not readable.")
    merged: dict[str, int] = {}
    for item in items:
        if not isinstance(item, dict):
            raise CartError("The cart is not readable.")
        sku, qty = item.get("sku"), item.get("qty")
        if not isinstance(sku, str) or not (0 < len(sku) <= 64) or not isinstance(qty, int) or isinstance(qty, bool):
            raise CartError("The cart is not readable.")
        if qty > 0:
            merged[sku] = min(merged.get(sku, 0) + qty, 99)
    return list(merged.items())


@dataclass
class Line:
    sku: str
    product_id: int
    slug: str
    title: str
    image: str | None
    unit_cents: int
    quantity: int
    max_qty: int
    track_stock: bool
    ship_class: str

    @property
    def total_cents(self) -> int:
        return self.unit_cents * self.quantity


@dataclass
class Quote:
    lines: list[Line] = field(default_factory=list)
    # Changes the shop made to the cart (sold out, fewer left, no longer sold), worded for
    # the shopper. Checkout refuses a cart that needed changes, so the shopper sees them first.
    problems: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)       # SKUs taken out of the cart
    subtotal_cents: int = 0
    ship_class: str = ""
    shipping_cents: int = 0
    free_shipping_over_cents: int = 0
    to_free_shipping_cents: int = 0
    total_cents: int = 0

    def as_json(self) -> dict:
        data = asdict(self)
        data["lines"] = [dict(asdict(line), total_cents=line.total_cents) for line in self.lines]
        return data


def build_quote(conn: sqlite3.Connection, cart: list[tuple[str, int]], settings: Settings) -> Quote:
    quote = Quote(free_shipping_over_cents=settings.free_shipping_over_cents)
    for sku, wanted in cart:
        row = conn.execute(
            """SELECT p.*, (SELECT file_name FROM product_images i WHERE i.product_id = p.id
                            ORDER BY sort_order, id LIMIT 1) AS image
               FROM products p WHERE p.sku = ?""", (sku,)).fetchone()
        if row is None or not row["visible"] or row["price_cents"] <= 0:
            quote.removed.append(sku)
            quote.problems.append(f"{row['title'] if row else sku} is no longer for sale and was removed.")
            continue
        limit = row["max_per_order"] or MAX_QTY
        if row["track_stock"]:
            limit = min(limit, row["stock"])
        if limit <= 0:
            quote.removed.append(sku)
            quote.problems.append(f"{row['title']} just sold out and was removed.")
            continue
        quantity = min(wanted, limit)
        if quantity < wanted:
            quote.problems.append(f"Only {limit} of {row['title']} can be ordered; the quantity was lowered.")
        quote.lines.append(Line(sku=sku, product_id=row["id"], slug=row["slug"], title=row["title"],
                                image=row["image"], unit_cents=row["price_cents"], quantity=quantity,
                                max_qty=limit, track_stock=bool(row["track_stock"]),
                                ship_class=row["ship_class"]))
    quote.subtotal_cents = sum(line.total_cents for line in quote.lines)
    if quote.lines:
        quote.ship_class = max((line.ship_class for line in quote.lines),
                               key=lambda c: SHIP_CLASS_ORDER.index(c) if c in SHIP_CLASS_ORDER else len(SHIP_CLASS_ORDER))
        free_over = settings.free_shipping_over_cents
        if free_over and quote.subtotal_cents >= free_over:
            quote.shipping_cents = 0
        else:
            quote.shipping_cents = settings.ship_rates_cents.get(quote.ship_class, max(settings.ship_rates_cents.values()))
            quote.to_free_shipping_cents = max(0, free_over - quote.subtotal_cents) if free_over else 0
    quote.total_cents = quote.subtotal_cents + quote.shipping_cents
    return quote


# --- checkout -----------------------------------------------------------------------------

class CheckoutError(Exception):
    def __init__(self, status: int, message: str, quote: Quote | None = None) -> None:
        super().__init__(message)
        self.status, self.message, self.quote = status, message, quote


def next_number(conn: sqlite3.Connection, mode: str, year: int) -> str:
    prefix = f"{'P6' if mode == 'live' else 'P6T'}-{year}-"
    row = conn.execute("SELECT max(number) AS n FROM orders WHERE number LIKE ?", (prefix + "%",)).fetchone()
    last = int(row["n"].rsplit("-", 1)[1]) if row["n"] else 0
    return f"{prefix}{last + 1:04d}"


def start_checkout(conn: sqlite3.Connection, stripe, settings: Settings, cart: list[tuple[str, int]],
                   base_url: str, now: datetime | None = None) -> dict:
    """Reserve the stock, record a pending order, and open a Stripe Checkout Session.

    Returns {"url": <Stripe checkout page>, "number": ...}. Raises CheckoutError: 400 for an
    empty cart, 409 when the cart had to change (the shopper reviews it first), 502 when
    Stripe can't be reached (the reservation is released again).
    """
    now = now or datetime.now(UTC)
    minutes = max(30, settings.reservation_minutes)   # Stripe's shortest session is 30 minutes
    # A minute beyond the session's own expiry, so the stock is never released while the
    # Stripe page is still open.
    expires = now + timedelta(minutes=minutes, seconds=60)
    conn.execute("BEGIN IMMEDIATE")   # one writer at a time: two shoppers can't take the last box
    try:
        quote = build_quote(conn, cart, settings)
        if quote.problems:
            raise CheckoutError(409, "Your cart changed. Please check it and try again.", quote)
        if not quote.lines:
            raise CheckoutError(400, "Your cart is empty.", quote)
        number = next_number(conn, settings.stripe_mode, now.year)
        cursor = conn.execute(
            """INSERT INTO orders (number, access_token, status, mode, ship_class, subtotal_cents,
                                   shipping_cents, total_cents, reserved_until, created_at)
               VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)""",
            (number, secrets.token_urlsafe(16), settings.stripe_mode, quote.ship_class, quote.subtotal_cents,
             quote.shipping_cents, quote.total_cents, now_iso(expires), now_iso(now)))
        order_id = cursor.lastrowid
        for line in quote.lines:
            if line.track_stock:
                taken = conn.execute("UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?",
                                     (line.quantity, line.product_id, line.quantity)).rowcount
                if not taken:   # can't happen inside BEGIN IMMEDIATE, but never oversell
                    raise CheckoutError(409, "Your cart changed. Please check it and try again.", quote)
                conn.execute("INSERT INTO stock_movements (product_id, change, reason, reference, at) "
                             "VALUES (?, ?, 'reservation', ?, ?)", (line.product_id, -line.quantity, number, now_iso(now)))
            conn.execute("""INSERT INTO order_items (order_id, product_id, sku, title, unit_price_cents, quantity, reserved)
                            VALUES (?, ?, ?, ?, ?, ?, ?)""",
                         (order_id, line.product_id, line.sku, line.title, line.unit_cents, line.quantity,
                          int(line.track_stock)))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise

    order = get_order(conn, number)
    params = session_params(settings, quote, order, base_url, expires)
    try:
        # The order's secret token makes the key unique even if order numbers repeat (a test
        # database rebuilt, or a copy of the store): Stripe refuses a reused key with new details.
        session = stripe.create_session(params, idempotency_key=f"checkout-{number}-{order['access_token'][:12]}")
    except (StripeError, OSError) as exc:
        log.warning("Stripe session for %s failed: %s", number, exc)
        release(conn, number, "cancelled", now=now, note="Stripe checkout could not start")
        raise CheckoutError(502, "Checkout is unavailable right now. Please try again in a few minutes.") from exc
    conn.execute("UPDATE orders SET stripe_session_id = ? WHERE number = ?", (session["id"], number))
    conn.commit()
    return {"url": session["url"], "number": number}


def tax_note(settings: Settings) -> str:
    """One line for the cart about sales tax, matching the tax mode."""
    if settings.tax_mode == "stripe":
        return "Utah orders: sales tax is added on the next page."
    return ""


def session_params(settings: Settings, quote: Quote, order: dict, base_url: str, expires: datetime) -> dict:
    """The Stripe Checkout Session for an order, built from the database quote."""
    stripe_tax = settings.tax_mode == "stripe"
    line_items = []
    for line in quote.lines:
        price = {"currency": "usd", "unit_amount": line.unit_cents,
                 "product_data": {"name": line.title[:250], "metadata": {"sku": line.sku}}}
        if stripe_tax:
            price["tax_behavior"] = "exclusive"
            price["product_data"]["tax_code"] = GENERAL_GOODS_TAX_CODE
        line_items.append({"price_data": price, "quantity": line.quantity})
    if quote.shipping_cents:
        ship_name = f"Flat-rate shipping ({SHIP_CLASS_NAMES.get(quote.ship_class, quote.ship_class)})"
    else:
        ship_name = f"Free shipping (orders over {notify.money(settings.free_shipping_over_cents)})"
    rate = {"type": "fixed_amount", "display_name": ship_name,
            "fixed_amount": {"amount": quote.shipping_cents, "currency": "usd"}}
    if stripe_tax:
        rate["tax_behavior"] = "exclusive"
    number, token = order["number"], order["access_token"]
    params = {
        "mode": "payment",
        "line_items": line_items,
        "shipping_address_collection": {"allowed_countries": ["US"]},
        "shipping_options": [{"shipping_rate_data": rate}],
        "allow_promotion_codes": True,
        "client_reference_id": number,
        "metadata": {"order_number": number},
        "payment_intent_data": {"metadata": {"order_number": number}},
        "expires_at": int(expires.timestamp()) - 60,
        "success_url": f"{base_url}/order/thanks?session_id={{CHECKOUT_SESSION_ID}}",
        # Back from Stripe without paying: the cart page releases this order's reservation.
        "cancel_url": f"{base_url}/cart?cancelled={urlquote(number)}&k={urlquote(token)}",
    }
    if quote.total_cents >= settings.three_d_secure_over_cents:
        # Asks the card's bank to confirm the buyer, which moves most fraud chargebacks to the bank.
        params["payment_method_options"] = {"card": {"request_three_d_secure": "any"}}
    if stripe_tax:
        params["automatic_tax"] = {"enabled": True}
    return params


# --- order changes ------------------------------------------------------------------------

def get_order(conn: sqlite3.Connection, number: str | None = None, *, session_id: str | None = None,
              payment_intent: str | None = None) -> dict | None:
    if number:
        row = conn.execute("SELECT * FROM orders WHERE number = ?", (number,)).fetchone()
    elif session_id:
        row = conn.execute("SELECT * FROM orders WHERE stripe_session_id = ?", (session_id,)).fetchone()
    elif payment_intent:
        row = conn.execute("SELECT * FROM orders WHERE stripe_payment_intent = ?", (payment_intent,)).fetchone()
    else:
        return None
    if row is None:
        return None
    order = dict(row)
    order["shipping"] = json.loads(order["shipping_json"]) if order["shipping_json"] else None
    order["items"] = [dict(r) for r in conn.execute(
        "SELECT * FROM order_items WHERE order_id = ? ORDER BY id", (order["id"],))]
    return order


def release(conn: sqlite3.Connection, number: str, status: str, now: datetime | None = None, note: str = "") -> bool:
    """Close a pending order as 'expired' or 'cancelled' and give its stock back.

    Does nothing (returns False) if the order is no longer pending, so it is safe to repeat.
    """
    assert status in ("expired", "cancelled")
    at = now_iso(now)
    conn.execute("BEGIN IMMEDIATE")
    try:
        changed = conn.execute(
            "UPDATE orders SET status = ?, closed_at = ?, notes = trim(notes || ' ' || ?) "
            "WHERE number = ? AND status = 'pending'", (status, at, note, number)).rowcount
        if changed:
            for item in conn.execute(
                    "SELECT i.* FROM order_items i JOIN orders o ON o.id = i.order_id "
                    "WHERE o.number = ? AND i.reserved = 1 AND i.product_id IS NOT NULL", (number,)).fetchall():
                conn.execute("UPDATE products SET stock = stock + ? WHERE id = ?", (item["quantity"], item["product_id"]))
                conn.execute("INSERT INTO stock_movements (product_id, change, reason, reference, at) "
                             "VALUES (?, ?, 'release', ?, ?)", (item["product_id"], item["quantity"], number, at))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return bool(changed)


def _shipping_from(session: dict) -> dict | None:
    # Newer Stripe API versions put it under collected_information; older ones at the top.
    return ((session.get("collected_information") or {}).get("shipping_details")
            or session.get("shipping_details") or None)


def mark_paid(conn: sqlite3.Connection, settings: Settings, session: dict, base_url: str,
              now: datetime | None = None) -> dict | None:
    """Record a paid Checkout Session on its order: amounts, address, reservation -> sale.

    Returns the order if this call marked it paid, None if there was nothing to do (not paid
    yet, unknown session, or already recorded).
    """
    if session.get("payment_status") not in ("paid", "no_payment_required"):
        return None
    order = get_order(conn, session_id=session.get("id"))
    if order is None:
        number = (session.get("metadata") or {}).get("order_number") or session.get("client_reference_id")
        order = get_order(conn, number) if number else None
    if order is None:
        log.warning("Paid Stripe session for an unknown order")
        return None
    if order["status"] not in ("pending", "expired", "cancelled"):
        return None   # already paid (or later): a repeated delivery
    at = now_iso(now)
    customer = session.get("customer_details") or {}
    totals = session.get("total_details") or {}
    intent = session.get("payment_intent")
    intent_id = intent.get("id") if isinstance(intent, dict) else intent
    notes = []
    conn.execute("BEGIN IMMEDIATE")
    try:
        # Read the status again under the write lock: the webhook and the reconcile job can
        # see the same payment at the same moment, and only one of them may record it.
        status = conn.execute("SELECT status FROM orders WHERE id = ?", (order["id"],)).fetchone()["status"]
        if status not in ("pending", "expired", "cancelled"):
            conn.rollback()
            return None
        if status == "pending":
            # The normal case: the reserved stock becomes sold stock.
            conn.execute("UPDATE stock_movements SET reason = 'sale', at = ? "
                         "WHERE reference = ? AND reason = 'reservation'", (at, order["number"]))
        else:
            # Paid after the reservation was released (a late payment). Take the stock again
            # if it's still there; if not, flag it so the owner refunds or finds another copy.
            for item in order["items"]:
                if not item["reserved"] or item["product_id"] is None:
                    continue
                taken = conn.execute("UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?",
                                     (item["quantity"], item["product_id"], item["quantity"])).rowcount
                if taken:
                    conn.execute("INSERT INTO stock_movements (product_id, change, reason, reference, at) "
                                 "VALUES (?, ?, 'sale', ?, ?)", (item["product_id"], -item["quantity"], order["number"], at))
                else:
                    notes.append(f"OVERSOLD: {item['quantity']} x {item['sku']} was no longer in stock.")
        conn.execute(
            """UPDATE orders SET status = 'paid', paid_at = ?, email = ?, name = ?, shipping_json = ?,
                   stripe_session_id = ?, stripe_payment_intent = ?, subtotal_cents = ?, shipping_cents = ?,
                   tax_cents = ?, discount_cents = ?, total_cents = ?, closed_at = NULL,
                   notes = trim(notes || ' ' || ?)
               WHERE id = ?""",
            (at, customer.get("email"), customer.get("name"), json.dumps(_shipping_from(session)),
             session.get("id"), intent_id,
             session.get("amount_subtotal", order["subtotal_cents"]),
             totals.get("amount_shipping", order["shipping_cents"]),
             totals.get("amount_tax", 0), totals.get("amount_discount", 0),
             session.get("amount_total", order["total_cents"]), " ".join(notes), order["id"]))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    order = get_order(conn, order["number"])
    test = " (TEST)" if order["mode"] == "test" else ""
    lines = ", ".join(f"{i['quantity']} x {i['title']}" for i in order["items"])
    notify.discord(settings, f"New order{test} {order['number']}: {notify.money(order['total_cents'])}. {lines}"
                             + (f" {' '.join(notes)}" if notes else ""))
    notify.order_confirmation(settings, order, status_url(base_url, order))
    return order


def record_refund(conn: sqlite3.Connection, settings: Settings, charge: dict) -> dict | None:
    """charge.refunded: store the refunded total. Stock is not put back automatically: returns
    are inspected first (plan decision D8), then restocked by hand."""
    order = get_order(conn, payment_intent=charge.get("payment_intent"))
    if order is None:
        return None
    refunded = int(charge.get("amount_refunded") or 0)
    status = "refunded" if refunded >= int(charge.get("amount") or order["total_cents"]) else "partially_refunded"
    conn.execute("UPDATE orders SET refunded_cents = ?, status = ? WHERE id = ?", (refunded, status, order["id"]))
    conn.commit()
    notify.discord(settings, f"Refund recorded on {order['number']}: {notify.money(refunded)} ({status}).")
    return get_order(conn, order["number"])


def record_dispute(conn: sqlite3.Connection, settings: Settings, dispute: dict) -> dict | None:
    order = get_order(conn, payment_intent=dispute.get("payment_intent"))
    if order is None:
        return None
    conn.execute("UPDATE orders SET status = 'disputed', notes = trim(notes || ' ' || ?) WHERE id = ?",
                 (f"Dispute opened: {dispute.get('reason') or 'no reason given'}.", order["id"]))
    conn.commit()
    notify.discord(settings, f"DISPUTE opened on {order['number']} ({notify.money(dispute.get('amount') or 0)}, "
                             f"{dispute.get('reason') or 'no reason given'}). Answer it in the Stripe dashboard.")
    return get_order(conn, order["number"])


def mark_shipped(conn: sqlite3.Connection, settings: Settings, number: str, carrier: str, tracking: str,
                 base_url: str, now: datetime | None = None) -> dict:
    """Mark a paid order shipped and email the customer their tracking number."""
    order = get_order(conn, number)
    if order is None:
        raise ValueError(f"No order {number}")
    if order["status"] != "paid":
        raise ValueError(f"Order {number} is {order['status']}, not paid")
    conn.execute("UPDATE orders SET status = 'shipped', shipped_at = ?, carrier = ?, tracking_number = ? WHERE id = ?",
                 (now_iso(now), carrier.strip(), tracking.strip(), order["id"]))
    conn.commit()
    order = get_order(conn, number)
    notify.shipped_email(settings, order, status_url(base_url, order))
    return order


def status_url(base_url: str, order: dict) -> str:
    return f"{base_url}/order/{urlquote(order['number'])}?k={urlquote(order['access_token'])}"


# --- webhooks and the reconcile job -------------------------------------------------------

def handle_event(conn: sqlite3.Connection, settings: Settings, event: dict, base_url: str) -> str:
    """Apply one verified Stripe event. Returns what happened, for the log and tests."""
    event_id, kind = event.get("id"), event.get("type", "")
    if not event_id:
        return "ignored"
    if conn.execute("SELECT 1 FROM webhook_events WHERE event_id = ?", (event_id,)).fetchone():
        return "duplicate"
    obj = (event.get("data") or {}).get("object") or {}
    result = "ignored"
    if kind in ("checkout.session.completed", "checkout.session.async_payment_succeeded"):
        result = "paid" if mark_paid(conn, settings, obj, base_url) else "no change"
    elif kind in ("checkout.session.expired", "checkout.session.async_payment_failed"):
        order = get_order(conn, session_id=obj.get("id"))
        result = "released" if order and release(conn, order["number"], "expired") else "no change"
    elif kind == "charge.refunded":
        result = "refund" if record_refund(conn, settings, obj) else "no change"
    elif kind == "charge.dispute.created":
        result = "dispute" if record_dispute(conn, settings, obj) else "no change"
    # Recorded after the change, so if applying it fails, Stripe's retry applies it later.
    conn.execute("INSERT OR IGNORE INTO webhook_events (event_id, type, handled_at) VALUES (?, ?, ?)",
                 (event_id, kind, now_iso()))
    conn.commit()
    return result


# The events handle_event acts on. reconcile() asks Stripe for the last few days of these, so a
# delivery missed while the server or tunnel was down is still applied (security review item 11:
# without this, a refund or dispute on a paid order was only ever recorded by the webhook).
CATCH_UP_EVENTS = ("checkout.session.completed", "checkout.session.async_payment_succeeded",
                   "checkout.session.expired", "checkout.session.async_payment_failed",
                   "charge.refunded", "charge.dispute.created")
# Stripe retries a failed webhook delivery for up to 3 days; look back that far.
CATCH_UP_DAYS = 3


def catch_up_events(conn: sqlite3.Connection, settings: Settings, stripe, base_url: str,
                    now: datetime | None = None) -> int:
    """Apply recent Stripe events the webhook hasn't (already-handled ones are skipped by
    handle_event's webhook_events check). Returns how many changed something."""
    now = now or datetime.now(UTC)
    since = int((now - timedelta(days=CATCH_UP_DAYS)).timestamp())
    applied = 0
    for event in stripe.list_events(list(CATCH_UP_EVENTS), since):
        if handle_event(conn, settings, event, base_url) not in ("duplicate", "ignored", "no change"):
            applied += 1
    return applied


def reconcile(conn: sqlite3.Connection, settings: Settings, stripe, base_url: str,
              now: datetime | None = None) -> dict[str, int]:
    """Catch up on anything a missed webhook would have done.

    Runs at start-up and every 15 minutes (main.py). A pending order whose Stripe session was
    paid is marked paid; one whose session expired (or ran past its reservation) is released.
    Then the last CATCH_UP_DAYS of Stripe events are applied (catch_up_events), which is what
    records a refund or dispute on an order that was already paid.
    """
    now = now or datetime.now(UTC)
    counts = {"paid": 0, "released": 0, "events": 0, "errors": 0}
    pending = conn.execute("SELECT number, stripe_session_id, reserved_until FROM orders "
                           "WHERE status = 'pending' ORDER BY id").fetchall()
    for row in pending:
        overdue = row["reserved_until"] and _parse(row["reserved_until"]) < now
        try:
            if not row["stripe_session_id"]:
                if overdue and release(conn, row["number"], "cancelled", now=now, note="no Stripe session"):
                    counts["released"] += 1
                continue
            session = stripe.retrieve_session(row["stripe_session_id"])
            if session.get("payment_status") in ("paid", "no_payment_required"):
                if mark_paid(conn, settings, session, base_url, now=now):
                    counts["paid"] += 1
            elif session.get("status") == "expired":
                if release(conn, row["number"], "expired", now=now):
                    counts["released"] += 1
            elif overdue and session.get("status") == "open":
                stripe.expire_session(row["stripe_session_id"])
                if release(conn, row["number"], "expired", now=now):
                    counts["released"] += 1
        except (StripeError, OSError) as exc:
            counts["errors"] += 1
            log.warning("Reconcile %s: %s", row["number"], exc)
    try:
        counts["events"] = catch_up_events(conn, settings, stripe, base_url, now=now)
    except (StripeError, OSError) as exc:
        counts["errors"] += 1
        log.warning("Reconcile events: %s", exc)
    return counts
