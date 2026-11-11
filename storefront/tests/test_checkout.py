"""Phase 2: cart pricing, checkout, webhooks, reconciliation and shipping (orders.py).

Stripe is replaced by FakeStripe, so nothing here touches the network. Fixture products (from
tests/fixtures/squarespace_export_sample.csv):
  JP-AE-BB   $71.99  stock 3   large-sealed
  ENG-BB-BB  $79.99  stock 1   small-sealed
  ENG-G25-TIN $26.42 stock 4   large-sealed
  JP-PACK-A  $5.00   stock 10  small-sealed
  111        $31.43  untracked accessory
  ENG-SS-ETB hidden; JP-BB-BOX hidden and sold out
"""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from storefront.app import orders
from storefront.app.db import connect
from storefront.app.main import create_app
from storefront.app.settings import load_settings
from storefront.app.stripe_api import StripeError, form_encode, sign_payload, verify_webhook

WEBHOOK_SECRET = "whsec_test_secret"


class FakeStripe:
    def __init__(self) -> None:
        self.sessions: dict[str, dict] = {}
        self.fail = False
        self.expired_calls: list[str] = []

    def create_session(self, params: dict, idempotency_key: str) -> dict:
        if self.fail:
            raise StripeError("Stripe is down")
        session_id = f"cs_test_{len(self.sessions) + 1}"
        session = {"id": session_id, "url": f"https://checkout.stripe.test/{session_id}", "status": "open",
                   "payment_status": "unpaid", "metadata": params["metadata"],
                   "client_reference_id": params["client_reference_id"], "params": params}
        self.sessions[session_id] = session
        return session

    def retrieve_session(self, session_id: str) -> dict:
        return self.sessions[session_id]

    def expire_session(self, session_id: str) -> dict:
        self.expired_calls.append(session_id)
        self.sessions[session_id]["status"] = "expired"
        return self.sessions[session_id]

    def pay(self, session_id: str, tax: int = 0) -> dict:
        """What Stripe's session looks like after the shopper pays."""
        session = self.sessions[session_id]
        params = session["params"]
        subtotal = sum(li["price_data"]["unit_amount"] * li["quantity"] for li in params["line_items"])
        shipping = params["shipping_options"][0]["shipping_rate_data"]["fixed_amount"]["amount"]
        session.update(
            status="complete", payment_status="paid", payment_intent=f"pi_{session_id}",
            amount_subtotal=subtotal, amount_total=subtotal + shipping + tax,
            total_details={"amount_shipping": shipping, "amount_tax": tax, "amount_discount": 0},
            customer_details={"email": "buyer@example.com", "name": "Ash Ketchum"},
            collected_information={"shipping_details": {"name": "Ash Ketchum", "address": {
                "line1": "1 Pallet Town Rd", "line2": None, "city": "Salt Lake City", "state": "UT",
                "postal_code": "84101", "country": "US"}}})
        return session


@pytest.fixture()
def shop(imported):
    return dataclasses.replace(imported, stripe_secret_key="sk_test_fake", stripe_webhook_secret=WEBHOOK_SECRET)


@pytest.fixture()
def fake():
    return FakeStripe()


@pytest.fixture()
def app_client(shop, fake):
    with TestClient(create_app(shop, stripe=fake), follow_redirects=False) as client:
        yield client


def cart(*items):
    return {"items": [{"sku": sku, "qty": qty} for sku, qty in items]}


def stock(settings, sku):
    with connect(settings.db_path) as conn:
        return conn.execute("SELECT stock FROM products WHERE sku = ?", (sku,)).fetchone()[0]


def order_row(settings, number):
    conn = connect(settings.db_path)
    try:
        return orders.get_order(conn, number)
    finally:
        conn.close()


def assert_stock_matches_movements(settings):
    """products.stock always equals the sum of its stock movements (plan "Data model")."""
    with connect(settings.db_path) as conn:
        bad = conn.execute(
            """SELECT p.sku, p.stock, coalesce(sum(m.change), 0) AS moved FROM products p
               LEFT JOIN stock_movements m ON m.product_id = p.id GROUP BY p.id
               HAVING p.stock != coalesce(sum(m.change), 0)""").fetchall()
    assert [tuple(r) for r in bad] == []


def start(client, *items):
    response = client.post("/checkout", json=cart(*items))
    assert response.status_code == 200, response.text
    session_id = response.json()["url"].rsplit("/", 1)[1]
    return session_id


def send_event(client, event: dict, secret: str = WEBHOOK_SECRET):
    payload = json.dumps(event).encode()
    return client.post("/stripe/webhook", content=payload,
                       headers={"Stripe-Signature": sign_payload(payload, secret), "Content-Type": "application/json"})


def number_of(fake, session_id):
    return fake.sessions[session_id]["metadata"]["order_number"]


# --- quote --------------------------------------------------------------------------------

def test_quote_prices_come_from_the_database(app_client):
    body = {"items": [{"sku": "JP-PACK-A", "qty": 2, "price": 1, "unit_cents": 1}]}
    quote = app_client.post("/cart/quote", json=body).json()
    assert quote["lines"][0]["unit_cents"] == 500
    assert quote["subtotal_cents"] == 1000


@pytest.mark.parametrize("items, ship_class, shipping", [
    ((("JP-PACK-A", 1),), "small-sealed", 1000),
    ((("JP-PACK-A", 1), ("ENG-G25-TIN", 1)), "large-sealed", 2000),     # the largest class wins
    ((("111", 1),), "accessory", 1000),
    ((("JP-AE-BB", 3),), "large-sealed", 0),                            # $215.97: free over $150
])
def test_shipping_class_and_free_shipping(app_client, items, ship_class, shipping):
    quote = app_client.post("/cart/quote", json=cart(*items)).json()
    assert (quote["ship_class"], quote["shipping_cents"]) == (ship_class, shipping)
    assert quote["total_cents"] == quote["subtotal_cents"] + shipping


def test_quote_reports_how_far_from_free_shipping(app_client):
    quote = app_client.post("/cart/quote", json=cart(("JP-AE-BB", 1), ("JP-PACK-A", 1))).json()
    assert quote["subtotal_cents"] == 7699
    assert quote["to_free_shipping_cents"] == 15000 - 7699


def test_quote_lowers_and_removes_what_cant_be_sold(app_client):
    quote = app_client.post("/cart/quote", json=cart(("ENG-BB-BB", 5), ("JP-BB-BOX", 1), ("NOPE", 1),
                                                     ("ENG-SS-ETB", 1))).json()
    assert [(line["sku"], line["quantity"]) for line in quote["lines"]] == [("ENG-BB-BB", 1)]
    assert set(quote["removed"]) == {"JP-BB-BOX", "NOPE", "ENG-SS-ETB"}
    assert len(quote["problems"]) == 4


@pytest.mark.parametrize("body", [{}, {"items": "x"}, {"items": [{"sku": 5, "qty": 1}]},
                                  {"items": [{"sku": "A", "qty": "1"}]}, {"items": [{"sku": "A", "qty": True}]}])
def test_unreadable_carts_are_refused(app_client, body):
    assert app_client.post("/cart/quote", json=body).status_code == 400
    assert app_client.post("/checkout", json=body).status_code == 400


# --- checkout -----------------------------------------------------------------------------

def test_checkout_reserves_stock_and_builds_the_session_from_the_database(app_client, shop, fake):
    session_id = start(app_client, ("JP-AE-BB", 2), ("111", 1))
    params = fake.sessions[session_id]["params"]
    assert [(li["price_data"]["unit_amount"], li["quantity"]) for li in params["line_items"]] == [(7199, 2), (3143, 1)]
    assert params["shipping_address_collection"] == {"allowed_countries": ["US"]}
    assert params["shipping_options"][0]["shipping_rate_data"]["fixed_amount"]["amount"] == 0   # $175.41
    assert params["payment_method_options"]["card"]["request_three_d_secure"] == "any"
    assert params["allow_promotion_codes"] is True
    assert "automatic_tax" not in params
    number = number_of(fake, session_id)
    assert number.startswith("P6T-")          # test mode numbers
    order = order_row(shop, number)
    assert order["status"] == "pending" and order["total_cents"] == 17541
    assert stock(shop, "JP-AE-BB") == 1
    assert_stock_matches_movements(shop)


def test_small_orders_skip_three_d_secure(app_client, fake):
    session_id = start(app_client, ("JP-PACK-A", 1))
    assert "payment_method_options" not in fake.sessions[session_id]["params"]


def test_checkout_refuses_a_cart_that_changed(app_client, shop):
    response = app_client.post("/checkout", json=cart(("ENG-BB-BB", 3)))
    assert response.status_code == 409
    assert response.json()["quote"]["lines"][0]["quantity"] == 1
    assert stock(shop, "ENG-BB-BB") == 1      # nothing reserved


def test_the_last_box_cant_be_sold_twice(app_client, shop):
    start(app_client, ("ENG-BB-BB", 1))
    second = app_client.post("/checkout", json=cart(("ENG-BB-BB", 1)))
    assert second.status_code == 409
    assert stock(shop, "ENG-BB-BB") == 0
    assert_stock_matches_movements(shop)


def test_stripe_failure_releases_the_reservation(app_client, shop, fake):
    fake.fail = True
    response = app_client.post("/checkout", json=cart(("JP-AE-BB", 1)))
    assert response.status_code == 502
    assert stock(shop, "JP-AE-BB") == 3
    with connect(shop.db_path) as conn:
        assert conn.execute("SELECT status FROM orders").fetchone()[0] == "cancelled"
    assert_stock_matches_movements(shop)


def test_stripe_tax_mode_asks_stripe_to_add_tax(imported, fake):
    settings = dataclasses.replace(imported, stripe_secret_key="sk_test_fake", tax_mode="stripe")
    with TestClient(create_app(settings, stripe=fake)) as client:
        params = fake.sessions[start(client, ("JP-PACK-A", 1))]["params"]
    assert params["automatic_tax"] == {"enabled": True}
    assert params["line_items"][0]["price_data"]["tax_behavior"] == "exclusive"
    with TestClient(create_app(settings, stripe=fake)) as client:
        assert 'data-tax-note="Utah orders: sales tax is added on the next page."' in client.get("/cart").text


def test_unknown_tax_mode_keeps_checkout_off(imported):
    settings = dataclasses.replace(imported, stripe_secret_key="sk_test_fake", tax_mode="included")
    assert not settings.checkout_enabled and "STOREFRONT_TAX_MODE" in settings.tax_problem


def test_checkout_key_survives_reused_order_numbers(app_client, fake):
    """Stripe remembers idempotency keys for 24 hours; a rebuilt test database starts at -0001
    again, so the key must include more than the order number."""
    key_holder = []
    original = fake.create_session
    fake.create_session = lambda params, idempotency_key: key_holder.append(idempotency_key) or original(params, idempotency_key)
    start(app_client, ("JP-PACK-A", 1))
    assert key_holder[0].startswith("checkout-P6T-") and len(key_holder[0]) > len("checkout-P6T-2026-0001-")


# --- payment, webhooks and reconciliation ---------------------------------------------------

def test_paid_webhook_turns_the_reservation_into_a_sale(app_client, shop, fake):
    session_id = start(app_client, ("JP-AE-BB", 1))
    event = {"id": "evt_1", "type": "checkout.session.completed", "data": {"object": fake.pay(session_id, tax=500)}}
    assert send_event(app_client, event).json()["result"] == "paid"
    number = number_of(fake, session_id)
    order = order_row(shop, number)
    assert order["status"] == "paid" and order["email"] == "buyer@example.com"
    assert order["tax_cents"] == 500 and order["total_cents"] == 7199 + 2000 + 500
    assert order["shipping"]["address"]["state"] == "UT"
    with connect(shop.db_path) as conn:
        reasons = [r[0] for r in conn.execute("SELECT reason FROM stock_movements WHERE reference = ?", (number,))]
    assert reasons == ["sale"]
    # The confirmation email went to the outbox (no SMTP in tests).
    emails = list(shop.outbox_dir.glob("*.eml"))
    assert len(emails) == 1 and "[TEST] Poke6s order" in emails[0].read_text()
    # Stripe delivers the same event again: nothing changes, no second email.
    assert send_event(app_client, event).json()["result"] == "duplicate"
    assert len(list(shop.outbox_dir.glob("*.eml"))) == 1
    assert stock(shop, "JP-AE-BB") == 2
    assert_stock_matches_movements(shop)


def test_webhook_with_a_bad_signature_is_refused(app_client, fake):
    session_id = start(app_client, ("JP-PACK-A", 1))
    event = {"id": "evt_x", "type": "checkout.session.completed", "data": {"object": fake.pay(session_id)}}
    assert send_event(app_client, event, secret="whsec_wrong").status_code == 400


def test_signature_check_refuses_old_deliveries():
    payload = b'{"id": "evt"}'
    header = sign_payload(payload, WEBHOOK_SECRET, timestamp=1_000_000)
    with pytest.raises(ValueError):
        verify_webhook(payload, header, WEBHOOK_SECRET, now=1_000_000 + 3600)
    assert verify_webhook(payload, header, WEBHOOK_SECRET, now=1_000_010) == {"id": "evt"}


def test_expired_session_gives_the_stock_back(app_client, shop, fake):
    session_id = start(app_client, ("JP-AE-BB", 2))
    fake.sessions[session_id]["status"] = "expired"
    event = {"id": "evt_2", "type": "checkout.session.expired", "data": {"object": fake.sessions[session_id]}}
    assert send_event(app_client, event).json()["result"] == "released"
    assert stock(shop, "JP-AE-BB") == 3
    assert order_row(shop, number_of(fake, session_id))["status"] == "expired"
    assert_stock_matches_movements(shop)


def test_reconcile_catches_a_missed_payment(app_client, shop, fake):
    session_id = start(app_client, ("JP-PACK-A", 3))
    fake.pay(session_id)                       # paid, but the webhook never arrived
    conn = connect(shop.db_path, writer=True)
    try:
        assert orders.reconcile(conn, shop, fake, "https://shop.test")["paid"] == 1
        assert orders.reconcile(conn, shop, fake, "https://shop.test")["paid"] == 0   # once only
    finally:
        conn.close()
    assert order_row(shop, number_of(fake, session_id))["status"] == "paid"
    assert_stock_matches_movements(shop)


def test_reconcile_releases_overdue_and_expired_checkouts(app_client, shop, fake):
    overdue = start(app_client, ("JP-AE-BB", 1))
    gone = start(app_client, ("JP-PACK-A", 2))
    fake.sessions[gone]["status"] = "expired"
    later = datetime.now(UTC) + timedelta(minutes=45)
    conn = connect(shop.db_path, writer=True)
    try:
        counts = orders.reconcile(conn, shop, fake, "https://shop.test", now=later)
    finally:
        conn.close()
    assert counts["released"] == 2
    assert fake.expired_calls == [overdue]     # the still-open session is closed on Stripe too
    assert (stock(shop, "JP-AE-BB"), stock(shop, "JP-PACK-A")) == (3, 10)
    assert_stock_matches_movements(shop)


def test_late_payment_after_release_takes_the_stock_again(app_client, shop, fake):
    session_id = start(app_client, ("JP-AE-BB", 1))
    number = number_of(fake, session_id)
    conn = connect(shop.db_path, writer=True)
    try:
        orders.release(conn, number, "expired")
        assert stock(shop, "JP-AE-BB") == 3
        assert orders.mark_paid(conn, shop, fake.pay(session_id), "https://shop.test")["status"] == "paid"
    finally:
        conn.close()
    assert stock(shop, "JP-AE-BB") == 2
    assert_stock_matches_movements(shop)


def test_thanks_page_confirms_without_waiting_for_the_webhook(app_client, shop, fake):
    session_id = start(app_client, ("JP-PACK-A", 1))
    fake.pay(session_id)
    page = app_client.get(f"/order/thanks?session_id={session_id}")
    assert page.status_code == 200 and "Thank you for your order" in page.text
    assert "Test order" in page.text
    assert page.headers["cache-control"] == "no-store"
    assert order_row(shop, number_of(fake, session_id))["status"] == "paid"


def test_order_page_needs_its_secret_link(app_client, shop, fake):
    session_id = start(app_client, ("JP-PACK-A", 1))
    order = order_row(shop, number_of(fake, session_id))
    assert app_client.get(f"/order/{order['number']}").status_code == 404
    assert app_client.get(f"/order/{order['number']}?k=wrong").status_code == 404
    assert app_client.get(f"/order/{order['number']}?k={order['access_token']}").status_code == 200


def test_returning_from_stripe_without_paying_releases_the_stock(app_client, shop, fake):
    session_id = start(app_client, ("JP-AE-BB", 2))
    cancel_url = fake.sessions[session_id]["params"]["cancel_url"]
    page = app_client.get(cancel_url.replace("http://testserver", ""))
    assert page.status_code == 200 and "Checkout cancelled" in page.text
    assert stock(shop, "JP-AE-BB") == 3
    assert fake.expired_calls == [session_id]
    assert_stock_matches_movements(shop)


def test_refund_and_dispute_are_recorded(app_client, shop, fake):
    session_id = start(app_client, ("JP-AE-BB", 1))
    send_event(app_client, {"id": "evt_p", "type": "checkout.session.completed",
                            "data": {"object": fake.pay(session_id)}})
    number, intent = number_of(fake, session_id), f"pi_{session_id}"
    send_event(app_client, {"id": "evt_r", "type": "charge.refunded", "data": {"object": {
        "payment_intent": intent, "amount": 8099, "amount_refunded": 1000}}})
    order = order_row(shop, number)
    assert (order["status"], order["refunded_cents"]) == ("partially_refunded", 1000)
    send_event(app_client, {"id": "evt_d", "type": "charge.dispute.created", "data": {"object": {
        "payment_intent": intent, "amount": 8099, "reason": "fraudulent"}}})
    assert order_row(shop, number)["status"] == "disputed"
    assert stock(shop, "JP-AE-BB") == 2        # refunds don't restock: returns are inspected first


# --- shipping ----------------------------------------------------------------------------

def test_shipping_an_order_emails_tracking(app_client, shop, fake):
    session_id = start(app_client, ("JP-PACK-A", 1))
    app_client.get(f"/order/thanks?session_id={session_id}")   # unpaid: stays pending
    number = number_of(fake, session_id)
    conn = connect(shop.db_path, writer=True)
    try:
        with pytest.raises(ValueError):
            orders.mark_shipped(conn, shop, number, "USPS", "9400", "https://shop.test")
        orders.mark_paid(conn, shop, fake.pay(session_id), "https://shop.test")
        order = orders.mark_shipped(conn, shop, number, "USPS", "9400111899223817461234", "https://shop.test")
    finally:
        conn.close()
    assert order["status"] == "shipped"
    shipped = [p.read_text() for p in shop.outbox_dir.glob("*.eml") if "has shipped" in p.read_text()]
    assert len(shipped) == 1 and "9400111899223817461234" in shipped[0]
    # The designed (HTML) version: logo attached to the email, tracking button to USPS.
    from email import policy
    from email.parser import BytesParser
    path = next(p for p in shop.outbox_dir.glob("*.eml") if "has shipped" in p.read_text())
    message = BytesParser(policy=policy.default).parsebytes(path.read_bytes())
    html = message.get_body(("html",)).get_content()
    assert "Your order is on its way" in html and "TEST ORDER" in html
    assert "https://tools.usps.com/go/TrackConfirmAction?tLabels=9400111899223817461234" in html
    logo = [part for part in message.walk() if part.get_content_type() == "image/png"]
    assert len(logo) == 1 and f'cid:{logo[0]["Content-ID"].strip("<>")}' in html
    assert "9400111899223817461234" in message.get_body(("plain",)).get_content()


# --- switches -----------------------------------------------------------------------------

def test_without_a_stripe_key_checkout_is_off(client):
    assert client.post("/checkout", json=cart(("JP-PACK-A", 1))).status_code == 503
    page = client.get("/p/pokemon-booster-pack-japanese").text
    assert "Buy on TCGplayer" in page and "Add to cart" not in page
    assert 'href="/cart"' not in page


def test_with_a_test_key_products_have_add_to_cart(app_client):
    page = app_client.get("/p/pokemon-booster-pack-japanese").text
    assert "Add to cart" in page and 'href="/cart"' in page
    assert app_client.get("/cart").status_code == 200


def test_checkout_switch_off_means_catalog_only_even_with_keys(shop, fake):
    # The public launch runs with STOREFRONT_CHECKOUT=0 while the test keys stay in .env.
    off = dataclasses.replace(shop, checkout_switch=False)
    assert not off.checkout_enabled
    with TestClient(create_app(off, stripe=fake), follow_redirects=False) as client:
        assert client.post("/checkout", json=cart(("JP-PACK-A", 1))).status_code == 503
        page = client.get("/p/pokemon-booster-pack-japanese").text
        assert "Buy on TCGplayer" in page and "Add to cart" not in page
    assert not fake.sessions


def test_checkout_switch_reads_storefront_checkout(monkeypatch):
    monkeypatch.setenv("STOREFRONT_CHECKOUT", "0")
    assert load_settings().checkout_switch is False
    monkeypatch.delenv("STOREFRONT_CHECKOUT")
    assert load_settings().checkout_switch is True


def test_live_keys_are_refused_unless_allowed(imported):
    live = dataclasses.replace(imported, stripe_secret_key="sk_live_x")
    assert not live.checkout_enabled
    assert dataclasses.replace(live, allow_live=True).checkout_enabled


def test_stripe_form_encoding():
    assert form_encode({"a": {"b": [1, {"c": True}]}, "n": None}) == [("a[b][0]", "1"), ("a[b][1][c]", "true")]
