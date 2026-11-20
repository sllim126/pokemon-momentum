"""The admin's refunds and returns (Phase 3, step 4): refund through Stripe with the admin's own
key (a fake here), put returned units back, the refund email and the order note. Uses the
synthetic store from conftest.py; emails go to the outbox folder."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from storefront.admin.main import create_admin_app
from storefront.app.db import connect
from storefront.app.stripe_api import StripeError

from storefront.tests.test_admin import HOME, admin, config, password_hash, sign_in  # noqa: F401
from storefront.tests.test_admin_orders import add_order, token
from storefront.tests.test_admin_products import a_product, fetch, stock_ledger_matches
from storefront.tests.test_admin_shipping import link_item


class FakeStripe:
    def __init__(self) -> None:
        self.calls: list[tuple[dict, str]] = []
        self.fail = ""

    def create_refund(self, params: dict, idempotency_key: str) -> dict:
        if self.fail:
            raise StripeError(self.fail)
        self.calls.append((params, idempotency_key))
        return {"id": f"re_{len(self.calls)}", "status": "succeeded", "amount": params["amount"]}


@pytest.fixture()
def fake():
    return FakeStripe()


@pytest.fixture()
def signed(imported, config, fake):  # noqa: F811
    with TestClient(create_admin_app(imported, config, refund_client=fake), client=HOME,
                    follow_redirects=False) as client:
        yield sign_in(client)


def order_row(settings, number: str) -> dict:
    with connect(settings.db_path) as conn:
        return dict(conn.execute("SELECT * FROM orders WHERE number = ?", (number,)).fetchone())


def post(client, number: str, **data):
    page = client.get(f"/orders/{number}").text
    return client.post(f"/orders/{number}/refund", data={"form_token": token(page), **data})


def test_full_refund(signed, imported, fake):
    add_order(imported, "P6-2026-0006")
    page = signed.get("/orders/P6-2026-0006").text
    assert "Refund or return" in page and 'value="138.99"' in page
    response = post(signed, "P6-2026-0006", amount="138.99", reason="requested_by_customer", confirm="1",
                    note="Address never confirmed")
    assert response.status_code == 303 and "done=refunded_outbox" in response.headers["location"]
    params, key = fake.calls[0]
    assert params == {"payment_intent": "pi_123", "amount": 13899, "reason": "requested_by_customer",
                      "metadata": {"order": "P6-2026-0006", "source": "poke6s admin"}}
    assert key == "refund-P6-2026-0006-0-13899"
    order = order_row(imported, "P6-2026-0006")
    assert (order["status"], order["refunded_cents"]) == ("refunded", 13899)
    assert "Refunded $138.99 (customer asked (return or cancel)). Address never confirmed" in order["notes"]
    emails = list(imported.outbox_dir.glob("*.eml"))
    assert len(emails) == 1 and "refund of $138.99" in emails[0].read_text()
    # Nothing left to refund: no money form any more.
    assert 'name="amount"' not in signed.get("/orders/P6-2026-0006").text


def test_partial_refund_keeps_an_unshipped_order_to_ship(signed, imported, fake):
    add_order(imported, "P6-2026-0006")
    post(signed, "P6-2026-0006", amount="20", reason="", confirm="1")
    order = order_row(imported, "P6-2026-0006")
    assert (order["status"], order["refunded_cents"]) == ("paid", 2000)
    assert "reason" not in fake.calls[0][0]
    page = signed.get("/orders/P6-2026-0006").text
    assert "up to $118.99" in page and "Mark shipped" in page
    too_much = post(signed, "P6-2026-0006", amount="200", reason="", confirm="1")
    assert too_much.status_code == 400 and "At most $118.99 is left" in too_much.text
    assert len(fake.calls) == 1


def test_refund_needs_confirming_and_a_valid_amount(signed, imported, fake):
    add_order(imported, "P6-2026-0006")
    unticked = post(signed, "P6-2026-0006", amount="138.99", reason="requested_by_customer")
    assert unticked.status_code == 400 and "Tick the box to confirm the $138.99 refund" in unticked.text
    bad = post(signed, "P6-2026-0006", amount="lots", reason="requested_by_customer", confirm="1")
    assert bad.status_code == 400 and "a price. Type it like" in bad.text and 'value="lots"' in bad.text
    nothing = post(signed, "P6-2026-0006", amount="", reason="", confirm="1")
    assert nothing.status_code == 400 and "Type an amount" in nothing.text
    assert fake.calls == [] and order_row(imported, "P6-2026-0006")["refunded_cents"] == 0


def test_stripe_refusal_changes_nothing(signed, imported, fake):
    add_order(imported, "P6-2026-0006")
    fake.fail = "Charge ch_1 has already been refunded."
    response = post(signed, "P6-2026-0006", amount="10", reason="", confirm="1")
    assert response.status_code == 400 and "Stripe refused the refund: Charge ch_1" in response.text
    order = order_row(imported, "P6-2026-0006")
    assert (order["status"], order["refunded_cents"], order["notes"]) == ("paid", 0, "")


def test_return_puts_stock_back_once(signed, imported, fake):
    product = a_product(imported)
    add_order(imported, "P6-2026-0006", "shipped")
    link_item(imported, "P6-2026-0006", product["id"], quantity=2)
    before = fetch(imported, product["id"])["stock"]
    response = post(signed, "P6-2026-0006", amount="0", reason="", confirm="1", **{f"back_{product['id']}": "1"})
    assert response.headers["location"].endswith("done=put_back") and fake.calls == []
    assert fetch(imported, product["id"])["stock"] == before + 1 and stock_ledger_matches(imported)
    assert "Put back 1x EN-AH-ETB." in order_row(imported, "P6-2026-0006")["notes"]
    # One of the two sold is back: only one more can be.
    over = post(signed, "P6-2026-0006", amount="0", reason="", confirm="1", **{f"back_{product['id']}": "2"})
    assert over.status_code == 400 and "more units than this order" in over.text
    both = post(signed, "P6-2026-0006", amount="138.99", reason="requested_by_customer", confirm="1",
                **{f"back_{product['id']}": "1"})
    assert both.headers["location"].endswith("done=refunded_outbox")
    assert fetch(imported, product["id"])["stock"] == before + 2
    page = signed.get("/orders/P6-2026-0006").text
    assert "Refund or return" not in page          # fully refunded and everything back


def test_test_orders_and_disputes_are_not_refunded_here(signed, imported, fake):
    add_order(imported, "P6T-2026-0002", mode="test")
    assert "refund it in Stripe&#39;s test dashboard" in signed.get("/orders/P6T-2026-0002").text
    response = post(signed, "P6T-2026-0002", amount="10", reason="", confirm="1")
    assert response.status_code == 400 and fake.calls == []
    add_order(imported, "P6-2026-0007", "disputed")
    assert "answer the dispute in Stripe" in signed.get("/orders/P6-2026-0007").text


def test_without_the_key_refunds_say_so(admin, imported):  # noqa: F811
    client = sign_in(admin)
    add_order(imported, "P6-2026-0006")
    assert "Refunds aren" in client.get("/orders/P6-2026-0006").text
    response = post(client, "P6-2026-0006", amount="10", reason="", confirm="1")
    assert response.status_code == 400 and "STOREFRONT_ADMIN_STRIPE_KEY is missing" in response.text
