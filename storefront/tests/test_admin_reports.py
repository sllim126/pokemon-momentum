"""The admin's Reports page (Phase 3, step 7): the month's sales in the Utah worksheet's columns,
the Stripe fees and refunds for the gross-up (Stripe faked here), the Utah tax summary and the
"Sold on TCGplayer" stock changes. Uses the synthetic store from conftest.py."""

from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from storefront.admin import reports
from storefront.admin.main import create_admin_app
from storefront.app import product_changes
from storefront.app.db import connect
from storefront.app.stripe_api import StripeError

from storefront.tests.test_admin import HOME, admin, config, password_hash, sign_in  # noqa: F401
from storefront.tests.test_admin_products import a_product


def unix(iso: str) -> int:
    return int(datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).timestamp())


def add(settings, number: str, paid_at: str, *, state: str = "UT", city: str = "south jordan", tax: int = 0,
        refunded: int = 0, mode: str = "live", status: str = "paid", name: str = "Misty Waterflower") -> None:
    subtotal, shipping = 11899, 2000
    total = subtotal + shipping + tax
    address = {"name": name, "address": {"line1": "1 Cerulean Way", "city": city, "state": state,
                                         "postal_code": "84095", "country": "US"}}
    if refunded >= total:
        status = "refunded"
    with connect(settings.db_path) as conn:
        cur = conn.execute(
            "INSERT INTO orders (number, access_token, status, mode, email, name, shipping_json, ship_class, "
            "subtotal_cents, shipping_cents, tax_cents, total_cents, refunded_cents, stripe_payment_intent, "
            "created_at, paid_at) VALUES (?, 'tok', ?, ?, 'misty@example.com', ?, ?, 'large-sealed', ?, ?, ?, ?, ?, ?, ?, ?)",
            (number, status, mode, name, json.dumps(address), subtotal, shipping, tax, total, refunded,
             f"pi_{number}", paid_at, paid_at))
        conn.execute("INSERT INTO order_items (order_id, sku, title, unit_price_cents, quantity) "
                     "VALUES (?, 'EN-AH-ETB', 'Ascended Heroes Elite Trainer Box', 11899, 1)", (cur.lastrowid,))


class FakeStripe:
    """list_charges() answers from a dict of payment intent -> charges; create_refund unused."""

    def __init__(self) -> None:
        self.charges: dict[str, list[dict]] = {}
        self.fail = ""

    def charge(self, number: str, amount: int, at: str, fee: int, refunds: list[tuple[int, str]] = (),
               status: str = "succeeded") -> None:
        self.charges.setdefault(f"pi_{number}", []).append({
            "id": f"ch_{number}", "status": status, "amount": amount, "created": unix(at),
            "balance_transaction": {"fee": fee, "available_on": unix(at) + 2 * 86400},
            "refunds": {"data": [{"id": f"re_{number}_{i}", "amount": a, "status": "succeeded", "created": unix(when)}
                                 for i, (a, when) in enumerate(refunds)]}})

    def list_charges(self, payment_intent: str) -> list[dict]:
        if self.fail:
            raise StripeError(self.fail)
        return self.charges.get(payment_intent, [])

    def create_refund(self, params, idempotency_key):  # pragma: no cover - not used here
        raise AssertionError("no refunds from the Reports page")


@pytest.fixture()
def fake():
    return FakeStripe()


@pytest.fixture()
def signed(imported, config, fake):  # noqa: F811
    with TestClient(create_admin_app(imported, config, refund_client=fake), client=HOME,
                    follow_redirects=False) as client:
        yield sign_in(client)


def rows_of(text: str) -> list[dict]:
    return list(csv.DictReader(io.StringIO(text)))


def test_month_bounds_are_utah_months():
    # Nov 1 00:00 MDT is 06:00 UTC; Dec 1 00:00 MST is 07:00 UTC.
    assert reports.month_bounds("2026-11") == ("2026-11-01T06:00:00Z", "2026-12-01T07:00:00Z")
    assert reports.month_bounds("2026-12")[1] == "2027-01-01T07:00:00Z"
    assert reports.parse_month("2026-13", today=datetime(2026, 11, 22).date()) == "2026-11"
    assert reports.parse_month("nonsense'", today=datetime(2026, 11, 22).date()) == "2026-11"


def test_reports_need_sign_in(admin):  # noqa: F811
    for path in ("/reports", "/reports/sales.csv", "/reports/stripe.csv"):
        assert admin.get(path).headers["location"] == "/login"


def test_sales_csv_matches_the_worksheet(signed, imported):
    add(imported, "P6-2026-0010", "2026-11-05T18:00:00Z", tax=886)                      # Utah, taxed
    add(imported, "P6-2026-0011", "2026-11-06T18:00:00Z", state="TX", city="garland")   # out of state
    add(imported, "P6-2026-0012", "2026-11-07T18:00:00Z", refunded=13899, state="TX")   # refunded in full
    add(imported, "P6-2026-0013", "2026-11-08T18:00:00Z", mode="test")                  # test: left out
    add(imported, "P6-2026-0014", "2026-11-01T05:30:00Z")   # Oct 31 in Utah: October's
    add(imported, "P6-2026-0015", "2026-11-09T18:00:00Z", status="expired")              # never paid
    response = signed.get("/reports/sales.csv?month=2026-11")
    assert response.status_code == 200
    assert 'filename="poke6s-sales-2026-11.csv"' in response.headers["content-disposition"]
    assert response.text.splitlines()[0] == ",".join(reports.WORKSHEET_COLUMNS)
    rows = rows_of(response.text)
    assert [r["source"] for r in rows] == ["poke6s.com order P6-2026-0010", "poke6s.com order P6-2026-0011",
                                          "poke6s.com order P6-2026-0012"]
    utah, texas, refunded = rows
    assert (utah["date"], utah["channel"], utah["sale_amount"], utah["tax_charged"], utah["utah_taxable"],
            utah["delivery_city_state"], utah["how_paid"]) == (
        "2026-11-05", "poke6s.com", "138.99", "8.86", "yes", "South Jordan, UT", "Stripe")
    assert utah["items"] == "Ascended Heroes Elite Trainer Box"
    assert utah["notes"] == "items 118.99, shipping 20.00"
    assert (texas["utah_taxable"], texas["delivery_city_state"]) == ("no", "Garland, TX")
    assert (refunded["sale_amount"], refunded["utah_taxable"], refunded["delivery_city_state"]) == (
        "0.00", "no (refunded)", "Fully refunded")
    assert [r["source"][-4:] for r in rows_of(signed.get("/reports/sales.csv?month=2026-10").text)] == ["0014"]


def test_page_shows_utah_summary_and_tcgplayer_sales(signed, imported):
    add(imported, "P6-2026-0010", "2026-11-05T18:00:00Z", tax=886)
    add(imported, "P6-2026-0011", "2026-11-06T18:00:00Z", state="TX")
    add(imported, "P6-2026-0012", "2026-11-07T18:00:00Z", tax=886, refunded=500)   # partly refunded
    add(imported, "P6-2026-0009", "2026-10-07T18:00:00Z", tax=886)                 # earlier in the year
    product = a_product(imported)
    with connect(imported.db_path) as conn:
        product_changes.adjust_stock(conn, product["id"], "sold_elsewhere", 1, "order 4F2A",
                                     now=datetime(2026, 11, 10, 18, tzinfo=UTC))
    with connect(imported.db_path) as conn:
        month = reports.utah_summary(conn, *reports.month_bounds("2026-11"))
        year = reports.utah_summary(conn, *reports.year_bounds("2026-11"))
        sold = reports.tcgplayer_sales(conn, "2026-11")
    assert (month["utah_orders"], month["utah_sales_cents"], month["utah_tax_cents"]) == (2, 27798, 1772)
    assert month["other_sales_cents"] == 13899 and month["partial"] == ["P6-2026-0012"]
    assert (year["utah_orders"], year["utah_tax_cents"]) == (3, 2658)
    assert [(s["sku"], s["units"], s["note"], s["date"]) for s in sold] == [(product["sku"], 1, "order 4F2A", "2026-11-10")]
    page = signed.get("/reports?month=2026-11").text
    assert "Orders paid in 2026-11 (3)" in page and "$17.72" in page and "$26.58" in page
    assert "order 4F2A" in page and "Partly refunded" in page and 'href="/reports/stripe.csv?month=2026-11"' in page


def test_stripe_csv_has_fees_refunds_and_totals(signed, imported, fake):
    add(imported, "P6-2026-0010", "2026-11-05T18:00:00Z", tax=886)
    fake.charge("P6-2026-0010", 14785, "2026-11-05T18:00:00Z", fee=459)
    # Paid in October, refunded in November: only the refund belongs to November.
    add(imported, "P6-2026-0009", "2026-10-20T18:00:00Z", refunded=13899)
    fake.charge("P6-2026-0009", 13899, "2026-10-20T18:00:00Z", fee=433, refunds=[(13899, "2026-11-21T18:00:00Z")])
    # A declined attempt on the same payment moved no money.
    fake.charge("P6-2026-0010", 14785, "2026-11-05T17:55:00Z", fee=0, status="failed")
    response = signed.get("/reports/stripe.csv?month=2026-11")
    assert response.status_code == 200 and 'poke6s-stripe-2026-11.csv' in response.headers["content-disposition"]
    rows = rows_of(response.text)
    sale, refund, total = rows
    assert (sale["order"], sale["type"], sale["gross"], sale["tax"], sale["shipping"], sale["stripe_fee"], sale["net"],
            sale["expected_payout_date"]) == ("P6-2026-0010", "sale", "147.85", "8.86", "20.00", "4.59", "143.26",
                                              "2026-11-07")
    assert (refund["date"], refund["order"], refund["type"], refund["gross"], refund["stripe_fee"], refund["net"]) == (
        "2026-11-21", "P6-2026-0009", "refund", "-138.99", "0.00", "-138.99")
    assert total["date"] == "TOTAL" and total["net"] == "4.27" and total["stripe_fee"] == "4.59"
    assert total["type"] == "sales 147.85, refunds -138.99"
    october = rows_of(signed.get("/reports/stripe.csv?month=2026-10").text)
    assert [(r["order"], r["type"]) for r in october[:-1]] == [("P6-2026-0009", "sale")]


def test_stripe_trouble_sends_back_to_the_page(signed, imported, fake):
    add(imported, "P6-2026-0010", "2026-11-05T18:00:00Z")
    fake.fail = "Permission denied"
    response = signed.get("/reports/stripe.csv?month=2026-11")
    assert response.status_code == 303 and response.headers["location"] == "/reports?month=2026-11&error=stripe"
    assert "didn't answer the fee lookup" in signed.get(response.headers["location"]).text


def test_without_the_admin_key_there_is_no_stripe_download(imported, config):  # noqa: F811
    with TestClient(create_admin_app(imported, config), client=HOME, follow_redirects=False) as client:
        sign_in(client)
        assert "/reports/stripe.csv" not in client.get("/reports").text
        assert client.get("/reports/stripe.csv?month=2026-11").headers["location"].endswith("error=nokey")
