"""The admin's Orders pages (Phase 3, step 2): the list, one order's page, mark shipped, notes,
address corrections, resending emails and packing slips. Uses the synthetic store from
conftest.py; emails go to the outbox folder (no email set up in tests)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from storefront.app import orders
from storefront.app.db import connect
from storefront.admin import packing_slip

# Fixtures and helpers shared with the step 1 tests.
from storefront.tests.test_admin import admin, config, password_hash, sign_in  # noqa: F401

ADDRESS = {"name": "Misty Waterflower", "address": {"line1": "2730g Teak wood Drive TX", "line2": None,
                                                    "city": "garland", "state": "TX", "postal_code": "75044",
                                                    "country": "US"}}


def add_order(settings, number: str, status: str = "paid", *, mode: str = "live", hours_ago: float = 2,
              name: str = "Misty Waterflower", email: str = "misty@example.com") -> None:
    when = (datetime.now(UTC) - timedelta(hours=hours_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    paid = when if status not in ("pending", "expired", "cancelled") else None
    with connect(settings.db_path) as conn:
        cur = conn.execute(
            "INSERT INTO orders (number, access_token, status, mode, email, name, shipping_json, ship_class, "
            "subtotal_cents, shipping_cents, total_cents, stripe_payment_intent, created_at, paid_at) "
            "VALUES (?, 'tok', ?, ?, ?, ?, ?, 'large-sealed', 11899, 2000, 13899, 'pi_123', ?, ?)",
            (number, status, mode, email, name, json.dumps(ADDRESS), when, paid))
        conn.execute("INSERT INTO order_items (order_id, sku, title, unit_price_cents, quantity) "
                     "VALUES (?, 'EN-AH-ETB', 'Ascended Heroes Elite Trainer Box', 11899, 1)", (cur.lastrowid,))


def token(page_text: str) -> str:
    return page_text.split('name="form_token" value="')[1].split('"')[0]


@pytest.fixture()
def signed(admin):  # noqa: F811
    return sign_in(admin)


def test_orders_pages_need_sign_in(admin, imported):  # noqa: F811
    add_order(imported, "P6-2026-0006")
    for path in ("/orders", "/orders/P6-2026-0006", "/orders/P6-2026-0006/packing-slip.pdf"):
        assert admin.get(path).headers["location"] == "/login"
    assert admin.post("/orders/P6-2026-0006/ship", data={"tracking": "940012345678"}).headers["location"] == "/login"


def test_list_tabs_and_search(signed, imported):
    add_order(imported, "P6-2026-0006")
    add_order(imported, "P6-2026-0005", "shipped", name="Brock Harrison", email="brock@example.com")
    add_order(imported, "P6-2026-0004", "expired")
    page = signed.get("/orders").text
    assert "P6-2026-0006" in page and "P6-2026-0005" not in page and "Garland, TX" in page
    assert "P6-2026-0005" in signed.get("/orders?view=shipped").text
    everything = signed.get("/orders?view=all").text
    assert "P6-2026-0006" in everything and "P6-2026-0005" in everything and "P6-2026-0004" not in everything
    assert "P6-2026-0004" in signed.get("/orders?view=unfinished").text
    found = signed.get("/orders?view=all&q=BROCK").text
    assert "P6-2026-0005" in found and "P6-2026-0006" not in found
    assert signed.get("/orders?view=nonsense").status_code == 200       # falls back to "To ship"


def test_order_page(signed, imported):
    add_order(imported, "P6-2026-0006")
    page = signed.get("/orders/P6-2026-0006").text
    assert "2730g Teak wood Drive TX" in page and "Ascended Heroes Elite Trainer Box" in page
    assert "$138.99" in page and "Mark shipped" in page and "packing-slip.pdf" in page
    assert "https://dashboard.stripe.com/payments/pi_123" in page
    assert signed.get("/orders/P6-2026-9999").status_code == 404
    assert signed.get("/orders/not-an-order").status_code == 404


def test_mark_shipped_emails_tracking(signed, imported):
    add_order(imported, "P6-2026-0006")
    form = token(signed.get("/orders/P6-2026-0006").text)
    assert signed.post("/orders/P6-2026-0006/ship", data={"form_token": "wrong", "tracking": "9400111"}).status_code == 403
    bad = signed.post("/orders/P6-2026-0006/ship", data={"form_token": form, "carrier": "USPS", "tracking": "12-34"})
    assert bad.status_code == 400 and "tracking number" in bad.text
    other = signed.post("/orders/P6-2026-0006/ship", data={"form_token": form, "carrier": "Other", "tracking": "ABC12345678"})
    assert other.status_code == 400
    done = signed.post("/orders/P6-2026-0006/ship",
                       data={"form_token": form, "carrier": "USPS", "tracking": "9400 1118 9922 3817 4612 34"})
    assert done.status_code == 303 and "done=shipped" in done.headers["location"]
    with connect(imported.db_path, readonly=True) as conn:
        order = orders.get_order(conn, "P6-2026-0006")
    assert order["status"] == "shipped" and order["tracking_number"] == "9400111899223817461234"
    emails = list(imported.outbox_dir.glob("*.eml"))
    assert len(emails) == 1 and b"9400111899223817461234" in emails[0].read_bytes()
    page = signed.get(done.headers["location"]).text
    assert "Marked shipped" in page and "tools.usps.com" in page and "Resend tracking email" in page
    again = signed.post("/orders/P6-2026-0006/ship", data={"form_token": form, "carrier": "USPS", "tracking": "94001118992238"})
    assert again.status_code == 400 and "not paid" in again.text


def test_notes_are_dated_and_kept(signed, imported):
    add_order(imported, "P6-2026-0006")
    form = token(signed.get("/orders/P6-2026-0006").text)
    signed.post("/orders/P6-2026-0006/note", data={"form_token": form, "note": "Emailed to confirm the address"})
    signed.post("/orders/P6-2026-0006/note", data={"form_token": form, "note": "Customer confirmed"})
    assert signed.post("/orders/P6-2026-0006/note", data={"form_token": form, "note": "   "}).status_code == 400
    with connect(imported.db_path, readonly=True) as conn:
        notes = orders.get_order(conn, "P6-2026-0006")["notes"].split("\n")
    assert len(notes) == 2 and notes[0].endswith(": Emailed to confirm the address") and "M" in notes[0]


def test_address_correction_keeps_the_old_one(signed, imported):
    add_order(imported, "P6-2026-0006")
    form = token(signed.get("/orders/P6-2026-0006").text)
    new = {"form_token": form, "name": "Misty Waterflower", "line1": "2730 Woods Ln", "line2": "# G",
           "city": "Garland", "state": "tx", "postal_code": "75044-2808", "reason": "Customer confirmed by email"}
    missing = signed.post("/orders/P6-2026-0006/address", data={**new, "city": ""})
    assert missing.status_code == 400 and "Missing: city" in missing.text and "2730 Woods Ln" in missing.text
    assert "done=address" in signed.post("/orders/P6-2026-0006/address", data=new).headers["location"]
    with connect(imported.db_path, readonly=True) as conn:
        order = orders.get_order(conn, "P6-2026-0006")
    assert order["shipping"]["address"]["line1"] == "2730 Woods Ln" and order["shipping"]["address"]["state"] == "TX"
    assert order["shipping"]["address"]["country"] == "US"
    assert "Was: Misty Waterflower, 2730g Teak wood Drive TX, garland, TX 75044" in order["notes"]
    assert "Customer confirmed by email" in order["notes"]
    # Once shipped, the address is history.
    with connect(imported.db_path, writer=True) as conn:
        orders.mark_shipped(conn, imported, "P6-2026-0006", "USPS", "94001118992238", "https://shop.test")
    assert signed.post("/orders/P6-2026-0006/address", data=new).status_code == 400


def test_resend(signed, imported):
    add_order(imported, "P6-2026-0006")
    form = token(signed.get("/orders/P6-2026-0006").text)
    ok = signed.post("/orders/P6-2026-0006/resend", data={"form_token": form, "kind": "confirmed"})
    assert ok.status_code == 303 and "done=resent" in ok.headers["location"]
    assert len(list(imported.outbox_dir.glob("*.eml"))) == 1
    # No tracking yet, so no tracking email.
    assert signed.post("/orders/P6-2026-0006/resend", data={"form_token": form, "kind": "shipped"}).status_code == 400


def test_packing_slip(signed, imported):
    add_order(imported, "P6-2026-0006")
    add_order(imported, "P6-2026-0004", "expired")
    slip = signed.get("/orders/P6-2026-0006/packing-slip.pdf")
    assert slip.status_code == 200 and slip.headers["content-type"] == "application/pdf"
    assert slip.content.startswith(b"%PDF") and 'filename="packing-slip-P6-2026-0006.pdf"' in slip.headers["content-disposition"]
    gift = signed.get("/orders/P6-2026-0006/packing-slip.pdf?gift=1")
    assert gift.status_code == 200 and "gift" in gift.headers["content-disposition"]
    assert signed.get("/orders/P6-2026-0004/packing-slip.pdf").status_code == 404    # never a sale


def test_packing_slip_logo_prints_on_white():
    png, width, height = packing_slip.print_logo()
    from io import BytesIO
    from PIL import Image
    image = Image.open(BytesIO(png))
    assert image.mode == "RGBA" and (width, height) == image.size
    assert image.getpixel((2, 2))[3] == 0          # the navy corner is see-through


def test_overview_links_to_orders(signed, imported):
    add_order(imported, "P6-2026-0006")
    page = signed.get("/").text
    assert 'href="/orders/P6-2026-0006"' in page and 'href="/orders"' in page and "orders_cli ship" not in page


def test_packing_slip_tidies_a_lower_case_city():
    order = {"shipping": ADDRESS, "name": "x"}
    assert packing_slip._tidy_city(order)["shipping"]["address"]["city"] == "Garland"
    assert ADDRESS["address"]["city"] == "garland"          # the order itself is unchanged
