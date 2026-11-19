"""The admin's Shipping page (Phase 3, step 3): the Pirate Ship file out, the tracking file back,
and each product's packed size. Uses the synthetic store from conftest.py; emails go to the
outbox folder (no email set up in tests)."""

from __future__ import annotations

import csv
import io
import re

import pytest

from storefront.admin import shipping
from storefront.app.db import connect

from storefront.tests.test_admin import admin, config, password_hash, sign_in  # noqa: F401
from storefront.tests.test_admin_orders import add_order, token
from storefront.tests.test_admin_products import a_product, fetch


@pytest.fixture()
def signed(admin):  # noqa: F811
    return sign_in(admin)


def link_item(settings, number: str, product_id: int, quantity: int = 1) -> None:
    """Point the order's one line at a real product (add_order's line has no product)."""
    with connect(settings.db_path) as conn:
        conn.execute("UPDATE order_items SET product_id = ?, quantity = ? WHERE order_id = "
                     "(SELECT id FROM orders WHERE number = ?)", (product_id, quantity, number))


def set_size(settings, product_id: int, weight=None, box=(None, None, None)) -> None:
    with connect(settings.db_path) as conn:
        conn.execute("UPDATE products SET weight_oz = ?, length_in = ?, width_in = ?, height_in = ? WHERE id = ?",
                     (weight, *box, product_id))


def ticked(page: str, number: str) -> bool:
    return bool(re.search(rf'value="{number}"[^>]*\bchecked\b', page))


def tracking_file(*rows, header=("Created Date", "Recipient", "Carrier", "Tracking Number", "Order ID", "Status")) -> bytes:
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(header)
    writer.writerows(rows)
    return out.getvalue().encode()


def test_pages_need_sign_in(admin, imported):  # noqa: F811
    assert admin.get("/shipping").headers["location"] == "/login"
    assert admin.post("/shipping/export", data={"order": "P6-2026-0006"}).headers["location"] == "/login"
    assert admin.post("/shipping/tracking/apply", data={"pick": "P6-2026-0006|USPS|9400111"}).headers["location"] == "/login"


def test_shipping_page_lists_paid_orders(signed, imported):
    product = a_product(imported)
    set_size(imported, product["id"], 26, (11, 7.5, 5.5))
    add_order(imported, "P6-2026-0006")
    link_item(imported, "P6-2026-0006", product["id"])
    add_order(imported, "P6-2026-0005", "shipped")
    add_order(imported, "P6T-2026-0002", mode="test")
    page = signed.get("/shipping").text
    assert "P6-2026-0006" in page and "P6-2026-0005" not in page and "P6T-2026-0002" in page
    assert "26 oz" in page and "11 x 7.5 x 5.5 in" in page and "Signature Confirmation" in page
    # Live orders are ticked; test orders aren't.
    assert ticked(page, "P6-2026-0006") and not ticked(page, "P6T-2026-0002")


def test_export_file_and_exported_mark(signed, imported):
    product = a_product(imported)
    set_size(imported, product["id"], 26, (11, 7.5, 5.5))
    add_order(imported, "P6-2026-0006")
    link_item(imported, "P6-2026-0006", product["id"])
    page = signed.get("/shipping").text
    response = signed.post("/shipping/export", data={"form_token": token(page), "order": ["P6-2026-0006", "junk"]})
    assert response.status_code == 200 and response.headers["content-type"].startswith("text/csv")
    assert 'attachment; filename="pirate-ship-' in response.headers["content-disposition"]
    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert len(rows) == 1
    row = rows[0]
    assert row["Order ID"] == "P6-2026-0006" and row["Name"] == "Misty Waterflower"
    assert row["Address"] == "2730g Teak wood Drive TX" and row["City"] == "garland" and row["State"] == "TX"
    assert row["Zipcode"] == "75044" and row["Country"] == "US"
    assert (row["Weight (oz)"], row["Length (in)"], row["Width (in)"], row["Height (in)"]) == ("26", "11", "7.5", "5.5")
    assert row["Rubber Stamp 1"] == "1x EN-AH-ETB"
    assert "Email" not in response.text and "misty@example.com" not in response.text
    # The order now says it went out in a file, and is no longer ticked by default.
    page = signed.get("/shipping").text
    assert "in a file" in page and not ticked(page, "P6-2026-0006")


def test_export_needs_a_ticked_order(signed, imported):
    add_order(imported, "P6-2026-0006")
    page = signed.get("/shipping").text
    response = signed.post("/shipping/export", data={"form_token": token(page)})
    assert response.status_code == 400 and "Tick at least one order" in response.text
    assert signed.post("/shipping/export", data={"form_token": "wrong", "order": "P6-2026-0006"}).status_code == 403


def test_weight_and_box_rules(imported):
    product = a_product(imported)
    add_order(imported, "P6-2026-0006")
    link_item(imported, "P6-2026-0006", product["id"], quantity=2)
    set_size(imported, product["id"], 26, (11, 7.5, 5.5))
    with connect(imported.db_path) as conn:
        order = shipping.unshipped(conn)[0]
    # Two units: weight adds up, the box is left for Pirate Ship.
    assert order["weight_oz"] == 52 and order["box"] is None and order["missing"] == []
    set_size(imported, product["id"], None)
    with connect(imported.db_path) as conn:
        orders = shipping.unshipped(conn)
    assert orders[0]["weight_oz"] is None and [i["product_id"] for i in shipping.missing_sizes(orders)] == [product["id"]]


def test_export_cells_never_start_a_formula():
    order = {"number": "P6-2026-0006", "name": "x", "shipping": {"name": "=HYPERLINK(1)", "address": {
        "line1": "+1 Main", "line2": "@home", "city": "Provo", "state": "UT", "postal_code": "84601", "country": "US"}},
        "box": None, "weight_oz": None, "stamp": "1x A"}
    row = shipping.export_rows([order])[0]
    assert row[1] == "HYPERLINK(1)" and row[3] == "1 Main" and row[4] == "home"


def test_tracking_upload_preview_and_apply(signed, imported):
    add_order(imported, "P6-2026-0006")
    add_order(imported, "P6-2026-0007", name="Brock Harrison", email="brock@example.com")
    add_order(imported, "P6-2026-0005", "shipped")
    add_order(imported, "P6-2026-0004", "refunded")
    data = tracking_file(
        ("11/19/2026", "Misty", "USPS", "9400 1112 0206 2132 4567 89", "P6-2026-0006", "Delivered"),
        ("11/19/2026", "Brock", "UPS", "1Z999AA10123456784", "Order P6-2026-0007", ""),
        ("11/19/2026", "Someone", "USPS", "9400111202062132000000", "TCG-12345", ""),     # another sale
        ("11/19/2026", "Gone", "USPS", "9400111202062132000001", "P6-2026-0004", ""),      # refunded order
        ("11/19/2026", "Void", "USPS", "9400111202062132000002", "P6-2026-0008", "Voided"),
        ("11/19/2026", "Excel", "USPS", "9.40011E+21", "P6-2026-0009", ""),
        ("11/19/2026", "Nobody", "USPS", "9400111202062132000003", "P6-2026-0099", ""),
    )
    page = signed.get("/shipping").text
    preview = signed.post("/shipping/tracking", data={"form_token": token(page)},
                          files={"file": ("shipments.csv", data, "text/csv")})
    assert preview.status_code == 200
    text = preview.text
    assert "Ready to mark shipped (2)" in text
    assert 'value="P6-2026-0006|USPS|9400111202062132456789"' in text
    assert 'value="P6-2026-0007|UPS|1Z999AA10123456784"' in text
    assert "the order is refunded" in text and "no such order in the store" in text
    assert "the label was voided" in text and "9.40011E+21" in text
    assert "1 label(s) for other sales" in text
    # Nothing has changed yet.
    with connect(imported.db_path) as conn:
        assert conn.execute("SELECT status FROM orders WHERE number = 'P6-2026-0006'").fetchone()[0] == "paid"
    response = signed.post("/shipping/tracking/apply", data={"form_token": token(text), "pick": [
        "P6-2026-0006|USPS|9400111202062132456789", "P6-2026-0005|USPS|9400111202062132999999",
        "bad|pick", "P6-2026-0006|Pigeon|9400111202062132456789"]})
    assert response.status_code == 303
    assert response.headers["location"] == "/shipping?done=shipped&n=1&failed=0&skipped=1"
    with connect(imported.db_path) as conn:
        row = conn.execute("SELECT status, carrier, tracking_number FROM orders WHERE number = 'P6-2026-0006'").fetchone()
        assert tuple(row) == ("shipped", "USPS", "9400111202062132456789")
        assert conn.execute("SELECT status FROM orders WHERE number = 'P6-2026-0007'").fetchone()[0] == "paid"
    assert len(list(imported.outbox_dir.glob("*.eml"))) == 1
    assert "Marked 1 order(s) shipped" in signed.get(response.headers["location"]).text


def test_tracking_file_problems(signed, imported):
    page = signed.get("/shipping").text
    def upload(data, name="shipments.csv"):
        return signed.post("/shipping/tracking", data={"form_token": token(page)}, files={"file": (name, data, "text/csv")})
    assert "No column called" in upload(b"Name,Order ID\nMisty,P6-2026-0006\n").text
    assert "Excel file" in upload(b"PK\x03\x04rest", "shipments.xlsx").text
    assert "bigger than 2 MB" in upload(b"x" * (shipping.MAX_UPLOAD_BYTES + 5)).text
    assert "The file is empty" in upload(b"").text or "Choose" in upload(b"").text


def test_two_labels_for_one_order_are_not_guessed(imported):
    add_order(imported, "P6-2026-0006")
    found = shipping.read_tracking(tracking_file(
        ("", "", "USPS", "9400111202062132000010", "P6-2026-0006", ""),
        ("", "", "USPS", "9400111202062132000011", "P6-2026-0006", ""),
        ("", "", "USPS", "9400111202062132000011", "P6-2026-0006", "")))
    with connect(imported.db_path) as conn:
        entries = shipping.check(conn, found["rows"])
    assert len(entries) == 1 and not entries[0]["ready"] and "two labels" in entries[0]["why"]


def test_carrier_from_tracking_when_no_column():
    found = shipping.read_tracking(tracking_file(("1Z999AA10123456784", "P6-2026-0006"), ("9400111202062132000010", "P6-2026-0007"),
                                                 header=("Tracking #", "Reference")))
    assert [(r["number"], r["carrier"]) for r in found["rows"]] == [("P6-2026-0006", "UPS"), ("P6-2026-0007", "USPS")]


def test_packed_size_on_product_page(signed, imported):
    product = a_product(imported)
    url = f"/products/{product['id']}"
    page = signed.get(url).text
    assert "Packed for shipping" in page
    response = signed.post(f"{url}/packed", data={"form_token": token(page), "weight": "1.5 lb", "length": "11",
                                                 "width": "7.5", "height": "5.5", "reason": "Weighed it"})
    assert response.status_code == 303 and response.headers["location"].endswith("done=packed#packed")
    saved = fetch(imported, product["id"])
    assert (saved["weight_oz"], saved["length_in"], saved["width_in"], saved["height_in"]) == (24, 11, 7.5, 5.5)
    page = signed.get(url).text
    assert "Packed size" in page and "24 oz, 11 x 7.5 x 5.5 in" in page and "Weighed it" in page
    # Same again: nothing logged.
    same = signed.post(f"{url}/packed", data={"form_token": token(page), "weight": "24", "length": "11",
                                             "width": "7.5", "height": "5.5"})
    assert same.headers["location"].endswith("done=same#packed")
    bad = signed.post(f"{url}/packed", data={"form_token": token(page), "weight": "heavy"})
    assert bad.status_code == 400 and "should be a number" in bad.text and 'value="heavy"' in bad.text
    partial = signed.post(f"{url}/packed", data={"form_token": token(page), "weight": "24", "length": "11"})
    assert partial.status_code == 400 and "all three box sides" in partial.text
