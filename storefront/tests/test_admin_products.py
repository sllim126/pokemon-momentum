"""The admin's Products pages (Phase 3, step 5): the list, one product's page, price changes,
show/hide, and stock (received, sold on TCGplayer, counted), each logged with a reason. Uses the
synthetic store from conftest.py."""

from __future__ import annotations

import pytest

from storefront.app import product_changes
from storefront.app.db import connect

# Fixtures and helpers shared with the step 1 and 2 tests.
from storefront.tests.test_admin import admin, config, password_hash, sign_in  # noqa: F401
from storefront.tests.test_admin_orders import token


@pytest.fixture()
def signed(admin):  # noqa: F811
    return sign_in(admin)


def a_product(settings, **where) -> dict:
    """A counted product from the synthetic import (visible and in stock unless told otherwise)."""
    conds = " AND ".join(f"{key} = ?" for key in where) or "1 = 1"
    with connect(settings.db_path) as conn:
        row = conn.execute(f"SELECT * FROM products WHERE track_stock = 1 AND {conds} ORDER BY id LIMIT 1",
                           list(where.values())).fetchone()
    assert row is not None
    return dict(row)


def fetch(settings, product_id: int) -> dict:
    with connect(settings.db_path) as conn:
        return dict(conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone())


def stock_ledger_matches(settings) -> bool:
    """products.stock must equal the sum of its stock_movements (the store's stock rule)."""
    with connect(settings.db_path) as conn:
        return not conn.execute(
            "SELECT 1 FROM products p WHERE p.stock != (SELECT coalesce(sum(change), 0) FROM stock_movements m "
            "WHERE m.product_id = p.id)").fetchone()


def post(client, path: str, **data):
    page = client.get(path.rsplit("/", 1)[0]).text
    return client.post(path, data={"form_token": token(page), **data})


def test_products_pages_need_sign_in(admin, imported):  # noqa: F811
    p = a_product(imported)
    for path in ("/products", f"/products/{p['id']}"):
        assert admin.get(path).headers["location"] == "/login"
    assert admin.post(f"/products/{p['id']}/price", data={"price": "1.00"}).headers["location"] == "/login"
    assert admin.post(f"/products/{p['id']}/stock", data={"kind": "received", "quantity": "1"}).headers["location"] == "/login"


def test_list_tabs_and_search(signed, imported):
    p = a_product(imported, visible=1)
    page = signed.get("/products").text
    assert p["title"] in page and "On the store" in page and "Hidden" in page
    assert p["title"] in signed.get(f"/products?q={p['sku']}").text
    assert "No products" in signed.get("/products?q=zzzz-nothing").text
    assert signed.get("/products?view=nonsense").status_code == 200
    assert 'href="/products"' in signed.get("/").text   # in the top menu


def test_product_page(signed, imported):
    p = a_product(imported)
    page = signed.get(f"/products/{p['id']}").text
    assert p["title"] in page and "Save price" in page and "Save stock" in page
    assert signed.get("/products/999999").status_code == 404
    assert signed.get("/products/abc").status_code == 404


def test_price_change_is_logged_with_reason(signed, imported):
    p = a_product(imported, visible=1)
    new = p["price_cents"] + 100
    path = f"/products/{p['id']}/price"
    # A reason is required.
    response = post(signed, path, price=f"{new / 100:.2f}", reason="")
    assert response.status_code == 400 and "Say why" in response.text
    assert fetch(imported, p["id"])["price_cents"] == p["price_cents"]
    response = post(signed, path, price=f"${new / 100:,.2f}", reason="5% under market")
    assert response.status_code == 303 and response.headers["location"].endswith("done=price")
    assert fetch(imported, p["id"])["price_cents"] == new
    page = signed.get(response.headers["location"]).text
    assert "Price saved" in page and "5% under market" in page and f"${new / 100:,.2f}" in page


def test_big_price_change_needs_confirming(signed, imported):
    p = a_product(imported, visible=1)
    path = f"/products/{p['id']}/price"
    typo = f"{p['price_cents']:d}"   # cents typed as dollars: 100x
    response = post(signed, path, price=typo, reason="new price")
    assert response.status_code == 400 and "big change" in response.text and 'name="confirm"' in response.text
    assert fetch(imported, p["id"])["price_cents"] == p["price_cents"]
    response = post(signed, path, price=typo, reason="new price", confirm="1")
    assert response.status_code == 303 and fetch(imported, p["id"])["price_cents"] == p["price_cents"] * 100


def test_bad_prices_are_refused(signed, imported):
    p = a_product(imported)
    path = f"/products/{p['id']}/price"
    for price, compare_at, message in (("abc", "", "Type it like 12.99"), ("0.10", "", "between $0.50"),
                                       ("10.00", "9.00", "must be higher")):
        response = post(signed, path, price=price, compare_at=compare_at, reason="test")
        assert response.status_code == 400 and message in response.text, price
    assert fetch(imported, p["id"])["price_cents"] == p["price_cents"]


def test_parse_money():
    assert product_changes.parse_money("12.99") == 1299
    assert product_changes.parse_money("$1,299") == 129900
    assert product_changes.parse_money("12.5") == 1250
    assert product_changes.parse_money(" ") is None
    for bad in ("12.999", "1,29", "-5", "12,99"):
        with pytest.raises(ValueError):
            product_changes.parse_money(bad)


def test_hide_and_show(signed, imported, client):
    p = a_product(imported, visible=1)
    assert client.get(f"/p/{p['slug']}").status_code == 200
    path = f"/products/{p['id']}/settings"
    response = post(signed, path, visible="0", featured="0", max_per_order="", reason="Keeping this one")
    assert response.status_code == 303 and fetch(imported, p["id"])["visible"] == 0
    assert client.get(f"/p/{p['slug']}").status_code == 404
    page = signed.get(f"/products/{p['id']}").text
    assert "Keeping this one" in page and "yes → no" in page
    post(signed, path, visible="1", featured="1", max_per_order="2")
    after = fetch(imported, p["id"])
    assert (after["visible"], after["featured"], after["max_per_order"]) == (1, 1, 2)
    response = post(signed, path, visible="1", featured="1", max_per_order="0")
    assert response.status_code == 400 and "per-order limit" in response.text


def test_stock_received_sold_and_counted(signed, imported):
    p = a_product(imported)
    path = f"/products/{p['id']}/stock"
    start = p["stock"]
    assert post(signed, path, kind="received", quantity="3", note="Costco run").status_code == 303
    assert fetch(imported, p["id"])["stock"] == start + 3
    assert post(signed, path, kind="sold_elsewhere", quantity="1").status_code == 303
    assert fetch(imported, p["id"])["stock"] == start + 2
    response = post(signed, path, kind="sold_elsewhere", quantity=str(start + 50))
    assert response.status_code == 400 and "have sold" in response.text
    assert post(signed, path, kind="counted", quantity="1", note="one dented, kept").status_code == 303
    assert fetch(imported, p["id"])["stock"] == 1
    response = post(signed, path, kind="counted", quantity="1")
    assert response.headers["location"].endswith("done=count_same")
    for kind, quantity in (("received", "0"), ("received", "x"), ("nonsense", "1"), ("received", "-2")):
        assert post(signed, path, kind=kind, quantity=quantity).status_code == 400
    page = signed.get(f"/products/{p['id']}").text
    assert "Costco run" in page and "Sold on TCGplayer" in page and "one dented, kept" in page
    assert stock_ledger_matches(imported)


def test_count_leaves_checkout_units_alone(imported):
    """3 on the shelf with 1 in an open checkout means 2 available, not 3."""
    p = a_product(imported)
    with connect(imported.db_path, writer=True) as conn:
        conn.execute("UPDATE products SET stock = stock - 1 WHERE id = ?", (p["id"],))
        conn.execute("INSERT INTO stock_movements (product_id, change, reason, reference, at) "
                     "VALUES (?, -1, 'reservation', 'P6-2026-0099', '2026-11-19T00:00:00Z')", (p["id"],))
        order = conn.execute("INSERT INTO orders (number, access_token, status, mode, ship_class, subtotal_cents, "
                             "shipping_cents, total_cents, created_at) VALUES "
                             "('P6-2026-0099', 't', 'pending', 'live', 'large-sealed', 100, 0, 100, "
                             "'2026-11-19T00:00:00Z')").lastrowid
        conn.execute("INSERT INTO order_items (order_id, product_id, sku, title, unit_price_cents, quantity, reserved) "
                     "VALUES (?, ?, ?, ?, 100, 1, 1)", (order, p["id"], p["sku"], p["title"]))
        assert product_changes.held(conn, p["id"]) == 1
        product_changes.adjust_stock(conn, p["id"], "counted", 3)
        assert conn.execute("SELECT stock FROM products WHERE id = ?", (p["id"],)).fetchone()[0] == 2
        with pytest.raises(ValueError, match="in a checkout"):
            product_changes.adjust_stock(conn, p["id"], "counted", 0)
    assert stock_ledger_matches(imported)


def test_uncounted_products_refuse_stock_changes(imported):
    with connect(imported.db_path) as conn:
        row = conn.execute("SELECT id FROM products WHERE track_stock = 0 LIMIT 1").fetchone()
    if row is None:
        pytest.skip("no uncounted product in the synthetic store")
    with connect(imported.db_path, writer=True) as conn, pytest.raises(ValueError, match="isn't stock-counted"):
        product_changes.adjust_stock(conn, row["id"], "received", 1)


def test_expired_form_token_is_refused(signed, imported):
    p = a_product(imported)
    response = signed.post(f"/products/{p['id']}/price", data={"form_token": "nope", "price": "9.99", "reason": "x"})
    assert response.status_code == 403
    assert fetch(imported, p["id"])["price_cents"] == p["price_cents"]


def test_keep_this_price_switch(signed, imported):
    # Ticked by default, so a typed price isn't put back by the nightly repricing (migration 011).
    p = a_product(imported, visible=1)
    path = f"/products/{p['id']}/price"
    page = signed.get(f"/products/{p['id']}").text
    assert 'name="price_manual" value="1"\n          checked' in page and "Nightly repricing (03:30 UTC)" in page
    new = p["price_cents"] + 100
    post(signed, path, price=f"{new / 100:.2f}", reason="hand price", price_manual="1")
    assert fetch(imported, p["id"])["price_manual"] == 1
    assert "Priced by hand:" in signed.get(f"/products/{p['id']}").text
    # Unticked, same price: the product goes back to its rule, and the history says so.
    response = post(signed, path, price=f"{new / 100:.2f}", reason="back to the rule")
    assert response.status_code == 303 and fetch(imported, p["id"])["price_manual"] == 0
    with connect(imported.db_path) as conn:
        fields = [r["field"] for r in conn.execute("SELECT field FROM product_changes WHERE product_id = ? ORDER BY id",
                                                   (p["id"],))]
    assert fields.count("price_manual") == 2
