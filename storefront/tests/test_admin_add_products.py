"""The admin's add-product page, detail edits and photos (Phase 3, step 5b). Uses the synthetic
store from conftest.py and its sample market data (630431 = Black Bolt Booster Bundle, EN)."""

from __future__ import annotations

import io

import pytest
from PIL import Image

from storefront.app import product_details
from storefront.app.db import connect

from storefront.tests.test_admin import admin, config, password_hash, sign_in  # noqa: F401
from storefront.tests.test_admin_orders import token
from storefront.tests.test_admin_products import a_product, fetch, stock_ledger_matches

NEW = {"sku": "ENG-BB-BUNDLE2", "title": "Black Bolt Booster Bundle - English", "product_type": "booster-bundle",
       "language": "en", "set_name": "", "tcgplayer_ids": "630431", "gtin": "", "description_md": "Six packs.",
       "price": "95.99", "stock": "3"}


@pytest.fixture()
def signed(admin):  # noqa: F811
    return sign_in(admin)


def photo_bytes(color: str = "red", fmt: str = "PNG") -> bytes:
    out = io.BytesIO()
    Image.new("RGB", (60, 40), color).save(out, fmt)
    return out.getvalue()


def add(client, **overrides):
    page = client.get("/products/new").text
    return client.post("/products/new", data={"form_token": token(page), "action": "save", **NEW, **overrides})


def product_id_from(response) -> int:
    return int(response.headers["location"].split("/products/")[1].split("?")[0])


def test_new_page_needs_sign_in(admin, imported):  # noqa: F811
    assert admin.get("/products/new").headers["location"] == "/login"
    assert admin.post("/products/new", data=NEW).headers["location"] == "/login"


def count_products(settings) -> int:
    with connect(settings.db_path) as conn:
        return conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]


def test_check_fills_in_from_the_market_data(signed, imported):
    before = count_products(imported)
    page = signed.get("/products/new").text
    response = signed.post("/products/new", data={"form_token": token(page), "action": "check",
                                                  "tcgplayer_ids": "630431", "stock": "0"})
    assert response.status_code == 200
    assert "Found it" in response.text and "Black Bolt Booster Bundle - English" in response.text
    assert 'value="Black Bolt"' in response.text and 'value="100.00"' in response.text
    assert count_products(imported) == before   # nothing saved
    with connect(imported.db_path) as conn:
        listed = conn.execute("SELECT title FROM products WHERE '|' || tcgplayer_product_ids || '|' LIKE '%|630431|%'").fetchone()
    if listed:   # the synthetic store already sells it: the page says so
        assert "Already in the store" in response.text
    unknown = signed.post("/products/new", data={"form_token": token(page), "action": "check", "tcgplayer_ids": "123"})
    assert "not in the market data" in unknown.text and "Already in the store" not in unknown.text


def test_add_product_starts_hidden_with_stock_and_set(signed, imported, client):
    response = add(signed)
    assert response.status_code == 303 and response.headers["location"].endswith("done=created")
    product = fetch(imported, product_id_from(response))
    assert product["visible"] == 0 and product["stock"] == 3 and product["kind"] == "sealed"
    assert (product["set_code"], product["set_name"], product["set_slug"]) == ("sv", "Black Bolt", "black-bolt")
    assert product["ship_class"] == "small-sealed" and product["tcgplayer_product_ids"] == "630431"
    assert product["slug"] == "black-bolt-booster-bundle-english" or product["slug"].startswith("black-bolt-booster-bundle-english-")
    assert "black bolt" in product["search_text"]
    with connect(imported.db_path) as conn:
        category = conn.execute("SELECT c.slug FROM categories c JOIN product_categories pc ON pc.category_id = c.id "
                                "WHERE pc.product_id = ?", (product["id"],)).fetchone()[0]
    assert category == "booster-bundles"
    assert stock_ledger_matches(imported)
    assert client.get(f"/p/{product['slug']}").status_code == 404   # hidden until shown
    page = signed.get(response.headers["location"]).text
    assert "Product added" in page and "Added" in page and "ENG-BB-BUNDLE2" in page


def test_add_product_refuses_bad_input(signed, imported):
    existing = a_product(imported)
    for overrides, message in (({"sku": existing["sku"].lower()}, "already a product"),
                               ({"sku": "bad sku!"}, "The SKU is"),
                               ({"title": ""}, "Give the product a title"),
                               ({"product_type": "single"}, "Pick what kind"),
                               ({"price": "0"}, "between $0.50"),
                               ({"tcgplayer_ids": "abc"}, "are numbers"),
                               ({"gtin": "12"}, "8 to 14 digits"),
                               ({"stock": "-1"}, "whole number")):
        response = add(signed, **overrides)
        assert response.status_code == 400 and message in response.text, overrides
    with connect(imported.db_path) as conn:
        assert not conn.execute("SELECT 1 FROM products WHERE sku = 'ENG-BB-BUNDLE2'").fetchone()


def test_edit_details_logs_each_change(signed, imported):
    product_id = product_id_from(add(signed))
    page = signed.get(f"/products/{product_id}").text
    data = {"form_token": token(page), "title": "Black Bolt Bundle (6 packs) - English", "product_type": "booster-bundle",
            "language": "en", "set_name": "Black Bolt", "tcgplayer_ids": "630431", "gtin": "0820650850000",
            "description_md": "Six packs.", "reason": "tidier title"}
    response = signed.post(f"/products/{product_id}/details", data=data)
    assert response.status_code == 303 and response.headers["location"].endswith("done=details")
    after = fetch(imported, product_id)
    assert after["title"] == "Black Bolt Bundle (6 packs) - English" and after["gtin"] == "0820650850000"
    assert after["slug"].startswith("black-bolt-booster-bundle-english")   # the address never changes
    page = signed.get(f"/products/{product_id}").text
    assert "tidier title" in page and "Barcode" in page
    again = signed.post(f"/products/{product_id}/details", data=data)
    assert again.headers["location"].endswith("done=same")
    bad = signed.post(f"/products/{product_id}/details", data={**data, "product_type": "nonsense"})
    assert bad.status_code == 400 and "Pick what kind" in bad.text


def test_type_change_moves_category_and_shipping(imported):
    from storefront.app.market import MarketReference
    market = MarketReference(imported.market_reference_path)
    p = a_product(imported)
    fields = {"title": p["title"], "product_type": "accessory", "language": "", "set_name": "", "tcgplayer_ids": "",
              "gtin": "", "description_md": p["description_md"]}
    with connect(imported.db_path, writer=True) as conn:
        assert "product_type" in product_details.update_details(conn, p["id"], fields, market)
        row = conn.execute("SELECT kind, ship_class, language FROM products WHERE id = ?", (p["id"],)).fetchone()
        assert tuple(row) == ("accessory", "accessory", None)
        assert conn.execute("SELECT c.slug FROM categories c JOIN product_categories pc ON pc.category_id = c.id "
                            "WHERE pc.product_id = ?", (p["id"],)).fetchone()[0] == "accessories"


def test_photos_upload_main_and_remove(signed, imported, client):
    product_id = product_id_from(add(signed))
    page = signed.get(f"/products/{product_id}").text
    files = [("photos", ("a.png", photo_bytes("red"), "image/png")),
             ("photos", ("b.jpg", photo_bytes("blue", "JPEG"), "image/jpeg"))]
    response = signed.post(f"/products/{product_id}/photos", data={"form_token": token(page)}, files=files)
    assert response.status_code == 303 and "done=photos" in response.headers["location"]
    with connect(imported.db_path) as conn:
        shots = product_details.photos(conn, product_id)
    assert len(shots) == 2 and shots[0]["original_ext"] == "png"
    for shot in shots:
        assert (imported.images_dir / "480" / f"{shot['file_name']}.webp").exists()
        assert signed.get(f"/images/480/{shot['file_name']}.webp").status_code == 200
    # The same photo again is stored once.
    page = signed.get(f"/products/{product_id}").text
    response = signed.post(f"/products/{product_id}/photos", data={"form_token": token(page)},
                           files=[("photos", ("a.png", photo_bytes("red"), "image/png"))])
    assert "done=photo_same" in response.headers["location"]
    # Make the second the main photo, then remove the first.
    signed.post(f"/products/{product_id}/photos/{shots[1]['id']}/main", data={"form_token": token(page)})
    with connect(imported.db_path) as conn:
        assert product_details.photos(conn, product_id)[0]["id"] == shots[1]["id"]
    signed.post(f"/products/{product_id}/photos/{shots[0]['id']}/remove", data={"form_token": token(page)})
    with connect(imported.db_path) as conn:
        assert [p["id"] for p in product_details.photos(conn, product_id)] == [shots[1]["id"]]
    assert not (imported.images_dir / "480" / f"{shots[0]['file_name']}.webp").exists()
    assert (imported.images_dir / "original" / f"{shots[0]['file_name']}.png").exists()   # kept for undo
    page = signed.get(f"/products/{product_id}").text
    assert "Uploaded" in page and "Removed" in page and "Made the main photo" in page


def test_bad_photos_are_refused(signed, imported):
    product_id = product_id_from(add(signed))
    page = signed.get(f"/products/{product_id}").text
    for name, data in (("notes.txt", b"hello"), ("empty.png", b""), ("fake.jpg", b"\xff\xd8\xff not really")):
        response = signed.post(f"/products/{product_id}/photos", data={"form_token": token(page)},
                               files=[("photos", (name, data, "image/jpeg"))])
        assert response.status_code == 400, name
    gif = io.BytesIO()
    Image.new("RGB", (10, 10)).save(gif, "GIF")
    response = signed.post(f"/products/{product_id}/photos", data={"form_token": token(page)},
                           files=[("photos", ("a.gif", gif.getvalue(), "image/gif"))])
    assert response.status_code == 400 and "JPEG, PNG or WebP" in response.text
    with connect(imported.db_path) as conn:
        assert product_details.photos(conn, product_id) == []


def test_photo_actions_check_the_product(signed, imported):
    first = product_id_from(add(signed))
    second = product_id_from(add(signed, sku="ENG-OTHER-1"))
    page = signed.get(f"/products/{first}").text
    signed.post(f"/products/{first}/photos", data={"form_token": token(page)},
                files=[("photos", ("a.png", photo_bytes(), "image/png"))])
    with connect(imported.db_path) as conn:
        shot = product_details.photos(conn, first)[0]
    # A photo of one product can't be removed through another product's address.
    response = signed.post(f"/products/{second}/photos/{shot['id']}/remove", data={"form_token": token(page)})
    assert response.status_code == 400
    assert signed.post(f"/products/{first}/photos/{shot['id']}/nonsense",
                       data={"form_token": token(page)}).status_code == 404
    with connect(imported.db_path) as conn:
        assert len(product_details.photos(conn, first)) == 1


def test_parse_ids():
    assert product_details.parse_ids("630431, 630432|630431") == ["630431", "630432"]
    assert product_details.parse_ids("") == []
    with pytest.raises(ValueError):
        product_details.parse_ids("tcgplayer.com/product/630431")
