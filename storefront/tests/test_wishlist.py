"""The wishlist (one Poke6s account, stage 2; app/wishlist.py): the ♡ on product pages, the list
on /account, sign-in when signed out, and clean-up when the account is deleted on the market."""

from __future__ import annotations

import sqlite3

from storefront.app import accounts, wishlist
from storefront.app.db import connect

from storefront.tests.test_accounts import market_db, now_iso
from storefront.tests.test_admin_products import a_product


def sign_in(client, settings, email="misty@example.com"):
    market_db(settings, {"good": (email, now_iso(5), now_iso(1))})
    client.cookies.set("poke6s_session", "good")


def test_signed_out_heart_goes_to_sign_in(client, imported):
    product = a_product(imported, visible=1)
    page = client.get(f"/p/{product['slug']}").text
    assert "♡ Save to wishlist" in page and f'href="{accounts.sign_in_url(imported)}"' in page
    response = client.post("/wishlist", data={"product_id": product["id"]})
    assert response.status_code == 303 and response.headers["location"] == accounts.sign_in_url(imported)


def test_save_list_and_remove(client, imported):
    sign_in(client, imported)
    product = a_product(imported, visible=1)
    response = client.post("/wishlist", data={"product_id": product["id"], "action": "add"})
    assert response.headers["location"] == f"/p/{product['slug']}"
    page = client.get(f"/p/{product['slug']}").text
    assert "♥ Saved to your wishlist" in page and 'name="action" value="remove"' in page
    account = client.get("/account").text
    assert "Your wishlist" in account and product["title"] in account
    response = client.post("/wishlist", data={"product_id": product["id"], "action": "remove", "back": "account"})
    assert response.headers["location"] == "/account#wishlist"
    assert "Nothing saved yet" in client.get("/account").text


def test_refuses_other_sites_and_unknown_products(client, imported):
    sign_in(client, imported)
    product = a_product(imported, visible=1)
    other = client.post("/wishlist", data={"product_id": product["id"]}, headers={"Origin": "https://evil.example"})
    assert other.status_code == 403
    assert client.post("/wishlist", data={"product_id": "abc"}).status_code == 400
    client.post("/wishlist", data={"product_id": 999999})
    with connect(imported.db_path) as conn:
        assert conn.execute("SELECT count(*) FROM wishlist").fetchone()[0] == 0


def test_deleted_accounts_lose_their_wishlist(imported):
    product = a_product(imported, visible=1)
    market_db(imported, {"good": ("misty@example.com", now_iso(5), now_iso(1))})
    with connect(imported.db_path) as conn:
        wishlist.add(conn, "misty@example.com", product["id"])
        wishlist.add(conn, "gone@example.com", product["id"])
        assert wishlist.drop_deleted_accounts(conn, imported) == 1
        assert [r[0] for r in conn.execute("SELECT email FROM wishlist")] == ["misty@example.com"]
    # An empty user table is a fault, not every account deleted: nothing goes.
    db = accounts.tracking_db(imported)
    market = sqlite3.connect(db)
    market.execute("DELETE FROM tracking_users")
    market.commit()
    market.close()
    with connect(imported.db_path) as conn:
        assert wishlist.drop_deleted_accounts(conn, imported) == 0
        assert conn.execute("SELECT count(*) FROM wishlist").fetchone()[0] == 1
