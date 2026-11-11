"""While checkout is off: the TCGplayer affiliate link, the "rebuilding" note and the
"email me when the store opens" list (owner, 2026-11-11)."""

from __future__ import annotations

import dataclasses
import sqlite3
from urllib.parse import parse_qs, urlparse

from fastapi.testclient import TestClient

from storefront.app import notify_list
from storefront.app.main import create_app

PRODUCT = "/p/black-bolt-booster-bundle-english"


def signups(settings) -> list[tuple[str, str]]:
    with sqlite3.connect(settings.db_path) as conn:
        return conn.execute("SELECT email, source FROM notify_signups ORDER BY id").fetchall()


def test_buy_on_tcgplayer_goes_to_the_product_through_the_affiliate_link(client):
    html = client.get(PRODUCT).text
    href = html.split('rel="sponsored noopener"')[0].rsplit('href="', 1)[1].split('"')[0]
    link = urlparse(href.replace("&amp;", "&"))
    assert f"{link.scheme}://{link.netloc}{link.path}" == "https://partner.tcgplayer.com/jR1OJb"
    assert parse_qs(link.query)["u"][0].startswith("https://www.tcgplayer.com/product/")
    assert "affiliate links" in html


def test_rebuilding_note_only_while_checkout_is_off(imported):
    with TestClient(create_app(imported)) as off:
        assert "We're rebuilding our store" in off.get("/").text
    on = dataclasses.replace(imported, stripe_secret_key="sk_test_fake", stripe_webhook_secret="whsec_x")
    with TestClient(create_app(on)) as shop:
        html = shop.get(PRODUCT).text
        assert "rebuilding" not in html and 'action="/notify"' not in html


def test_join_the_list(client, imported):
    response = client.post("/notify", data={"email": " Someone@Example.com ", "source": PRODUCT})
    assert response.status_code == 303 and response.headers["location"] == "/notify?joined=1"
    client.post("/notify", data={"email": "someone@example.com", "source": "/"})   # already on it
    assert signups(imported) == [("someone@example.com", PRODUCT)]
    assert "You're on the list" in client.get("/notify?joined=1").text


def test_bad_address_bot_trap_and_odd_source(client, imported):
    assert client.post("/notify", data={"email": "not-an-email"}).headers["location"] == "/notify?error=1"
    assert client.post("/notify", data={"email": "bot@example.com", "website": "x"}).status_code == 303
    client.post("/notify", data={"email": "a@example.com", "source": "//evil.example/"})
    assert signups(imported) == [("a@example.com", "")]


def test_sign_ups_are_rate_limited(client):
    codes = [client.post("/notify", data={"email": f"p{n}@example.com"}).status_code for n in range(7)]
    assert codes[:5] == [303] * 5 and codes[-1] == 429


def test_clean_email():
    assert notify_list.clean_email("A@B.co") == "a@b.co"
    for bad in ("", "a@b", "a b@c.com", "@b.com", "a@@b.com", "x" * 250 + "@b.com"):
        assert notify_list.clean_email(bad) is None
