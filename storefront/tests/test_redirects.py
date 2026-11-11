"""301s from old Squarespace addresses."""

from __future__ import annotations

from storefront.app import redirects


def test_csv_redirects(client):
    for old, new in [("/faq-1", "/faq"), ("/new-page", "/terms"), ("/FAQ-1/", "/faq"),
                     ("/shop/booster-box/japanese", "/c/booster-boxes?lang=ja")]:
        response = client.get(old)
        assert response.status_code == 301, old
        assert response.headers["location"] == new


def test_trailing_slash_is_one_301(client):
    response = client.get("/shop/?lang=ja")
    assert response.status_code == 301 and response.headers["location"] == "/shop?lang=ja"


def test_trailing_slash_redirect_stays_on_this_site(client):
    # "//host/" or "/\\host/" as a Location header would send the browser to another site.
    for path in ("//example.com/", "///example.com/", "/\\example.com/", "//example.com/a/"):
        # Full URL, so the test client doesn't read "//example.com" as the host.
        response = client.get("http://testserver" + path)
        location = response.headers.get("location", "")
        assert response.status_code == 301, path
        assert location.startswith("/") and not location.startswith(("//", "/\\")), location


def test_product_redirects_follow_current_slug(client):
    response = client.get("/shop/p/5svl6sjep9plwrs2tqi7988h0xrmhz")
    assert response.status_code == 301
    assert response.headers["location"] == "/p/black-bolt-booster-bundle-english"


def test_hidden_or_unknown_products_go_to_shop(client):
    assert client.get("/shop/p/hidden-etb").headers["location"] == "/shop"
    assert client.get("/shop/p/roaring-moon-sar").headers["location"] == "/shop"  # single, not imported


def test_shop_tag_links(client):
    assert client.get("/shop?tag=Japanese").headers["location"] == "/shop?lang=ja"
    assert client.get("/shop?tag=Booster Bundle").headers["location"] == "/shop?type=booster-bundle"
    assert client.get("/shop?tag=Singles").headers["location"] == "/shop"


def test_every_csv_target_is_a_real_route():
    """Each redirect target must start with one of the app's address patterns."""
    prefixes = ("/shop", "/c/", "/sets", "/p/", "/faq", "/terms", "/returns", "/privacy", "/about",
                "/contact", "/shipping", "/notify")
    for old, new in redirects.load().items():
        assert new == "/" or new.startswith(prefixes), (old, new)
        assert redirects.normalize(old) == old


def test_www_redirects_to_the_public_address(imported):
    import dataclasses
    from fastapi.testclient import TestClient
    from storefront.app.main import create_app
    live = dataclasses.replace(imported, base_url="https://poke6s.com")
    with TestClient(create_app(live), follow_redirects=False) as client:
        moved = client.get("/shop?sort=new", headers={"host": "www.poke6s.com"})
        assert moved.status_code == 301
        assert moved.headers["location"] == "https://poke6s.com/shop?sort=new"
        assert client.get("/healthz", headers={"host": "poke6s.com"}).status_code == 200
