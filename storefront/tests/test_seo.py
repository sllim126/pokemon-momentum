"""Sitemap, canonical links, Open Graph and JSON-LD structured data."""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET


def jsonld_blocks(html: str) -> list[dict]:
    return [json.loads(b) for b in re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.S)]


def test_sitemap_lists_visible_products_and_pages(client):
    response = client.get("/sitemap.xml")
    assert response.headers["content-type"].startswith("application/xml")
    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    locs = [e.text for e in ET.fromstring(response.text).findall("s:url/s:loc", ns)]
    assert "https://shop.test/p/black-bolt-booster-bundle-english" in locs
    assert "https://shop.test/c/booster-boxes" in locs
    assert "https://shop.test/sets/black-bolt" in locs
    assert not any("surging-sparks-elite" in loc for loc in locs)  # hidden
    # Text pages are listed once the owner approves them (no "draft: true"; all were approved
    # 2026-11-09). A page marked draft would stay out of the sitemap.
    assert "https://shop.test/returns" in locs
    assert "https://shop.test/shipping" in locs


def test_product_structured_data(client):
    html = client.get("/p/black-bolt-booster-bundle-english").text
    blocks = jsonld_blocks(html)
    product = next(b for b in blocks if b["@type"] == "Product")
    assert product["sku"] == "ENG-BB-BB"
    assert product["offers"]["price"] == "79.99"
    assert product["offers"]["priceCurrency"] == "USD"
    # Checkout is off in this client (catalog mode): no "InStock" claim.
    assert "availability" not in product["offers"]
    assert product["offers"]["itemCondition"] == "https://schema.org/NewCondition"
    crumbs = next(b for b in blocks if b["@type"] == "BreadcrumbList")
    assert [i["name"] for i in crumbs["itemListElement"]][:3] == ["Home", "Shop", "Booster bundles"]


def test_home_has_organization(client):
    org = next(b for b in jsonld_blocks(client.get("/").text) if b["@type"] == "Organization")
    assert org["url"] == "https://shop.test/"


def test_canonical_and_open_graph(client):
    html = client.get("/p/black-bolt-booster-bundle-english?utm_source=x").text
    assert '<link rel="canonical" href="https://shop.test/p/black-bolt-booster-bundle-english">' in html
    assert '<meta property="og:type" content="product">' in html
    assert '<meta property="product:price:amount" content="79.99">' in html


def test_jsonld_cannot_close_script_tag():
    from storefront.app.seo import dump
    assert "</script>" not in dump({"name": "</script><script>alert(1)</script>"})


def test_footer_and_organization_list_the_social_accounts(client):
    home = client.get("/").text
    assert 'aria-label="Community"' in home and "https://discord.gg/AejcWHkCG" in home
    assert '"sameAs": [' in home and "https://www.youtube.com/@Poke6s_TCG" in home


def test_catalog_mode_hides_stock_and_checkout_mode_shows_it(imported):
    import dataclasses
    from fastapi.testclient import TestClient
    from storefront.app.main import create_app

    path = "/p/black-bolt-booster-bundle-english"
    with TestClient(create_app(imported)) as off:
        html = off.get(path).text
        assert "In stock" not in html and " left<" not in html
    on = dataclasses.replace(imported, stripe_secret_key="sk_test_fake", stripe_webhook_secret="whsec_x")
    with TestClient(create_app(on)) as shop:
        html = shop.get(path).text
        product = next(b for b in jsonld_blocks(html) if b["@type"] == "Product")
        assert product["offers"]["availability"] == "https://schema.org/InStock"
        assert "In stock" in html or " left<" in html


# --- Google Merchant Center feed ------------------------------------------------------------

G = "{http://base.google.com/ns/1.0}"


def feed_items(client) -> dict[str, dict]:
    response = client.get("/feeds/google.xml")
    assert response.status_code == 200 and response.headers["content-type"].startswith("application/xml")
    items = {}
    for item in ET.fromstring(response.text).find("channel").findall("item"):
        fields: dict = {}
        for child in item:
            name = child.tag.replace(G, "")
            if name == "shipping":
                fields[name] = {c.tag.replace(G, ""): c.text for c in child}
            elif name == "additional_image_link":
                fields.setdefault(name, []).append(child.text)
            else:
                fields[name] = child.text
        items[fields["id"]] = fields
    return items


def with_photos(settings, sku: str, *names: str) -> None:
    from storefront.app.db import connect
    with connect(settings.db_path, writer=True) as conn:
        product_id = conn.execute("SELECT id FROM products WHERE sku = ?", (sku,)).fetchone()[0]
        for n, name in enumerate(names):
            conn.execute("INSERT INTO product_images (product_id, file_name, sort_order) VALUES (?, ?, ?)",
                         (product_id, name, n))


def test_google_feed_lists_sellable_products_with_photos(imported):
    import dataclasses
    from fastapi.testclient import TestClient
    from storefront.app.main import create_app
    on = dataclasses.replace(imported, stripe_secret_key="sk_test_fake", stripe_webhook_secret="whsec_x")
    with_photos(on, "ENG-BB-BB", "bb-front", "bb-back")
    with_photos(on, "JP-AE-BB", "ae-front")
    with TestClient(create_app(on), follow_redirects=False) as client:
        items = feed_items(client)
    assert set(items) == {"ENG-BB-BB", "JP-AE-BB"}          # only products with a photo
    bundle = items["ENG-BB-BB"]
    assert bundle["link"] == "https://shop.test/p/black-bolt-booster-bundle-english"
    assert bundle["image_link"] == "https://shop.test/media/1200/bb-front.webp"
    assert bundle["additional_image_link"] == ["https://shop.test/media/1200/bb-back.webp"]
    assert bundle["price"] == "79.99 USD" and bundle["condition"] == "new" and bundle["brand"] == "Pokémon"
    assert bundle["identifier_exists"] == "no"                # no barcode saved
    assert bundle["shipping"] == {"country": "US", "service": "Standard", "price": "10.00 USD"}
    assert "*" not in bundle["description"] and bundle["availability"] in ("in_stock", "out_of_stock")


def test_google_feed_is_empty_while_checkout_is_off(client, imported):
    with_photos(imported, "ENG-BB-BB", "bb-front")
    assert feed_items(client) == {}                           # catalog mode: nothing for sale


def test_feed_shipping_and_plain_text(imported):
    from storefront.app import seo
    assert seo.feed_shipping_cents(imported, 15000, "large-sealed") == 0     # free at $150
    assert seo.feed_shipping_cents(imported, 14299, "oversize") == 2000
    assert seo.plain_text("**Six** packs.\n\n- [Black Bolt](https://x.test) set") == "Six packs. Black Bolt set"
