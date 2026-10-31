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
    # Draft text pages stay out of the sitemap until the owner approves them.
    assert "https://shop.test/returns" not in locs


def test_product_structured_data(client):
    html = client.get("/p/black-bolt-booster-bundle-english").text
    blocks = jsonld_blocks(html)
    product = next(b for b in blocks if b["@type"] == "Product")
    assert product["sku"] == "ENG-BB-BB"
    assert product["offers"]["price"] == "79.99"
    assert product["offers"]["priceCurrency"] == "USD"
    assert product["offers"]["availability"] == "https://schema.org/InStock"
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
