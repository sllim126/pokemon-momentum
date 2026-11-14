"""Public pages: filters, search, product page, market price, text pages, headers."""

from __future__ import annotations

import re


def product_links(html: str) -> list[str]:
    return re.findall(r'<li class="card">\s*<a href="(/p/[^"]+)"', html)


def test_health(client):
    body = client.get("/healthz").json()
    assert body["ok"] and body["visible_products"] == 7 and body["schema"] == "004_mailing_list_unsubscribe"


def test_home(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "Featured" in response.text and "New arrivals" in response.text


def test_shop_lists_only_visible(client):
    links = product_links(client.get("/shop").text)
    assert len(links) == 7
    assert "/p/pokemon-surging-sparks-elite-trainer-box-english" not in links


def test_language_and_type_filters(client):
    ja = product_links(client.get("/shop?lang=ja").text)
    assert set(ja) == {"/p/pokemon-abyss-eye-booster-box-japanese", "/p/pokemon-booster-pack-japanese",
                       "/p/pokemon-booster-pack-japanese-2"}
    boxes = product_links(client.get("/shop?type=booster-box").text)
    assert boxes == ["/p/pokemon-abyss-eye-booster-box-japanese"]
    # Unknown filter values are ignored rather than emptying the page.
    assert len(product_links(client.get("/shop?lang=xx&type=nope").text)) == 7


def test_set_filter_and_set_page(client):
    assert product_links(client.get("/shop?set=black-bolt").text) == ["/p/black-bolt-booster-bundle-english"]
    page = client.get("/sets/black-bolt")
    assert page.status_code == 200 and "Black Bolt" in page.text
    assert client.get("/sets/not-a-set").status_code == 404


def test_category_page(client):
    page = client.get("/c/accessories")
    assert page.status_code == 200
    assert sorted(product_links(page.text)) == ["/p/tcg-desk-mat-built-for-hits-binders-and-bulk-23-6-13-8",
                                                "/p/tcg-desk-mat-built-for-hits-binders-and-bulk-31-5-15-5"]
    assert client.get("/c/nope").status_code == 404


def test_search_matches_every_word_and_ignores_accents(client):
    assert product_links(client.get("/shop?q=abyss+box").text) == ["/p/pokemon-abyss-eye-booster-box-japanese"]
    assert len(product_links(client.get("/shop?q=pokémon+japanese").text)) == 3
    assert product_links(client.get("/shop?q=abyss+bundle").text) == []
    # LIKE wildcards are matched literally.
    assert product_links(client.get("/shop?q=%25").text) == []


def test_sorting(client):
    asc = product_links(client.get("/shop?sort=price-asc").text)
    desc = product_links(client.get("/shop?sort=price-desc").text)
    assert asc[0] == "/p/pokemon-booster-pack-japanese" and desc[0] == "/p/black-bolt-booster-bundle-english"


def test_product_page_shows_market_price_and_trend(client):
    page = client.get("/p/black-bolt-booster-bundle-english").text
    assert "Market price $100.00" in page
    assert "30 days +4.2%" in page
    assert "20% below market" in page
    assert "https://market.poke6s.com/?kind=sealed&amp;product=630431%7C%7CNormal" in page
    assert "Buy on TCGplayer" in page
    assert "$79.99" in page and "<s>$93.99</s>" in page


def test_market_price_averages_multi_id_listings(client):
    page = client.get("/p/poke-ball-tin-g25-english").text
    assert "Market price $25.00" in page and "averaged over 2 designs" in page


def test_product_description_is_escaped(client):
    page = client.get("/p/pokemon-abyss-eye-booster-box-japanese").text
    assert "<strong>30 packs</strong>" in page
    assert "<script>alert" not in page


def test_hidden_product_is_404(client):
    assert client.get("/p/pokemon-surging-sparks-elite-trainer-box-english").status_code == 404


def test_text_pages(client):
    page = client.get("/returns")
    assert page.status_code == 200 and "14 days" in page.text and "inspect" in page.text
    assert client.get("/no-such-page").status_code == 404


def test_not_indexed_before_launch(client):
    response = client.get("/")
    assert response.headers["X-Robots-Tag"] == "noindex, nofollow"
    assert '<meta name="robots" content="noindex, nofollow">' in response.text
    assert client.get("/robots.txt").text == "User-agent: *\nDisallow: /\n"
    assert "frame-ancestors 'none'" in response.headers["Content-Security-Policy"]


def test_media_rejects_bad_names(client):
    assert client.get("/media/480/..%2Fstore.db.webp").status_code == 404
    assert client.get("/media/999/x.webp").status_code == 404
