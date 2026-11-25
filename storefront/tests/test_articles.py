"""Articles: loading, pages, old /resources redirects, sitemap, and the Squarespace post import."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from PIL import Image

from storefront.app import article_import
from storefront.app.articles import Articles, market_boxes, parse, tcgplayer_links
from storefront.app.main import create_app

NEWER = """---
title: Why I Built a Market Screener
date: 2026-03-19
description: Momentum, not last-sold prices.
author: Adam Mills
categories: Market Tools, Market Talk
tags: Price Momentum, Sealed
cover:
old_path: /resources/why-i-built-a-pokemon-market-screener
---
Intro paragraph.

![Chart](/media/1200/chart-0123456789.webp)

<script>alert(1)</script>
"""

OLDER = """---
title: Journey Together Master Set Checklist
date: 2025-12-15
description: Every card and promo.
categories: Guides
tags: Master Sets
cover: journey-abcdef0123
old_path: /resources/sv9
---
Checklist text.
"""

DRAFT = """---
title: Not Ready Yet
date: 2026-04-01
categories: Market Tools
draft: true
---
Work in progress.
"""


@pytest.fixture()
def articles_dir(tmp_path: Path) -> Path:
    folder = tmp_path / "articles"
    folder.mkdir()
    (folder / "why-i-built-a-market-screener.md").write_text(NEWER, encoding="utf-8")
    (folder / "journey-together-master-set-checklist.md").write_text(OLDER, encoding="utf-8")
    (folder / "not-ready-yet.md").write_text(DRAFT, encoding="utf-8")
    return folder


@pytest.fixture()
def blog_client(imported, articles_dir):
    with TestClient(create_app(imported, articles_dir=articles_dir), follow_redirects=False) as client:
        yield client


def test_parse_front_matter_and_cover_fallback():
    article = parse("x", NEWER)
    assert article.categories == ("Market Tools", "Market Talk")
    assert article.tags == ("Price Momentum", "Sealed")
    # No cover given: the first picture in the body is used.
    assert article.cover == "chart-0123456789" and article.cover_in_body
    assert 'loading="lazy"' in article.html
    # Raw HTML in the Markdown is escaped, never run.
    assert "<script>" not in article.html and "&lt;script&gt;" in article.html
    assert article.minutes == 1 and not article.draft


def test_listing_order_drafts_and_filters(articles_dir):
    articles = Articles(articles_dir)
    assert [a.slug for a in articles.published()] == ["why-i-built-a-market-screener",
                                                     "journey-together-master-set-checklist"]
    assert [a.slug for a in articles.filtered(category="market tools")] == ["why-i-built-a-market-screener"]
    assert [a.slug for a in articles.filtered(tag="Master Sets")] == ["journey-together-master-set-checklist"]
    # Draft articles don't count towards the category list.
    assert dict(articles.categories())["Market Tools"] == 1


def test_old_blog_addresses(articles_dir):
    articles = Articles(articles_dir)
    assert articles.redirect_for("/resources") == "/articles"
    assert articles.redirect_for("/resources/sv9/") == "/articles/journey-together-master-set-checklist"
    assert articles.redirect_for("/Resources/SV9") == "/articles/journey-together-master-set-checklist"
    assert articles.redirect_for("/resources/category/Market+Tools") == "/articles?category=Market%20Tools"
    assert articles.redirect_for("/resources/tag/Pok%C3%A9mon+TCG") == "/articles?tag=Pok%C3%A9mon%20TCG"
    assert articles.redirect_for("/resources/gone-post") == "/articles"
    assert articles.redirect_for("/shop") is None


def test_article_pages(blog_client):
    listing = blog_client.get("/articles")
    assert listing.status_code == 200
    assert "Why I Built a Market Screener" in listing.text and "Not Ready Yet" not in listing.text
    assert listing.text.index("Why I Built") < listing.text.index("Journey Together")

    filtered = blog_client.get("/articles?category=Guides").text
    assert "Journey Together" in filtered and "Why I Built" not in filtered

    page = blog_client.get("/articles/journey-together-master-set-checklist")
    assert page.status_code == 200
    assert '"@type": "BlogPosting"' in page.text and '"datePublished": "2025-12-15"' in page.text
    assert '<meta property="og:type" content="article">' in page.text
    assert "/media/1200/journey-abcdef0123.webp" in page.text   # social card from the cover
    assert "December 15, 2025" in page.text

    draft = blog_client.get("/articles/not-ready-yet")
    assert draft.status_code == 200 and "Draft" in draft.text
    assert blog_client.get("/articles/no-such-article").status_code == 404


def test_redirects_sitemap_and_home(blog_client):
    response = blog_client.get("/resources/sv9")
    assert response.status_code == 301
    assert response.headers["location"] == "/articles/journey-together-master-set-checklist"
    assert blog_client.get("/resources").headers["location"] == "/articles"

    sitemap = blog_client.get("/sitemap.xml").text
    assert "https://shop.test/articles/why-i-built-a-market-screener</loc><lastmod>2026-03-19" in sitemap
    assert "not-ready-yet" not in sitemap

    home = blog_client.get("/").text
    assert "Latest articles" in home and "/articles/why-i-built-a-market-screener" in home


# --- import from the saved Squarespace copy --------------------------------------------------

CDN = "https://images.squarespace-cdn.com/content/v1/site/123-ABC/pic.png"


def _png() -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (40, 30), (200, 30, 30)).save(buffer, "PNG")
    return buffer.getvalue()


def _archive(tmp_path: Path) -> Path:
    archive = tmp_path / "site_archive"
    (archive / "posts").mkdir(parents=True)
    (archive / "images").mkdir()
    (archive / "images" / "pic-1a2b3c4d.png").write_bytes(_png())
    (archive / "images.csv").write_text(f"url,file,used_by\n{CDN},pic-1a2b3c4d.png,post:x\n", encoding="utf-8")

    def post(name: str, title: str, old: str, body: str, cover: str = "") -> None:
        front = [
            "---",
            f"title: {json.dumps(title)}",
            "date: 2026-01-05",
            f"old_url: https://poke6s.com{old}",
            'author: "Adam Mills"',
            'categories: ["Collecting"]',
            'tags: ["Master Sets", "TCG"]',
            f"cover_image: {cover}",
            'excerpt: "A look at what completion costs &amp; why."',
            "---",
            "",
        ]
        (archive / "posts" / name).write_text("\n".join(front) + body, encoding="utf-8")

    post("2026-01-05-lets-talk.md", "Let’s talk about Master Sets…", "/resources/lets-talk-about-master-sets",
         f"See [Journey Together](https://poke6s.com/shop/p/journey-together-packs) and "
         f"[the checklist](https://poke6s.com/resources/sv9).\n\n"
         f"[![Card](../images/pic-1a2b3c4d.png)]({CDN})\n\n"
         "![Gone](../images/broken-00000000.webp)\n",
         cover="../images/pic-1a2b3c4d.png")
    post("2025-12-15-sv9.md", "Journey Together Master Set Checklist &amp; Promo Guide", "/resources/sv9",
         "Checklist.\n")
    return archive


def test_import_posts(tmp_path: Path):
    archive = _archive(tmp_path)
    out, images_dir = tmp_path / "articles", tmp_path / "images"
    report = article_import.run(archive, articles_dir=out, images_dir=images_dir)
    assert sorted(report.written) == ["journey-together-master-set-checklist-and-promo-guide",
                                      "lets-talk-about-master-sets"]
    assert report.missing_images == ["broken-00000000.webp"]

    article = parse("lets-talk-about-master-sets", (out / "lets-talk-about-master-sets.md").read_text())
    assert article.title == "Let’s talk about Master Sets…"
    assert article.old_path == "/resources/lets-talk-about-master-sets"
    assert article.description == "A look at what completion costs & why."
    assert article.cover and (images_dir / "1200" / f"{article.cover}.webp").is_file()
    text = (out / "lets-talk-about-master-sets.md").read_text()
    assert "](/shop/p/journey-together-packs)" in text                       # site link made relative
    assert "](/articles/journey-together-master-set-checklist-and-promo-guide)" in text  # post link
    assert f"![Card](/media/1200/{article.cover}.webp)" in text and CDN not in text  # unwrapped
    assert "Gone" not in text                                               # missing image dropped

    # Existing files are kept unless --force.
    (out / "lets-talk-about-master-sets.md").write_text("edited", encoding="utf-8")
    again = article_import.run(archive, articles_dir=out, images_dir=images_dir)
    assert "lets-talk-about-master-sets" in again.kept
    assert (out / "lets-talk-about-master-sets.md").read_text() == "edited"
    article_import.run(archive, articles_dir=out, images_dir=images_dir, force=True)
    assert (out / "lets-talk-about-master-sets.md").read_text() != "edited"


def test_cover_that_is_also_in_the_text_is_not_repeated():
    text = OLDER.replace("Checklist text.", "Checklist text.\n\n![Banner](/media/1200/journey-abcdef0123.webp)")
    assert parse("x", text).cover_in_body
    assert not parse("x", OLDER).cover_in_body


SCHEDULED = """---
title: Coming Soon
date: 2099-01-01
categories: Market Talk
draft: false
---
Proofread and waiting for its date.
"""


def test_scheduled_article_waits_for_its_date(imported, articles_dir):
    """A finished article with a future date stays hidden until that date, then goes live."""
    (articles_dir / "coming-soon.md").write_text(SCHEDULED, encoding="utf-8")
    articles = Articles(articles_dir)
    scheduled = articles.get("coming-soon")
    assert "coming-soon" not in [a.slug for a in articles.published()]
    assert articles.is_scheduled(scheduled) and not articles.is_scheduled(articles.get("not-ready-yet"))
    # On its date it appears, newest first, with no reload.
    assert articles.published(today="2099-01-01")[0].slug == "coming-soon"
    assert not articles.is_scheduled(scheduled, today="2099-01-01")

    with TestClient(create_app(imported, articles_dir=articles_dir), follow_redirects=False) as client:
        page = client.get("/articles/coming-soon")
        assert page.status_code == 200 and "Scheduled: goes live on 2099-01-01" in page.text
        assert "Coming Soon" not in client.get("/articles").text
        # Draft and scheduled pages list the review queue; live pages don't.
        assert 'class="review-queue"' in page.text and "/articles/not-ready-yet" in page.text
        assert 'class="review-queue"' not in client.get("/articles/why-i-built-a-market-screener").text
        assert "coming-soon" not in client.get("/sitemap.xml").text


class _StubMarket:
    """Stands in for market.MarketReference."""

    def __init__(self, products: dict) -> None:
        self.products = products

    def data(self) -> dict:
        return {"latest_date": "2026-11-19", "products": self.products}

    def entry(self, product_id: str):
        return self.products.get(product_id)


def test_market_boxes_use_todays_prices():
    market = _StubMarket({
        "712094": {"name": "Delta Reign Booster Box", "price": 192.54, "roc_30d_pct": -19.1,
                   "url": "https://market.poke6s.com/?kind=sealed&product=712094%7C%7CNormal"},
        "712102": {"name": "Delta <Reign> ETB", "price": 76.21, "roc_30d_pct": None, "url": None},
    })
    html = parse("x", "---\ntitle: X\n---\nIntro.\n\n[[market 712094 712102 999]]\n\nOutro.\n").html
    out = market_boxes(html, market)
    assert "[[market" not in out and out.count('class="market-box"') == 2      # unknown 999 skipped
    assert "$192.54" in out and "▼ 19.1% in 30 days" in out and "Price history" in out
    assert "https://tcgplayer-cdn.tcgplayer.com/product/712094_200w.jpg" in out
    assert "Delta &lt;Reign&gt; ETB" in out                                     # names escaped
    assert "<figcaption>" not in out and "Intro." in out and "Outro." in out    # no caption on prices
    # Cards show their set, and Japanese products say so (English and Japanese share names).
    market.products["655886"] = {"name": "Mega Charizard X ex - 110/080", "price": 654.46, "kind": "card",
                                 "group_name": "M2: Inferno X", "market": "JP", "roc_30d_pct": -8.0}
    card = market_boxes(parse("x", "[[market 655886]]\n").html, market)
    assert '<span class="market-box-set">Inferno X · Japanese</span>' in card
    assert "market-box-set" not in out                                         # English sealed: none
    assert 'class="market-boxes market-boxes-cards"' in card and "655886_in_1000x1000.jpg" in card
    assert "market-boxes-cards" not in out
    # [[cards ...]]: pictures, names and sets only.
    art = market_boxes(parse("x", "[[cards 655886]]\n").html, market)
    assert "655886_in_1000x1000.jpg" in art and "Inferno X · Japanese" in art
    assert "$" not in art and "Price history" not in art and "Card images from TCGplayer." in art
    # A tag whose products are all unknown disappears; text that merely mentions one stays.
    assert market_boxes(parse("x", "[[market 1]]\n").html, market).strip() == ""
    inline = parse("x", "Write [[market 712094]] on its own line.\n").html
    assert market_boxes(inline, market) == inline


def test_article_page_allows_tcgplayer_pictures(blog_client):
    csp = blog_client.get("/articles/journey-together-master-set-checklist").headers["content-security-policy"]
    assert "img-src 'self' data: https://tcgplayer-cdn.tcgplayer.com" in csp
    assert "tcgplayer" not in blog_client.get("/articles").headers["content-security-policy"]


def test_tcgplayer_links_use_the_affiliate_link():
    """[name](tcgplayer:<id>) goes to the product page through the affiliate link, as sponsored."""
    html = parse("x", "---\ntitle: X\n---\nBuy [the ETB](tcgplayer:672401) or [ours](/shop).").html
    out = tcgplayer_links(html, "https://partner.tcgplayer.com/abc")
    assert ('<a href="https://partner.tcgplayer.com/abc?u=https%3A%2F%2Fwww.tcgplayer.com%2Fproduct%2F672401" '
            'rel="sponsored noopener">the ETB</a>') in out
    assert '<a href="/shop">ours</a>' in out          # other links untouched
    plain = tcgplayer_links(html, "")
    assert 'href="https://www.tcgplayer.com/product/672401" rel="sponsored noopener"' in plain
