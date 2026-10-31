"""Search-engine and social-sharing helpers: JSON-LD, sitemap, robots.

Structured data follows schema.org as Google documents it for merchant listings: Product with
an Offer (price, currency, availability, condition), BreadcrumbList, and Organization; articles
get BlogPosting.
"""

from __future__ import annotations

import json
from xml.sax.saxutils import escape

from .settings import Settings


def money(cents: int | None) -> str:
    return f"{(cents or 0) / 100:.2f}"


def image_url(settings: Settings, file_name: str | None, width: int = 1200) -> str:
    if not file_name:
        return f"{settings.base_url}/static/img/og-default.png"
    return f"{settings.base_url}/media/{width}/{file_name}.webp"


def product_jsonld(settings: Settings, product: dict) -> dict:
    url = f"{settings.base_url}/p/{product['slug']}"
    if product["in_stock"]:
        availability = "https://schema.org/InStock"
    else:
        availability = "https://schema.org/OutOfStock"
    data = {
        "@context": "https://schema.org",
        "@type": "Product",
        "name": product["title"],
        "sku": product["sku"],
        "url": url,
        "image": [image_url(settings, i["file_name"]) for i in product.get("images", [])]
                 or [image_url(settings, None)],
        "brand": {"@type": "Brand", "name": "Pokémon"},
        "offers": {
            "@type": "Offer",
            "url": url,
            "priceCurrency": "USD",
            "price": money(product["price_cents"]),
            "availability": availability,
            "itemCondition": "https://schema.org/NewCondition" if product["kind"] != "single"
            else "https://schema.org/UsedCondition",
            "seller": {"@type": "Organization", "name": "Poke6s"},
        },
    }
    if product.get("gtin"):
        data["gtin"] = product["gtin"]
    if product.get("description_md"):
        data["description"] = product["description_md"][:500]
    if product.get("set_name"):
        data["category"] = product["set_name"]
    return data


def breadcrumbs_jsonld(settings: Settings, crumbs: list[tuple[str, str]]) -> dict:
    """crumbs: [(name, path), ...] from the home page down."""
    return {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": n, "name": name, "item": f"{settings.base_url}{path}"}
            for n, (name, path) in enumerate(crumbs, start=1)
        ],
    }


def article_jsonld(settings: Settings, article) -> dict:
    """BlogPosting for one article (articles.Article)."""
    data = {
        "@context": "https://schema.org",
        "@type": "BlogPosting",
        "headline": article.title,
        "url": f"{settings.base_url}/articles/{article.slug}",
        "mainEntityOfPage": f"{settings.base_url}/articles/{article.slug}",
        "image": [image_url(settings, article.cover)],
        "publisher": {"@type": "Organization", "name": "Poke6s",
                      "logo": {"@type": "ImageObject", "url": f"{settings.base_url}/static/img/logo.png"}},
    }
    if article.date:
        data["datePublished"] = article.date
    if article.author:
        data["author"] = {"@type": "Person", "name": article.author}
    if article.description:
        data["description"] = article.description
    if article.tags:
        data["keywords"] = ", ".join(article.tags)
    return data


def organization_jsonld(settings: Settings) -> dict:
    return {
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": "Poke6s",
        "legalName": "Poke6s LLC",
        "url": settings.base_url + "/",
        "logo": f"{settings.base_url}/static/img/logo.png",
        "email": settings.contact_email,
        "sameAs": [settings.market_base_url, settings.tcgplayer_store_url],
    }


def dump(data: dict) -> str:
    """JSON for a <script type="application/ld+json"> block. '<' is escaped so text in a
    product title can never close the script tag."""
    return json.dumps(data, ensure_ascii=False).replace("<", "\\u003c")


def sitemap_xml(settings: Settings, entries: dict, page_slugs: list[str], articles=()) -> str:
    urls: list[tuple[str, str | None]] = [("/", None), ("/shop", None), ("/sets", None)]
    if articles:
        urls.append(("/articles", None))
        urls += [(f"/articles/{a.slug}", a.date or None) for a in articles]
    urls += [(f"/c/{c['slug']}", None) for c in entries["categories"]]
    urls += [(f"/sets/{s['slug']}", None) for s in entries["sets"]]
    urls += [(f"/p/{p['slug']}", p["updated_at"][:10]) for p in entries["products"]]
    urls += [(f"/{slug}", None) for slug in page_slugs]
    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for path, lastmod in urls:
        lines.append("  <url><loc>" + escape(settings.base_url + path) + "</loc>"
                     + (f"<lastmod>{lastmod}</lastmod>" if lastmod else "") + "</url>")
    lines.append("</urlset>")
    return "\n".join(lines) + "\n"


def robots_txt(settings: Settings) -> str:
    if not settings.allow_indexing:
        # Before launch (or on a LAN copy): keep every crawler out.
        return "User-agent: *\nDisallow: /\n"
    return f"User-agent: *\nDisallow: /order/\nDisallow: /checkout\n\nSitemap: {settings.base_url}/sitemap.xml\n"
