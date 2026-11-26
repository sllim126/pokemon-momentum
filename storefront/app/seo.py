"""Search-engine and social-sharing helpers: JSON-LD, sitemap, robots.

Structured data follows schema.org as Google documents it for merchant listings: Product with
an Offer (price, currency, availability, condition), BreadcrumbList, and Organization; articles
get BlogPosting.
"""

from __future__ import annotations

import json
import re
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
    # Catalog mode (checkout off, owner 2026-11-11): nothing can be bought here, so don't tell
    # search engines "InStock". Sold-out items still say OutOfStock, which stays true.
    if not product["in_stock"]:
        availability = "https://schema.org/OutOfStock"
    elif settings.checkout_enabled:
        availability = "https://schema.org/InStock"
    else:
        availability = None
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
    if availability is None:
        del data["offers"]["availability"]
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


# Poke6s community and social accounts, shown in the footer and listed as the shop's other
# profiles (Organization "sameAs"), which helps search engines connect them to the shop.
SOCIAL_LINKS = (
    ("Discord", "https://discord.gg/AejcWHkCG"),
    ("YouTube", "https://www.youtube.com/@Poke6s_TCG"),
    ("Instagram", "https://www.instagram.com/poke6s/"),
    ("Facebook", "https://www.facebook.com/poke6s/"),
    ("X", "https://x.com/poke6s"),
    ("Reddit", "https://reddit.com/u/poke6s"),
)


def organization_jsonld(settings: Settings) -> dict:
    return {
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": "Poke6s",
        "legalName": "Poke6s LLC",
        "url": settings.base_url + "/",
        "logo": f"{settings.base_url}/static/img/logo.png",
        "email": settings.contact_email,
        "sameAs": [settings.market_base_url, settings.tcgplayer_store_url] + [url for _name, url in SOCIAL_LINKS],
    }


def dump(data: dict) -> str:
    """JSON for a <script type="application/ld+json"> block. '<' is escaped so text in a
    product title can never close the script tag."""
    return json.dumps(data, ensure_ascii=False).replace("<", "\\u003c")


def sitemap_xml(settings: Settings, entries: dict, page_slugs: list[str], articles=()) -> str:
    urls: list[tuple[str, str | None]] = [("/", None), ("/shop", None), ("/sets", None), ("/collector-hub", None), ("/placeholders", None), ("/binder-art", None)]
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
    return f"User-agent: *\nDisallow: /order/\nDisallow: /checkout\nDisallow: /cart\nDisallow: /account\n\nSitemap: {settings.base_url}/sitemap.xml\n"


# --- Google Merchant Center feed (/feeds/google.xml; plan "Later (Phases 4-5)") ---------------
# Free product listings in Google Shopping. Merchant Center fetches this file on a schedule the
# owner sets up there (Products -> Add products -> Scheduled fetch). Google's product data spec:
# https://support.google.com/merchants/answer/7052112. Account-level settings (returns policy,
# handling time, US sales tax) live in Merchant Center, not here.

GOOGLE_NS = "http://base.google.com/ns/1.0"
MAX_ADDITIONAL_IMAGES = 10     # Google's limit for additional_image_link
_MD_LINK = re.compile(r"!?\[([^\]]*)\]\([^)]*\)")
_MD_MARKS = re.compile(r"[*_`#>]+")


def plain_text(markdown_text: str, limit: int = 5000) -> str:
    """A product description without Markdown marks, on one line, within Google's 5,000 characters."""
    text = _MD_LINK.sub(r"\1", markdown_text or "")
    text = _MD_MARKS.sub("", text)
    text = re.sub(r"^\s*-\s+", "", text, flags=re.M)
    return " ".join(text.split())[:limit]


def feed_shipping_cents(settings: Settings, price_cents: int, ship_class: str) -> int:
    """Shipping for one unit, as checkout charges it (orders.py): the class's flat rate, or
    nothing at or over the free-shipping line."""
    if settings.free_shipping_over_cents and price_cents >= settings.free_shipping_over_cents:
        return 0
    rates = settings.ship_rates_cents
    return rates.get(ship_class, max(rates.values()) if rates else 0)


def google_feed_xml(settings: Settings, products: list[dict]) -> str:
    """The RSS 2.0 feed Merchant Center reads: one <item> per product. `products` come from
    catalog.feed_products (visible, priced, with their photos). Products without a photo are left
    out (Google refuses an item without image_link), and so is everything while checkout is off
    (catalog mode: nothing can be bought here, so nothing should be advertised as for sale)."""
    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             f'<rss version="2.0" xmlns:g="{GOOGLE_NS}">', "<channel>",
             "  <title>Poke6s</title>", f"  <link>{escape(settings.base_url)}/</link>",
             "  <description>Pokémon TCG sealed products from Poke6s</description>"]

    def tag(name: str, value) -> None:
        lines.append(f"    <g:{name}>{escape(str(value))}</g:{name}>")

    for p in products if settings.checkout_enabled else []:
        if not p["images"]:
            continue
        lines.append("  <item>")
        tag("id", p["sku"])
        tag("title", p["title"][:150])
        tag("description", plain_text(p["description_md"]) or p["title"])
        tag("link", f"{settings.base_url}/p/{p['slug']}")
        tag("image_link", image_url(settings, p["images"][0]))
        for name in p["images"][1:1 + MAX_ADDITIONAL_IMAGES]:
            tag("additional_image_link", image_url(settings, name))
        tag("availability", "in_stock" if p["in_stock"] else "out_of_stock")
        tag("price", f"{money(p['price_cents'])} USD")
        # Bulk lots are cards pulled from packs: Google counts those as used.
        tag("condition", "used" if p["kind"] == "bulk" else "new")
        if p["kind"] != "accessory":   # accessories come from several makers
            tag("brand", "Pokémon")
        if p.get("gtin"):
            tag("gtin", p["gtin"])
        else:
            # No barcode saved yet (the admin's Details panel takes one). Google lists the item
            # anyway; adding UPC/JAN barcodes helps it match searches.
            tag("identifier_exists", "no")
        if p.get("product_type"):
            tag("product_type", p["product_type"])
        lines.append("    <g:shipping><g:country>US</g:country><g:service>Standard</g:service>"
                     f"<g:price>{money(feed_shipping_cents(settings, p['price_cents'], p['ship_class']))} USD"
                     "</g:price></g:shipping>")
        lines.append("  </item>")
    lines += ["</channel>", "</rss>"]
    return "\n".join(lines) + "\n"
