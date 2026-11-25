"""The public storefront (poke6s.com): FastAPI + Jinja2, server-rendered.

Run: `docker-compose up -d storefront` (port 8010). Plan: docs/storefront_plan.md.
Developer guide (routes, import, tests): storefront/README.md.

Checkout (Phase 2) is on only when a Stripe key is set (settings.checkout_enabled); without one,
products show "Buy on TCGplayer" as in Phase 1. The cart lives in the browser (localStorage) and
is priced by the server; orders.py has the checkout and order rules.
Articles (the blog carried over from Squarespace's /resources) are Markdown files; see articles.py.
Pages open the database read-only per request. Only checkout, the Stripe webhook, the thanks
page and the reconcile job open a writer (orders.py); the importer and the later LAN-only
admin are the other writers.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import re
import sqlite3
import time
from collections import defaultdict, deque
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import (FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse,
                               Response)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import accounts, catalog, support, wishlist, classify, collector_hub, images, notify, notify_list, orders, placeholder_library, redirects, seo, stripe_api
from .articles import Articles, market_boxes
from .binder_art import SetLogos
from .placeholder_library import PlaceholderFiles
from .db import connect, latest_migration, migrate
from .market import MarketReference
from .head import HeadAsGet
from .metrics import install_metrics
from .visitors import client_ip, install_visitor_counting
from .markdown import render, render_description, split_front_matter
from .settings import STOREFRONT_DIR, Settings, load_settings

APP_DIR = Path(__file__).resolve().parent
PAGES_DIR = STOREFRONT_DIR / "content" / "pages"
ARTICLES_DIR = STOREFRONT_DIR / "content" / "articles"
MEDIA_NAME = re.compile(r"^[a-z0-9-]+$")
RECONCILE_EVERY_SECONDS = 15 * 60
# Checkout attempts per visitor per hour. Each attempt holds stock for 30 minutes.
CHECKOUTS_PER_HOUR = 10
# What one visitor may hold at once in unpaid checkouts (security review 2026-11-11, item 10):
# without these, 10 checkouts an hour could reserve the whole shelf. Paid, cancelled and
# expired checkouts stop counting straight away. Visitors are told to finish or cancel first.
OPEN_CHECKOUTS_PER_VISITOR = 3
UNITS_HELD_PER_VISITOR = 12
# The per-visitor tables are tidied once they pass this many visitors.
VISITOR_TABLE_TIDY_AT = 1000
# Sign-ups for the "email me when the store opens" list, per visitor per hour.
SIGNUPS_PER_HOUR = 5
# Support messages (/contact's message box): per visitor, and for the whole site so faked
# addresses can't flood the admin either.
SUPPORT_PER_HOUR = 3
SUPPORT_SITE_PER_HOUR = 30
log = logging.getLogger("storefront")

SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "X-Frame-Options": "DENY",
    # Everything is served from our own origin. Cloudflare Web Analytics (at launch) and
    # Stripe (Phase 2 redirects away, so no change needed) are the expected additions.
    "Content-Security-Policy": (
        "default-src 'self'; img-src 'self' data:; script-src 'self'; style-src 'self'; "
        "object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'"
    ),
    # Security review 2026-11-11, item 16. Browsers remember to use HTTPS for a year (the site
    # is HTTPS-only behind Cloudflare). No includeSubDomains: other poke6s.com names (mail)
    # aren't ours to promise.
    "Strict-Transport-Security": "max-age=31536000",
    # No page needs the camera, microphone, location or USB; Stripe's checkout is its own page.
    "Permissions-Policy": "camera=(), microphone=(), geolocation=(), usb=()",
}
# The Binder Art Maker's page (/binder-art) also needs blob: images: the visitor's own picture is
# read with URL.createObjectURL and never leaves the browser. Everything else stays the same.
BINDER_ART_CSP = SECURITY_HEADERS["Content-Security-Policy"].replace("img-src 'self' data:", "img-src 'self' data: blob:")
# Article pages may show TCGplayer's product pictures in market boxes (articles.market_boxes).
ARTICLE_CSP = SECURITY_HEADERS["Content-Security-Policy"].replace(
    "img-src 'self' data:", "img-src 'self' data: https://tcgplayer-cdn.tcgplayer.com")
# Largest cart or checkout request accepted (item 16). A full cart (50 lines) is about 3 KB.
MAX_CART_BODY_BYTES = 32 * 1024


def load_pages() -> dict[str, dict]:
    """Markdown pages from storefront/content/pages/<slug>.md, keyed by slug."""
    pages = {}
    for path in sorted(PAGES_DIR.glob("*.md")):
        meta, body = split_front_matter(path.read_text(encoding="utf-8"))
        pages[path.stem] = {
            "slug": path.stem,
            "title": meta.get("title", path.stem.title()),
            "description": meta.get("description", ""),
            "draft": meta.get("draft", "").lower() == "true",
            "html": render(body),
        }
    return pages


def create_app(settings: Settings | None = None, articles_dir: Path | None = None, stripe=None) -> FastAPI:
    """Build the app. `stripe` replaces the real Stripe client (tests pass a fake)."""
    settings = settings or load_settings()
    market = MarketReference(settings.market_reference_path)
    set_logos = SetLogos(settings.set_logo_dir, settings.groups_csv)
    placeholder_files = PlaceholderFiles(settings.placeholder_dir)
    pages = load_pages()
    articles = Articles(articles_dir or ARTICLES_DIR)
    if not settings.checkout_switch:
        log.info("Checkout switched off (STOREFRONT_CHECKOUT=0): catalog only.")
    elif settings.stripe_secret_key and settings.setup_problem:
        log.warning("Checkout off: %s", settings.setup_problem)
    elif settings.stripe_secret_key and not settings.checkout_enabled:
        log.warning("Live Stripe key refused: set STOREFRONT_ALLOW_LIVE=1 to take real payments.")
    if stripe is None and settings.checkout_enabled:
        stripe = stripe_api.StripeClient(settings.stripe_secret_key)
    checkout_attempts: dict[str, deque] = defaultdict(deque)
    signup_attempts: dict[str, deque] = defaultdict(deque)
    support_attempts: dict[str, deque] = defaultdict(deque)
    support_site: deque = deque()
    # Order numbers of each visitor's checkouts, to count what they still hold (held_by).
    open_checkouts: dict[str, set[str]] = defaultdict(set)

    def reconcile_once() -> None:
        with writer() as conn:
            counts = orders.reconcile(conn, settings, stripe, settings.base_url)
            # A few days after shipping: the one Google review request per order.
            counts["review_emails"] = orders.send_review_requests(conn, settings, settings.base_url)
            # Wishlists of accounts deleted on the market site go too (privacy page).
            counts["wishlists_dropped"] = wishlist.drop_deleted_accounts(conn, settings)
            # Answered support messages are kept a year (privacy page).
            counts["support_dropped"] = support.drop_old(conn)
        # Order emails that failed earlier get another try (notify.py, review item 12).
        counts.update({f"email_{k}": v for k, v in notify.retry_queued(settings).items()})
        if any(counts.values()):
            log.info("Reconcile: %s", counts)

    async def reconcile_loop() -> None:
        # Catches up on missed webhooks (plan step 6): at start-up, then every 15 minutes.
        while True:
            try:
                await asyncio.to_thread(reconcile_once)
            except Exception:   # never let one bad pass stop the loop
                log.exception("Reconcile pass failed")
            await asyncio.sleep(RECONCILE_EVERY_SECONDS)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        migrate(settings.db_path)
        task = asyncio.create_task(reconcile_loop()) if stripe is not None and settings.checkout_enabled else None
        yield
        if task:
            task.cancel()

    app = FastAPI(title="Poke6s storefront", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    # Trailing slashes are handled in not_found() with one 301, not Starlette's 307 hop.
    app.router.redirect_slashes = False
    # HEAD answered as GET without the body (head.py). Added first, so it sits inside the metrics
    # and visitor counting: they still see HEAD, and monitors' HEADs aren't counted as visits.
    app.add_middleware(HeadAsGet)
    # Request counts and timings for Grafana; /metrics answers only Prometheus on the host.
    # Registered first so no catch-all page route can shadow it.
    install_metrics(app, "storefront")
    # Visitors and page views per day, no cookies (visitors.py); kept in the store's data folder.
    install_visitor_counting(app, "storefront", settings.data_dir / "visitors.sqlite3")
    app.mount("/static", StaticFiles(directory=APP_DIR / "static"), name="static")
    templates = Jinja2Templates(directory=APP_DIR / "templates")
    env = templates.env
    env.globals.update(settings=settings, checkout_enabled=settings.checkout_enabled, tax_note=orders.tax_note(settings), year=datetime.now(UTC).year, sort_labels=catalog.SORT_LABELS,
                       type_labels=classify.PRODUCT_TYPE_LABELS, language_labels=classify.LANGUAGE_LABELS,
                       footer_pages=pages,
                       social_links=seo.SOCIAL_LINKS, discord_url=dict(seo.SOCIAL_LINKS).get("Discord", ""))
    # Cloudflare keeps /static files for 4 hours, so the stylesheet and script links carry a
    # short hash of their contents (?v=...): a deploy that changes them changes the address.
    env.globals["static_v"] = {
        name: hashlib.sha256((APP_DIR / "static" / name).read_bytes()).hexdigest()[:10]
        for name in ("store.css", "store.js")}
    env.filters["money"] = lambda cents: f"${(cents or 0) / 100:,.2f}"
    env.filters["dollars"] = lambda value: f"${value:,.2f}"
    env.filters["jsonld"] = seo.dump
    env.filters["markdown"] = render
    env.filters["description"] = render_description
    env.filters["longdate"] = _long_date

    @contextmanager
    def db() -> Iterator[sqlite3.Connection]:
        # sqlite3's own context manager commits but doesn't close; this one closes.
        conn = connect(settings.db_path, readonly=True)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def writer() -> Iterator[sqlite3.Connection]:
        conn = connect(settings.db_path, writer=True)
        try:
            yield conn
        finally:
            conn.close()

    def site_url(request: Request) -> str:
        """Address to send shoppers back to from Stripe. Until launch base_url is localhost, so
        the address the shopper used is taken instead (a phone on the home network)."""
        if any(host in settings.base_url for host in ("://localhost", "://127.0.0.1")):
            return str(request.base_url).rstrip("/")
        return settings.base_url

    def tcgplayer_link(item: dict) -> str:
        """Where "Buy on TCGplayer" goes: the product's own listing link if one is set, else its
        TCGplayer product page (first TCGplayer ID) through the affiliate link (owner,
        2026-11-11), else the TCGplayer store's front page."""
        if item.get("tcgplayer_url"):
            return item["tcgplayer_url"]
        ids = [pid.strip() for pid in (item.get("tcgplayer_product_ids") or "").split("|") if pid.strip()]
        if not ids or not ids[0].isdigit():
            return settings.tcgplayer_store_url
        target = f"https://www.tcgplayer.com/product/{ids[0]}"
        if not settings.tcgplayer_affiliate_base:
            return target
        return f"{settings.tcgplayer_affiliate_base}?{urlencode({'u': target})}"

    async def cart_json(request: Request):
        """The request's JSON, read only up to MAX_CART_BODY_BYTES (ValueError beyond that)."""
        body = bytearray()
        async for chunk in request.stream():
            body += chunk
            if len(body) > MAX_CART_BODY_BYTES:
                raise ValueError("request too large")
        return json.loads(body)

    def visitor(request: Request) -> str:
        # The visitor's address. CF-Connecting-IP (set by the Cloudflare Tunnel) is believed
        # only from a private-network peer, the same rule as the visit counter (visitors.py);
        # the port itself only listens on this machine (docker-compose.yml).
        return client_ip(request.scope) or "?"

    def tidy(table: dict, stale) -> None:
        """Forget visitors with nothing current, so the tables can't grow forever."""
        if len(table) > VISITOR_TABLE_TIDY_AT:
            for key in [k for k, v in table.items() if stale(v)]:
                del table[key]

    def held_by(conn: sqlite3.Connection, key: str) -> tuple[int, int]:
        """(open checkouts, units reserved) for one visitor; drops their finished checkouts."""
        numbers = open_checkouts.get(key)
        if not numbers:
            return 0, 0
        marks = ",".join("?" * len(numbers))
        rows = conn.execute(
            f"""SELECT o.number, COALESCE(SUM(CASE WHEN i.reserved THEN i.quantity END), 0) AS units
                FROM orders o LEFT JOIN order_items i ON i.order_id = o.id
                WHERE o.number IN ({marks}) AND o.status = 'pending' AND o.reserved_until > ?
                GROUP BY o.number""", (*numbers, orders.now_iso())).fetchall()
        numbers.intersection_update(r["number"] for r in rows)
        return len(rows), sum(r["units"] for r in rows)

    def current_account(request: Request) -> accounts.Account | None:
        """The signed-in Poke6s account (accounts.py), looked up once per request."""
        if not hasattr(request.state, "account"):
            request.state.account = accounts.account_for(settings, request.cookies.get(accounts.COOKIE))
        return request.state.account

    def page(request: Request, template: str, status_code: int = 200, **context) -> HTMLResponse:
        with db() as conn:
            nav = catalog.nav_categories(conn)
        path = request.url.path
        context.setdefault("canonical", f"{settings.base_url}{path}")
        context.setdefault("og_image", seo.image_url(settings, None))
        account = current_account(request)
        response = templates.TemplateResponse(
            request, template, {"nav": nav, "path": path, "account": account,
                                "sign_in_url": accounts.sign_in_url(settings), **context}, status_code=status_code)
        if account is not None:
            # The header names the account: never let a shared cache keep a signed-in page.
            response.headers["Cache-Control"] = "private, no-store"
        return response

    @app.middleware("http")
    async def headers(request: Request, call_next):
        # www.poke6s.com reaches the store through the same tunnel; send it to the one
        # public address (STOREFRONT_BASE_URL) so search engines see a single site.
        host = request.headers.get("host", "").split(":")[0].lower()
        if host.startswith("www.") and settings.base_url.startswith("https://"):
            target = f"{settings.base_url}{request.url.path}"
            if request.url.query:
                target += f"?{request.url.query}"
            return RedirectResponse(target, status_code=301)
        response = await call_next(request)
        for key, value in SECURITY_HEADERS.items():
            response.headers.setdefault(key, value)
        if not settings.allow_indexing:
            response.headers["X-Robots-Tag"] = "noindex, nofollow"
        return response

    @app.exception_handler(StarletteHTTPException)
    async def not_found(request: Request, exc: StarletteHTTPException):
        if exc.status_code != 404:
            return PlainTextResponse(str(exc.detail), status_code=exc.status_code)
        if request.method in {"GET", "HEAD"}:
            path = request.url.path
            # Old blog addresses (/resources/...) first: their targets depend on the articles.
            target = articles.redirect_for(path) or redirects.lookup(path)
            if target is None and len(path) > 1 and path.endswith("/"):
                query = f"?{request.url.query}" if request.url.query else ""
                # Collapse leading slashes (and backslashes, which browsers treat the same):
                # "//example.com/" must stay on this site, not become a link to example.com.
                clean = "/" + path.lstrip("/\\").rstrip("/")
                return RedirectResponse(clean + query, status_code=301)
            if target is None and request.url.path.lower().startswith("/shop/p/"):
                with db() as conn:
                    target = catalog.product_redirect(conn, redirects.normalize(request.url.path)) or "/shop"
            if target:
                return RedirectResponse(target, status_code=301)
        return page(request, "404.html", status_code=404, title="Page not found")

    # --- service -------------------------------------------------------------------------

    @app.get("/healthz")
    def healthz():
        with db() as conn:
            products = conn.execute("SELECT count(*) FROM products WHERE visible = 1").fetchone()[0]
            version = latest_migration(conn)
        reference = market.data()
        return {"ok": True, "visible_products": products, "schema": version,
                "market_reference_date": reference.get("latest_date")}

    @app.get("/robots.txt", response_class=PlainTextResponse)
    def robots():
        return seo.robots_txt(settings)

    @app.get("/sitemap.xml")
    def sitemap():
        with db() as conn:
            entries = catalog.sitemap_entries(conn)
        public_pages = [slug for slug, p in pages.items() if not p["draft"]]
        return Response(seo.sitemap_xml(settings, entries, public_pages, articles.published()),
                        media_type="application/xml")

    @app.get("/media/{width}/{name}.webp")
    def media(width: str, name: str):
        # width is read as text so a non-number is a 404 page, not FastAPI's 422 JSON.
        if not (width.isascii() and width.isdigit()) or int(width) not in images.SIZES \
                or not MEDIA_NAME.match(name):
            raise HTTPException(404)
        path = settings.images_dir / width / f"{name}.webp"
        if not path.is_file():
            raise HTTPException(404)
        # File names carry a content hash (images.py), so they can be cached for a year.
        return FileResponse(path, media_type="image/webp",
                            headers={"Cache-Control": "public, max-age=31536000, immutable"})

    # --- shopping pages ------------------------------------------------------------------

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request):
        with db() as conn:
            sections = catalog.home_sections(conn)
            sets = catalog.list_sets(conn)[:8]
        return page(request, "home.html", sections=sections, sets=sets, latest_articles=articles.published()[:3],
                    jsonld=[seo.organization_jsonld(settings)],
                    description="Sealed Pokémon TCG booster boxes, bundles and ETBs, English and Japanese, "
                                "priced against the live market.")

    def listing(request: Request, filters: catalog.Filters, *, title: str, heading: str,
                crumbs: list[tuple[str, str]], intro: str = "", fixed: tuple[str, ...] = ()):
        with db() as conn:
            result = catalog.list_products(conn, filters)
        return page(request, "listing.html", title=title, heading=heading, intro=intro,
                    filters=filters, result=result, fixed=fixed, crumbs=crumbs,
                    query_for=lambda **changes: query_for(filters, fixed, **changes),
                    jsonld=[seo.breadcrumbs_jsonld(settings, crumbs)],
                    description=intro or f"{heading} at Poke6s.")

    @app.get("/shop", response_class=HTMLResponse)
    def shop(request: Request, q: str = "", lang: str = "", type: str = "", set: str = "",
             sort: str = "newest", tag: str = ""):
        if tag:
            # Old Squarespace filter links: /shop?tag=Japanese -> /shop?lang=ja
            mapped = redirects.SHOP_TAG_FILTERS.get(tag.strip().lower(), {"q": tag})
            return RedirectResponse("/shop" + (f"?{urlencode(mapped)}" if mapped else ""), status_code=301)
        filters = catalog.Filters(q=q, lang=lang, type=type, set=set, sort=sort, page=_page_number(request))
        heading = f'Search: "{" ".join(q.split())}"' if q.strip() else "Shop all"
        return listing(request, filters, title=heading, heading=heading,
                       crumbs=[("Home", "/"), ("Shop", "/shop")])

    @app.get("/c/{slug}", response_class=HTMLResponse)
    def category(request: Request, slug: str, lang: str = "", type: str = "", set: str = "",
                 sort: str = "newest", q: str = ""):
        with db() as conn:
            cat = catalog.get_category(conn, slug)
        if cat is None:
            raise HTTPException(404)
        filters = catalog.Filters(q=q, lang=lang, type=type, set=set, sort=sort, category=slug,
                                  page=_page_number(request))
        return listing(request, filters, title=cat["name"], heading=cat["name"], intro=cat["description"],
                       crumbs=[("Home", "/"), ("Shop", "/shop"), (cat["name"], f"/c/{slug}")],
                       fixed=("category",))

    @app.get("/sets", response_class=HTMLResponse)
    def sets(request: Request):
        with db() as conn:
            all_sets = catalog.list_sets(conn)
        return page(request, "sets.html", title="Browse by set", sets=all_sets,
                    description="Every Pokémon TCG set we currently stock.")

    @app.get("/sets/{slug}", response_class=HTMLResponse)
    def one_set(request: Request, slug: str, lang: str = "", type: str = "", sort: str = "newest", q: str = ""):
        with db() as conn:
            found = catalog.get_set(conn, slug)
        if found is None:
            raise HTTPException(404)
        filters = catalog.Filters(q=q, lang=lang, type=type, set=slug, sort=sort, page=_page_number(request))
        return listing(request, filters, title=found["name"], heading=found["name"],
                       crumbs=[("Home", "/"), ("Sets", "/sets"), (found["name"], f"/sets/{slug}")],
                       fixed=("set",))

    @app.get("/p/{slug}", response_class=HTMLResponse)
    def product(request: Request, slug: str):
        with db() as conn:
            item = catalog.get_product(conn, slug)
            if item is None:
                raise HTTPException(404)
            others = catalog.related(conn, item)
        summary = market.summary(item["tcgplayer_product_ids"])
        below_market = None
        if summary and summary.price > 0:
            diff = (summary.price * 100 - item["price_cents"]) / (summary.price * 100) * 100
            below_market = round(diff) if diff >= 1 else None
        crumbs = [("Home", "/"), ("Shop", "/shop")]
        if item["category"]:
            crumbs.append((item["category"]["name"], f"/c/{item['category']['slug']}"))
        crumbs.append((item["title"], f"/p/{slug}"))
        description = f"{item['title']} for {seo.money(item['price_cents'])} USD at Poke6s."
        account = current_account(request)
        saved = False
        if account is not None:
            with db() as conn:
                saved = wishlist.has(conn, account.email, item["id"])
        return page(request, "product.html", title=item["title"], product=item, market=summary, saved=saved,
                    below_market=below_market, related=others, crumbs=crumbs,
                    buy_url=tcgplayer_link(item),
                    og_image=seo.image_url(settings, item["image"]), og_type="product",
                    description=description,
                    jsonld=[seo.product_jsonld(settings, item), seo.breadcrumbs_jsonld(settings, crumbs)])

    # --- articles -----------------------------------------------------------------------

    @app.get("/articles", response_class=HTMLResponse)
    def article_list(request: Request, category: str = "", tag: str = ""):
        found = articles.filtered(category=category.strip(), tag=tag.strip())
        filter_name = category.strip() or tag.strip()
        heading = f"Articles: {filter_name}" if filter_name else "Articles"
        crumbs = [("Home", "/"), ("Articles", "/articles")]
        return page(request, "articles.html", title=heading, heading=heading, items=found,
                    categories=articles.categories(), category=category.strip(), tag=tag.strip(),
                    crumbs=crumbs, canonical=f"{settings.base_url}/articles",
                    jsonld=[seo.breadcrumbs_jsonld(settings, crumbs)],
                    description="Notes on the Pokémon TCG market, sets, collecting and running a small card shop.")

    @app.get("/articles/{slug}", response_class=HTMLResponse)
    def article(request: Request, slug: str):
        found = articles.get(slug)
        if found is None:
            raise HTTPException(404)
        crumbs = [("Home", "/"), ("Articles", "/articles"), (found.title, f"/articles/{slug}")]
        scheduled = articles.is_scheduled(found)
        # Drafts and scheduled posts list the rest of the review queue; live posts don't.
        waiting = articles.waiting() if found.draft or scheduled else []
        response = page(request, "article.html", title=found.title, article=found,
                        scheduled=scheduled, waiting=waiting,
                        body_html=market_boxes(found.html, market),   # today's prices, per view
                        more=articles.more_like(found), crumbs=crumbs, og_type="article",
                        og_image=seo.image_url(settings, found.cover), description=found.description,
                        jsonld=[seo.article_jsonld(settings, found), seo.breadcrumbs_jsonld(settings, crumbs)])
        response.headers["Content-Security-Policy"] = ARTICLE_CSP
        return response

    # --- free tools ---------------------------------------------------------------------
    # Collector Hub, Placeholder Library and Binder Art Maker, moved from market.poke6s.com on
    # 2026-11-20 (docs/market_inventory.md, option B). Collector Hub: server-rendered from
    # app/collector_hub.py. Placeholder Library: app/placeholder_library.py, which also serves
    # the generated pages and files both pages link to. Binder Art Maker: the work happens in the browser
    # (static/binder-art.js, with jsPDF and JSZip served from static/vendor/ because the CSP
    # allows no other script hosts).

    @app.get("/binder-art", response_class=HTMLResponse)
    def binder_art(request: Request):
        crumbs = [("Home", "/"), ("Binder Art Maker", "/binder-art")]
        response = page(request, "binder_art.html", title="Binder Art Maker", crumbs=crumbs,
                        jsonld=[seo.breadcrumbs_jsonld(settings, crumbs)],
                        description="Free tool: cut one picture into Pokémon-card-size pieces for a 2×2 to 4×4 "
                                    "binder page, with an optional set logo. Print-ready PDF with cut marks, or card PNGs.")
        response.headers["Content-Security-Policy"] = BINDER_ART_CSP
        return response

    @app.get("/collector-hub", response_class=HTMLResponse)
    def collector_hub_page(request: Request):
        crumbs = [("Home", "/"), ("Collector Hub", "/collector-hub")]
        return page(request, "collector_hub.html", title="Collector Hub", crumbs=crumbs,
                    sections=collector_hub.SECTIONS, resource_url=placeholder_library.resource_url,
                    download_url=placeholder_library.download_url, jsonld=[seo.breadcrumbs_jsonld(settings, crumbs)],
                    description="Free master-set tools: checklists for every Scarlet & Violet and Mega Evolution "
                                "set, and printable binder placeholder cards.")

    @app.get("/placeholders", response_class=HTMLResponse)
    def placeholder_library_page(request: Request):
        crumbs = [("Home", "/"), ("Collector Hub", "/collector-hub"), ("Placeholder Library", "/placeholders")]
        return page(request, "placeholder_library.html", title="Placeholder Library", crumbs=crumbs,
                    sections=placeholder_library.SECTIONS, files=placeholder_files,
                    resource_url=placeholder_library.resource_url, download_url=placeholder_library.download_url,
                    jsonld=[seo.breadcrumbs_jsonld(settings, crumbs)],
                    description="Free printable placeholder cards for Pokémon TCG binders, with checklists and "
                                "spreadsheets for every Scarlet & Violet and Mega Evolution set. Updated nightly.")

    @app.get("/placeholders/files/{bucket}/{rel:path}")
    def placeholder_file(bucket: str, rel: str):
        path = placeholder_files.asset(bucket, rel)
        if path is None:
            raise HTTPException(404)
        # Rebuilt nightly; ten minutes of caching keeps the tunnel quiet without going stale.
        headers = {"Cache-Control": "public, max-age=600"}
        media = placeholder_library.media_type(path)
        if path.suffix.lower() == ".html":
            body = path.read_bytes()
            headers["Content-Security-Policy"] = placeholder_library.page_csp(
                SECURITY_HEADERS["Content-Security-Policy"], body)
            return Response(body, media_type=media, headers=headers)
        return FileResponse(path, media_type=media, headers=headers)

    @app.get("/placeholders/download/{download_id}")
    def placeholder_download(download_id: str):
        path = placeholder_files.download(download_id)
        if path is None:
            raise HTTPException(404)
        return FileResponse(path, media_type=placeholder_library.media_type(path), filename=path.name,
                            headers={"Cache-Control": "public, max-age=600"})

    @app.get("/binder-art/logos.json")
    def binder_art_logos():
        # Changes when a logo is added or the nightly set list updates: an hour's cache is plenty.
        return JSONResponse({"logos": set_logos.logos()}, headers={"Cache-Control": "public, max-age=3600"})

    @app.get("/binder-art/logos/{name}")
    def binder_art_logo(name: str):
        path = set_logos.path(name.removesuffix(".png")) if name.endswith(".png") else None
        if path is None:
            raise HTTPException(404)
        return FileResponse(path, media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})

    # --- cart, checkout and orders (Phase 2; rules in orders.py) --------------------------

    @app.get("/cart", response_class=HTMLResponse)
    def cart(request: Request, cancelled: str = "", k: str = ""):
        notice = ""
        if cancelled and k:
            # Back from Stripe without paying: give the reserved stock back straight away
            # rather than after the 30-minute hold.
            with writer() as conn:
                order = orders.get_order(conn, cancelled)
                if order and order["status"] == "pending" and _token_matches(order["access_token"], k):
                    if stripe is not None and order["stripe_session_id"]:
                        try:
                            stripe.expire_session(order["stripe_session_id"])
                        except (stripe_api.StripeError, OSError):
                            pass   # the reconcile job tries again; the session expires anyway
                    orders.release(conn, order["number"], "expired", note="shopper went back to the cart")
            notice = "Checkout cancelled. Your items are still in your cart."
        response = page(request, "cart.html", title="Your cart", notice=notice,
                        free_over=settings.free_shipping_over_cents)
        response.headers["Cache-Control"] = "no-store"
        return response

    # --- "email me when the store opens" list (while checkout is off) ----------------------

    @app.get("/notify", response_class=HTMLResponse)
    def notify_page(request: Request, joined: str = "", error: str = ""):
        # Before launch: "email me when we open". With checkout on, the same list is "News and
        # restocks" (the footer form posts here too).
        if settings.checkout_enabled:
            title, description = "News and restocks", "Get an email about new stock, restocks and sales at Poke6s."
        else:
            title, description = "Get an email when we open", "Join the list and we'll email you when checkout opens on poke6s.com."
        return page(request, "notify.html", title=title, description=description,
                    joined=joined == "1", error=error == "1")

    @app.post("/notify")
    async def notify_join(request: Request):
        form = await request.form()
        # Bots fill every field; people never see "website" (hidden with CSS).
        if str(form.get("website") or ""):
            return RedirectResponse("/notify?joined=1", status_code=303)
        email = notify_list.clean_email(str(form.get("email") or ""))
        if email is None:
            return RedirectResponse("/notify?error=1", status_code=303)
        now = time.monotonic()
        tidy(signup_attempts, lambda v: not v or now - v[-1] > 3600)
        attempts = signup_attempts[visitor(request)]
        while attempts and now - attempts[0] > 3600:
            attempts.popleft()
        if len(attempts) >= SIGNUPS_PER_HOUR:
            return PlainTextResponse("Too many sign-ups from here. Please try again later.", status_code=429)
        attempts.append(now)
        with writer() as conn:
            notify_list.add(conn, email, str(form.get("source") or ""))
        return RedirectResponse("/notify?joined=1", status_code=303)

    # --- unsubscribe from the email list (app/mailing.py) ---------------------------------
    # The emailed link shows a confirm page (GET changes nothing, so link scanners can't
    # unsubscribe anyone); the button, or a mail app's one-click unsubscribe (RFC 8058, a POST
    # with "List-Unsubscribe=One-Click"), does it. The token is the address's own random code.

    def unsubscribe_page(request: Request, token: str, state: str, email: str = "") -> HTMLResponse:
        response = page(request, "unsubscribe.html", title="Unsubscribe", state=state, email=email,
                        token=token, status_code=404 if state == "missing" else 200)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Robots-Tag"] = "noindex, nofollow"
        return response

    @app.get("/unsubscribe/{token}", response_class=HTMLResponse)
    def unsubscribe_confirm(request: Request, token: str):
        with db() as conn:
            row = notify_list.find_by_token(conn, token)
        if row is None:
            return unsubscribe_page(request, token, "missing")
        return unsubscribe_page(request, token, "done" if row["unsubscribed_at"] else "confirm", row["email"])

    @app.post("/unsubscribe/{token}")
    async def unsubscribe(request: Request, token: str):
        with writer() as conn:
            found = notify_list.unsubscribe(conn, token)
            row = notify_list.find_by_token(conn, token) if found else None
        form = await request.form()
        if form.get("List-Unsubscribe") == "One-Click":
            return PlainTextResponse("Unsubscribed." if found else "Unknown link.", status_code=200 if found else 404)
        return unsubscribe_page(request, token, "done" if found else "missing", row["email"] if row else "")

    @app.post("/cart/quote")
    async def cart_quote(request: Request):
        try:
            cart_items = orders.parse_cart(await cart_json(request))
        except (orders.CartError, ValueError):
            return JSONResponse({"error": "The cart is not readable."}, status_code=400)
        with db() as conn:
            quote = orders.build_quote(conn, cart_items, settings)
        return quote.as_json()

    @app.post("/checkout")
    async def checkout(request: Request):
        if not settings.checkout_enabled or stripe is None:
            return JSONResponse({"error": "Checkout isn't open yet."}, status_code=503)
        key, now = visitor(request), time.monotonic()
        tidy(checkout_attempts, lambda v: not v or now - v[-1] > 3600)
        tidy(open_checkouts, lambda v: not v)
        attempts = checkout_attempts[key]
        while attempts and now - attempts[0] > 3600:
            attempts.popleft()
        if len(attempts) >= CHECKOUTS_PER_HOUR:
            return JSONResponse({"error": "Too many checkout attempts. Please try again later."}, status_code=429)
        try:
            cart_items = orders.parse_cart(await cart_json(request))
        except (orders.CartError, ValueError):
            return JSONResponse({"error": "The cart is not readable."}, status_code=400)
        with db() as conn:
            open_count, units = held_by(conn, key)
        wanted = sum(quantity for _, quantity in cart_items)
        if open_count >= OPEN_CHECKOUTS_PER_VISITOR or units + wanted > UNITS_HELD_PER_VISITOR:
            return JSONResponse({"error": "You already have checkouts waiting for payment. Please finish or "
                                          "cancel them first, or try again in 30 minutes."}, status_code=429)
        attempts.append(now)
        base = site_url(request)

        def run() -> dict:
            with writer() as conn:
                result = orders.start_checkout(conn, stripe, settings, cart_items, base)
            open_checkouts[key].add(result["number"])
            return result

        try:
            result = await asyncio.to_thread(run)
        except orders.CheckoutError as exc:
            body = {"error": exc.message}
            if exc.quote is not None:
                body["quote"] = exc.quote.as_json()
            return JSONResponse(body, status_code=exc.status)
        return {"url": result["url"]}

    def order_page(request: Request, order: dict, thanks: bool) -> HTMLResponse:
        # The carrier's own tracking page, the same link as the shipping email's button.
        response = page(request, "order.html", title=f"Order {order['number']}", order=order, thanks=thanks,
                        tracking_url=notify.tracking_url(order.get("carrier"), order.get("tracking_number")))
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/order/thanks", response_class=HTMLResponse)
    def order_thanks(request: Request, session_id: str = ""):
        with writer() as conn:
            order = orders.get_order(conn, session_id=session_id) if session_id else None
            if order is None:
                raise HTTPException(404)
            if order["status"] == "pending" and stripe is not None:
                # Usually the webhook has already arrived; if not, ask Stripe now so the
                # shopper sees "paid" without waiting for the reconcile job.
                try:
                    orders.mark_paid(conn, settings, stripe.retrieve_session(session_id), site_url(request))
                except (stripe_api.StripeError, OSError):
                    pass
                order = orders.get_order(conn, order["number"])
        return order_page(request, order, thanks=True)

    @app.get("/order/{number}", response_class=HTMLResponse)
    def order_status(request: Request, number: str, k: str = ""):
        with db() as conn:
            order = orders.get_order(conn, number)
        if order is None or not _token_matches(order["access_token"], k):
            raise HTTPException(404)
        return order_page(request, order, thanks=False)

    @app.post("/stripe/webhook")
    async def stripe_webhook(request: Request):
        payload = await request.body()
        try:
            event = stripe_api.verify_webhook(payload, request.headers.get("stripe-signature", ""),
                                              settings.stripe_webhook_secret)
        except ValueError:
            return JSONResponse({"error": "bad signature"}, status_code=400)
        base = site_url(request)

        def run() -> str:
            with writer() as conn:
                return orders.handle_event(conn, settings, event, base)

        return {"ok": True, "result": await asyncio.to_thread(run)}

    # --- the Poke6s account (step 9b; accounts.py): sign-in happens on the market site ------

    @app.get("/account", response_class=HTMLResponse)
    def account_page(request: Request):
        account = current_account(request)
        my_orders = []
        if account is not None:
            with db() as conn:
                my_orders = accounts.my_orders(conn, account)
            for order in my_orders:
                order["status_url"] = orders.status_url(settings.base_url, order)
                order["tracking_url"] = notify.tracking_url(order.get("carrier"), order.get("tracking_number"))
        saved_items = []
        if account is not None:
            with db() as conn:
                saved_items = wishlist.items(conn, account.email)
        response = page(request, "account.html", title="Your account", my_orders=my_orders, saved_items=saved_items,
                        sign_out_url=accounts.sign_out_url(settings),
                        manage_url=f"{settings.market_base_url}/account-settings")
        response.headers["Cache-Control"] = "private, no-store"
        return response

    @app.post("/wishlist")
    async def wishlist_change(request: Request):
        """The ♡ on product pages and Remove on the account page: a plain form post, then back to
        the page it came from. Signed out: off to sign in. The sign-in cookie is SameSite=Lax, so
        another site's form can't post as the visitor; the Origin check is a second wall."""
        origin = request.headers.get("origin")
        if origin and origin.rstrip("/") not in (settings.base_url, site_url(request)):
            return PlainTextResponse("Form came from another site.", status_code=403)
        account = current_account(request)
        if account is None:
            return RedirectResponse(accounts.sign_in_url(settings), status_code=303)
        form = await request.form()
        try:
            product_id = int(str(form.get("product_id") or ""))
        except ValueError:
            return PlainTextResponse("No such product.", status_code=400)
        with writer() as conn:
            if form.get("action") == "remove":
                wishlist.remove(conn, account.email, product_id)
            else:
                wishlist.add(conn, account.email, product_id)
            slug = conn.execute("SELECT slug FROM products WHERE id = ?", (product_id,)).fetchone()
        # Back to the product page, or the account page (only these two: no open redirect).
        target = "/account#wishlist" if form.get("back") == "account" or slug is None else f"/p/{slug[0]}"
        return RedirectResponse(target, status_code=303)

    @app.post("/support")
    async def support_send(request: Request):
        """The Contact page's message box (support.py). Saved, then a Discord ping with the
        message number only; the owner answers by email from the admin Overview."""
        origin = request.headers.get("origin")
        if origin and origin.rstrip("/") not in (settings.base_url, site_url(request)):
            return PlainTextResponse("Form came from another site.", status_code=403)
        form = await request.form()
        # Bots fill every field; people never see "website" (hidden with CSS).
        if str(form.get("website") or ""):
            return RedirectResponse("/contact?sent=1#support", status_code=303)
        fields, error = support.clean(str(form.get("name") or ""), str(form.get("email") or ""),
                                      str(form.get("order_number") or ""), str(form.get("message") or ""))
        if fields is None:
            typed = {key: str(form.get(key) or "")[:support.MAX_MESSAGE]
                     for key in ("name", "email", "order_number", "message")}
            return contact_page(request, pages.get("contact"), status_code=400, error=error, typed=typed)
        now = time.monotonic()
        tidy(support_attempts, lambda v: not v or now - v[-1] > 3600)
        attempts = support_attempts[visitor(request)]
        for queue in (attempts, support_site):
            while queue and now - queue[0] > 3600:
                queue.popleft()
        if len(attempts) >= SUPPORT_PER_HOUR or len(support_site) >= SUPPORT_SITE_PER_HOUR:
            return PlainTextResponse("Too many messages from here right now. Please email admin@poke6s.com instead.",
                                     status_code=429)
        attempts.append(now)
        support_site.append(now)
        account = current_account(request)
        with writer() as conn:
            number = support.add(conn, fields, signed_in=bool(account and account.email == fields["email"]))
        about = f" about {fields['order_number']}" if fields["order_number"] else ""
        await asyncio.to_thread(notify.discord, settings,
                                f"New support message #{number}{about} on poke6s.com. Read it on the admin Overview.")
        return RedirectResponse("/contact?sent=1#support", status_code=303)

    # --- text pages (registered last so they never shadow the routes above) --------------

    @app.get("/{slug}", response_class=HTMLResponse)
    def text_page(request: Request, slug: str):
        found = pages.get(slug)
        if found is None:
            raise HTTPException(404)
        if slug == "contact":
            return contact_page(request, found)
        return page(request, "page.html", title=found["title"], page_data=found,
                    description=found["description"])

    def contact_page(request: Request, found: dict, status_code: int = 200, error: str = "",
                     typed: dict | None = None) -> HTMLResponse:
        """/contact with its three ways to reach us (templates/_support.html). ?order= fills in the
        order number (from "Get help with this order"); a signed-in visitor's email is filled in."""
        account = current_account(request)
        query = request.query_params
        order = str(query.get("order") or "").strip().upper()
        form = typed or {"name": "", "email": account.email if account else "",
                         "order_number": order if support.ORDER_NUMBER.match(order) else "", "message": ""}
        response = page(request, "page.html", status_code=status_code, title=found["title"], page_data=found,
                        description=found["description"], support_form=form, support_error=error,
                        support_sent=query.get("sent") == "1")
        response.headers["Cache-Control"] = "private, no-store"
        return response

    return app


def _long_date(value: str) -> str:
    """'2026-03-19' -> 'March 19, 2026' (left as it is if it isn't a date)."""
    try:
        day = datetime.strptime(value, "%Y-%m-%d")
    except (TypeError, ValueError):
        return value or ""
    return f"{day:%B} {day.day}, {day.year}"


def _page_number(request: Request) -> int:
    # Plain ASCII digits only, at most 6: str.isdigit() also accepts "²", which int() refuses,
    # and a 5,000-digit number is too long for int() (both were 500 errors).
    value = request.query_params.get("page", "1")
    return int(value) if value.isascii() and value.isdigit() and len(value) <= 6 else 1


def _token_matches(expected: str, given: str) -> bool:
    """Constant-time check of an order link's secret. Compared as bytes: compare_digest raises
    TypeError on non-ASCII text, and the token comes from the address bar."""
    return bool(given) and hmac.compare_digest(expected.encode(), given.encode("utf-8", "replace"))


def query_for(filters: catalog.Filters, fixed: tuple[str, ...], **changes) -> str:
    """Query string for a filter link: the current filters with `changes` applied.

    Filters fixed by the address (the category on /c/..., the set on /sets/...) are left out.
    Changing any filter returns to page 1.
    """
    values = {"q": filters.q, "lang": filters.lang, "type": filters.type, "set": filters.set,
              "sort": filters.sort, "page": filters.page}
    if "page" not in changes:
        values["page"] = 1
    values.update(changes)
    for key in fixed:
        values.pop(key, None)
    values = {k: v for k, v in values.items()
              if v not in ("", None) and not (k == "sort" and v == "newest") and not (k == "page" and v == 1)}
    return ("?" + urlencode(values)) if values else "?"


app = create_app()
