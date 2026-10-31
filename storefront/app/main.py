"""The public storefront (poke6s.com): FastAPI + Jinja2, server-rendered.

Run: `docker-compose up -d storefront` (port 8010). Plan: docs/storefront_plan.md.
Developer guide (routes, import, tests): storefront/README.md.

Phase 1 is browse-only: products show "Buy on TCGplayer" until the Stripe checkout (Phase 2).
The database is opened read-only per request; only the importer (and later the LAN-only
admin) writes to it.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import catalog, classify, images, redirects, seo
from .db import connect, latest_migration, migrate
from .market import MarketReference
from .markdown import render, split_front_matter
from .settings import STOREFRONT_DIR, Settings, load_settings

APP_DIR = Path(__file__).resolve().parent
PAGES_DIR = STOREFRONT_DIR / "content" / "pages"
MEDIA_NAME = re.compile(r"^[a-z0-9-]+$")

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
}


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


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    market = MarketReference(settings.market_reference_path)
    pages = load_pages()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        migrate(settings.db_path)
        yield

    app = FastAPI(title="Poke6s storefront", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    # Trailing slashes are handled in not_found() with one 301, not Starlette's 307 hop.
    app.router.redirect_slashes = False
    app.mount("/static", StaticFiles(directory=APP_DIR / "static"), name="static")
    templates = Jinja2Templates(directory=APP_DIR / "templates")
    env = templates.env
    env.globals.update(settings=settings, year=datetime.now(UTC).year, sort_labels=catalog.SORT_LABELS,
                       type_labels=classify.PRODUCT_TYPE_LABELS, language_labels=classify.LANGUAGE_LABELS,
                       ship_class_labels=classify.SHIP_CLASS_LABELS, footer_pages=pages)
    env.filters["money"] = lambda cents: f"${(cents or 0) / 100:,.2f}"
    env.filters["dollars"] = lambda value: f"${value:,.2f}"
    env.filters["jsonld"] = seo.dump
    env.filters["markdown"] = render

    @contextmanager
    def db() -> Iterator[sqlite3.Connection]:
        # sqlite3's own context manager commits but doesn't close; this one closes.
        conn = connect(settings.db_path, readonly=True)
        try:
            yield conn
        finally:
            conn.close()

    def page(request: Request, template: str, status_code: int = 200, **context) -> HTMLResponse:
        with db() as conn:
            nav = catalog.nav_categories(conn)
        path = request.url.path
        context.setdefault("canonical", f"{settings.base_url}{path}")
        context.setdefault("og_image", seo.image_url(settings, None))
        response = templates.TemplateResponse(
            request, template, {"nav": nav, "path": path, **context}, status_code=status_code)
        return response

    @app.middleware("http")
    async def headers(request: Request, call_next):
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
            target = redirects.lookup(request.url.path)
            path = request.url.path
            if target is None and len(path) > 1 and path.endswith("/"):
                query = f"?{request.url.query}" if request.url.query else ""
                return RedirectResponse(path.rstrip("/") + query, status_code=301)
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
        return Response(seo.sitemap_xml(settings, entries, public_pages), media_type="application/xml")

    @app.get("/media/{width}/{name}.webp")
    def media(width: int, name: str):
        if width not in images.SIZES or not MEDIA_NAME.match(name):
            raise HTTPException(404)
        path = settings.images_dir / str(width) / f"{name}.webp"
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
        return page(request, "home.html", sections=sections, sets=sets,
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
        return page(request, "product.html", title=item["title"], product=item, market=summary,
                    below_market=below_market, related=others, crumbs=crumbs,
                    buy_url=item["tcgplayer_url"] or settings.tcgplayer_store_url,
                    og_image=seo.image_url(settings, item["image"]), og_type="product",
                    description=description,
                    jsonld=[seo.product_jsonld(settings, item), seo.breadcrumbs_jsonld(settings, crumbs)])

    # --- text pages (registered last so they never shadow the routes above) --------------

    @app.get("/{slug}", response_class=HTMLResponse)
    def text_page(request: Request, slug: str):
        found = pages.get(slug)
        if found is None:
            raise HTTPException(404)
        return page(request, "page.html", title=found["title"], page_data=found,
                    description=found["description"])

    return app


def _page_number(request: Request) -> int:
    value = request.query_params.get("page", "1")
    return int(value) if value.isdigit() else 1


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
