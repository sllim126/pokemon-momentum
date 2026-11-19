"""The store admin: a separate app on port 8011, for the home network only (Phase 3).

Why separate: everything the public app (storefront/app/main.py, port 8010) serves is on the
internet through the Cloudflare Tunnel. The admin changes orders and stock, so it is its own
app and process (docs/storefront_plan.md, "How it fits together"; security review 2026-11-11,
item 15), and several independent walls keep it off the internet:

1. docker-compose publishes port 8011 on the server's LAN address only, and the admin sits on
   its own Docker network, so the tunnel connector (cloudflared) can't even reach it.
2. refuse_reason() below turns away any request that came through Cloudflare (CF-* headers)
   or from an address outside the private ranges.
3. Every page except /login and /healthz needs the owner's sign-in (auth.py), and every form
   carries a per-session token.

Run (docker-compose service `storefront-admin`):
    python -m uvicorn storefront.admin.main:app --host 0.0.0.0 --port 8011
"""

from __future__ import annotations

import ipaddress
import logging
import os
import re
import sqlite3
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote, urlencode, urlsplit

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..app import notify
from ..app import orders as store_orders
from ..app import product_changes, product_details
from ..app.classify import PRODUCT_TYPE_LABELS
from ..app.db import connect, latest_migration, migrate
from ..app.market import MarketReference
from ..app.settings import Settings, load_settings
from . import orders_view, overview, packing_slip, pricing_view, products_view
from .auth import COOKIE_NAME, SESSION_DAYS, Lockout, Sessions, check_password

log = logging.getLogger("storefront.admin")
ADMIN_DIR = Path(__file__).resolve().parent
STORE_STATIC = ADMIN_DIR.parent / "app" / "static"

# Headers Cloudflare adds to every request it forwards. Any one of them means the request came
# through the tunnel or Cloudflare's network, which must never happen here.
CLOUDFLARE_HEADERS = ("cf-connecting-ip", "cf-ray", "cf-visitor", "cf-ipcountry", "cdn-loop")

SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "X-Frame-Options": "DENY",
    "X-Content-Type-Options": "nosniff",
    # same-origin, not no-referrer: with no-referrer, browsers send `Origin: null` on form posts,
    # and _same_origin() would refuse the admin's own forms. Other sites still get nothing.
    "Referrer-Policy": "same-origin",
    "Content-Security-Policy": "default-src 'self'; img-src 'self' data:; style-src 'self'; "
                               "script-src 'self'; form-action 'self'; frame-ancestors 'none'",
    "X-Robots-Tag": "noindex, nofollow",
}


# Order numbers as the store makes them (P6-2026-0001, test mode P6T-2026-0001). Anything else in
# an /orders/... address is a 404 before the database is asked.
ORDER_NUMBER = re.compile(r"^P6T?-\d{4}-\d{4,6}$")
# Tracking numbers: letters and digits only once spaces are taken out (USPS 20-22 digits, UPS
# 1Z..., FedEx 12-15, DHL 10). Loose on purpose; this only catches paste mistakes.
TRACKING = re.compile(r"^[A-Za-z0-9]{8,40}$")
# Messages after a change (Post/Redirect/Get: the page is reloaded with ?done=<key>).
DONE = {
    "shipped": "Marked shipped. The tracking email went to the customer.",
    "shipped_outbox": "Marked shipped. Email isn't set up, so the tracking email was saved to the outbox.",
    "shipped_email_failed": "Marked shipped, but the tracking email failed. It will retry every 15 minutes "
                            "(see Discord); you can also resend it below.",
    "resent": "Email sent again.",
    "resend_failed": "The email failed again and is queued for retry (see Discord).",
    "note": "Note added.",
    "address": "Address changed. The old one is kept in the notes.",
}
PRODUCT_DONE = {
    "price": "Price saved. The store shows it now.",
    "settings": "Saved. The store shows the change now.",
    "same": "Nothing changed.",
    "stock": "Stock saved.",
    "count_same": "The count matched; nothing changed.",
    "created": "Product added. It's hidden until you show it: add photos and check the details first.",
    "details": "Details saved. The store shows them now.",
    "photos": "Photos added.",
    "photo_same": "Those photos are already on this product.",
    "photo_main": "Main photo changed.",
    "photo_remove": "Photo removed.",
}


@dataclass
class AdminConfig:
    password_hash: str = ""
    session_secret: str = ""
    # Nightly backups (storefront/tools/backup_store.py), read for the Overview page.
    backup_dir: Path | None = None
    # The owner's private costs file, for the Pricing page (pricing_view.py). Read-only mount.
    costs_file: Path | None = None

    @property
    def ready(self) -> bool:
        """Sign-in works only once both the password hash and the session secret are set."""
        return bool(self.password_hash and len(self.session_secret) >= 32)


def load_config(settings: Settings) -> AdminConfig:
    default_backups = settings.data_dir.parent / "backups" / "storefront"
    return AdminConfig(
        password_hash=os.getenv("STOREFRONT_ADMIN_PASSWORD_HASH", "").strip(),
        session_secret=os.getenv("STOREFRONT_ADMIN_SESSION_SECRET", "").strip(),
        backup_dir=Path(os.getenv("STOREFRONT_ADMIN_BACKUP_DIR", str(default_backups))),
        costs_file=Path(os.getenv("STOREFRONT_ADMIN_COSTS_FILE", "/app/costs/store_sku_costs.csv")),
    )


def refuse_reason(headers, client_host: str | None) -> str:
    """Why this request must be turned away ('' when it may proceed). See the module docstring."""
    if any(name in headers for name in CLOUDFLARE_HEADERS):
        return "came through Cloudflare"
    try:
        address = ipaddress.ip_address(client_host or "")
    except ValueError:
        return "no usable client address"
    if getattr(address, "ipv4_mapped", None):
        address = address.ipv4_mapped
    if not (address.is_private or address.is_loopback):
        return "not from the home network"
    return ""


def _same_origin(request: Request) -> bool:
    """A form post must come from this admin's own pages (checked when the browser says)."""
    origin = request.headers.get("origin")
    if not origin or origin == "null":
        return origin is None
    return urlsplit(origin).netloc == request.headers.get("host", "")


def create_admin_app(settings: Settings | None = None, config: AdminConfig | None = None) -> FastAPI:
    settings = settings or load_settings()
    config = config or load_config(settings)
    sessions = Sessions(config.session_secret, config.password_hash) if config.ready else None
    lockout = Lockout()
    if not config.ready:
        log.warning("Admin sign-in not set up: run `python3 -m storefront.admin.passwd` on the server.")

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        # The public app migrates too; doing it here as well means either can start first.
        migrate(settings.db_path)
        yield

    app = FastAPI(title="Poke6s admin", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.mount("/static", StaticFiles(directory=ADMIN_DIR / "static"), name="static")
    app.mount("/store-static", StaticFiles(directory=STORE_STATIC), name="store-static")
    templates = Jinja2Templates(directory=ADMIN_DIR / "templates")
    templates.env.filters["money"] = notify.money
    templates.env.filters["when"] = notify.local_time
    templates.env.filters["hours"] = overview.hours_label
    templates.env.globals.update(settings=settings)
    market = MarketReference(settings.market_reference_path)
    # Product photos for the Products pages (the 480 px copies; images.py has the layout).
    # check_dir=False: a fresh install has no images folder yet.
    app.mount("/images", StaticFiles(directory=settings.images_dir, check_dir=False), name="images")

    @app.middleware("http")
    async def guard(request: Request, call_next):
        reason = refuse_reason(request.headers, request.client.host if request.client else None)
        if reason:
            log.warning("Refused %s %s: %s", request.method, request.url.path, reason)
            response: Response = PlainTextResponse("Not available.", status_code=403)
        elif request.method == "POST" and not _same_origin(request):
            response = PlainTextResponse("Form came from another site.", status_code=403)
        else:
            response = await call_next(request)
        for name, value in SECURITY_HEADERS.items():
            response.headers.setdefault(name, value)
        return response

    @contextmanager
    def db() -> Iterator[sqlite3.Connection]:
        # Read-only, for pages. Changes open writer() below, one connection per change.
        conn = connect(settings.db_path, readonly=True)
        try:
            yield conn
        finally:
            conn.close()

    def signed_in(request: Request) -> str | None:
        cookie = request.cookies.get(COOKIE_NAME)
        return cookie if sessions and sessions.valid(cookie) else None

    def page(request: Request, name: str, cookie: str | None, status_code: int = 200, **context) -> HTMLResponse:
        token = sessions.form_token(cookie) if sessions and cookie else ""
        return templates.TemplateResponse(request, name, {"form_token": token, "signed_in": bool(cookie),
                                                          **context}, status_code=status_code)

    def to_login() -> RedirectResponse:
        return RedirectResponse("/login", status_code=303)

    @app.get("/healthz")
    def healthz():
        with db() as conn:
            schema = latest_migration(conn)
        return JSONResponse({"ok": True, "schema": schema, "sign_in_ready": config.ready})

    @app.get("/login", response_class=HTMLResponse)
    def login_page(request: Request):
        if signed_in(request):
            return RedirectResponse("/", status_code=303)
        return page(request, "login.html", None, ready=config.ready)

    @app.post("/login")
    def login(request: Request, password: str = Form("")):
        address = request.client.host if request.client else "?"
        if not sessions:
            return page(request, "login.html", None, status_code=503, ready=False)
        if lockout.locked(address):
            return page(request, "login.html", None, status_code=429, ready=True,
                        error="Too many wrong passwords. Try again in 15 minutes.")
        if not check_password(password, config.password_hash):
            if lockout.failed(address):
                log.warning("Admin sign-in locked for %s after repeated wrong passwords", address)
                notify.discord(settings, f"Store admin: sign-in locked for {address} after "
                                         f"repeated wrong passwords (15 minutes).")
            return page(request, "login.html", None, status_code=401, ready=True,
                        error="That password isn't right.")
        lockout.succeeded(address)
        cookie = sessions.new()
        response = RedirectResponse("/", status_code=303)
        # Not `secure`: the admin is plain http on the home network (no certificate there).
        response.set_cookie(COOKIE_NAME, cookie, max_age=SESSION_DAYS * 86400, httponly=True,
                            samesite="strict", path="/")
        return response

    @app.post("/logout")
    def logout(request: Request, form_token: str = Form("")):
        cookie = signed_in(request)
        if cookie and not sessions.form_token_ok(cookie, form_token):
            return PlainTextResponse("This form has expired. Reload the page and try again.", status_code=403)
        response = to_login()
        response.delete_cookie(COOKIE_NAME, path="/")
        return response

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        with db() as conn:
            data = overview.build(conn, settings, config.backup_dir or Path("/nonexistent"))
        return page(request, "overview.html", cookie, data=data, active="overview",
                    now=datetime.now(UTC).isoformat())

    # --- orders (Phase 3, step 2) -------------------------------------------------------------

    @contextmanager
    def writer() -> Iterator[sqlite3.Connection]:
        """A connection that may change things, for one change (the store's rules: orders.py)."""
        conn = connect(settings.db_path, writer=True)
        try:
            yield conn
        finally:
            conn.close()

    def form_ok(cookie: str, token: str) -> bool:
        return bool(sessions and sessions.form_token_ok(cookie, token))

    def expired_form() -> PlainTextResponse:
        return PlainTextResponse("This form has expired. Reload the page and try again.", status_code=403)

    def order_url(number: str, **params) -> str:
        query = f"?{urlencode(params)}" if params else ""
        return f"/orders/{quote(number)}{query}"

    def order_page(request: Request, cookie: str, number: str, status_code: int = 200, **context) -> Response:
        if not ORDER_NUMBER.match(number):
            return PlainTextResponse("Not found.", status_code=404)
        with db() as conn:
            order = store_orders.get_order(conn, number)
        if order is None:
            return PlainTextResponse("Not found.", status_code=404)
        # The "change the address" form: what was typed (after an error, opened) or the current one.
        typed = context.pop("address_form", None)
        shipping = order["shipping"] or {}
        current = {"name": shipping.get("name") or order["name"] or "", **(shipping.get("address") or {})}
        return page(request, "order.html", cookie, status_code=status_code, order=order, active="orders",
                    address_form=typed or current, address_open=bool(typed),
                    address=notify.address_lines(order), timeline=orders_view.timeline(order),
                    stripe_url=orders_view.stripe_url(order), waiting=orders_view.waiting_hours(order),
                    track_url=notify.tracking_url(order["carrier"], order["tracking_number"]),
                    status_url=store_orders.status_url(settings.base_url, order),
                    carriers=orders_view.CARRIERS, done=DONE.get(request.query_params.get("done", "")),
                    **context)

    @app.get("/orders", response_class=HTMLResponse)
    def order_list(request: Request, view: str = orders_view.DEFAULT_VIEW, q: str = ""):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        view = view if view in orders_view.VIEWS else orders_view.DEFAULT_VIEW
        q = q.strip()[:100]
        with db() as conn:
            rows = orders_view.list_orders(conn, view, q)
            counts = orders_view.counts(conn)
        return page(request, "orders.html", cookie, rows=rows, counts=counts, view=view, q=q,
                    views=orders_view.VIEWS, active="orders", page_size=orders_view.PAGE_SIZE)

    @app.get("/orders/{number}", response_class=HTMLResponse)
    def order_detail(request: Request, number: str):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        return order_page(request, cookie, number)

    @app.get("/orders/{number}/packing-slip.pdf")
    def order_packing_slip(request: Request, number: str, gift: int = 0):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not ORDER_NUMBER.match(number):
            return PlainTextResponse("Not found.", status_code=404)
        with db() as conn:
            order = store_orders.get_order(conn, number)
        if order is None or order["status"] in ("pending", "expired", "cancelled"):
            return PlainTextResponse("Not found.", status_code=404)
        pdf = packing_slip.build(order, settings, prices=not gift)
        name = f"packing-slip-{number}{'-gift' if gift else ''}.pdf"
        # inline: opens in the browser's PDF viewer, ready to print.
        return Response(pdf, media_type="application/pdf",
                        headers={"Content-Disposition": f'inline; filename="{name}"'})

    @app.post("/orders/{number}/ship")
    def order_ship(request: Request, number: str, form_token: str = Form(""), carrier: str = Form("USPS"),
                   carrier_other: str = Form(""), tracking: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        tracking = "".join(tracking.split())
        name = carrier_other.strip()[:40] if carrier == "Other" else carrier
        if carrier not in orders_view.CARRIERS or not name:
            return order_page(request, cookie, number, 400, error="Pick a carrier (or type one for Other).")
        if not TRACKING.match(tracking):
            return order_page(request, cookie, number, 400, error="That tracking number doesn't look right: "
                              "letters and digits only, 8 to 40 of them.", tracking=tracking)
        try:
            with writer() as conn:
                order = store_orders.mark_shipped(conn, settings, number, name, tracking, settings.base_url)
        except ValueError as exc:
            return order_page(request, cookie, number, 400, error=str(exc))
        if not order["email_sent"]:
            done = "shipped_email_failed"
        else:
            done = "shipped_outbox" if settings.email_transport == "outbox" else "shipped"
        log.info("Order %s marked shipped from the admin (%s)", number, name)
        return RedirectResponse(order_url(number, done=done), status_code=303)

    @app.post("/orders/{number}/resend")
    def order_resend(request: Request, number: str, form_token: str = Form(""), kind: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        with db() as conn:
            order = store_orders.get_order(conn, number) if ORDER_NUMBER.match(number) else None
        if order is None:
            return PlainTextResponse("Not found.", status_code=404)
        if kind not in ("confirmed", "shipped") or (kind == "shipped" and not order["tracking_number"]) \
                or order["status"] in ("pending", "expired", "cancelled") or not order["email"]:
            return order_page(request, cookie, number, 400, error="That email can't be sent for this order.")
        url = store_orders.status_url(settings.base_url, order)
        ok = (notify.shipped_email if kind == "shipped" else notify.order_confirmation)(settings, order, url)
        return RedirectResponse(order_url(number, done="resent" if ok else "resend_failed"), status_code=303)

    @app.post("/orders/{number}/note")
    def order_note(request: Request, number: str, form_token: str = Form(""), note: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        try:
            with writer() as conn:
                store_orders.add_note(conn, number, note)
        except ValueError as exc:
            return order_page(request, cookie, number, 400, error=str(exc))
        return RedirectResponse(order_url(number, done="note"), status_code=303)

    @app.post("/orders/{number}/address")
    def order_address(request: Request, number: str, form_token: str = Form(""), name: str = Form(""),
                      line1: str = Form(""), line2: str = Form(""), city: str = Form(""), state: str = Form(""),
                      postal_code: str = Form(""), reason: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        fields = {"name": name, "line1": line1, "line2": line2, "city": city, "state": state,
                  "postal_code": postal_code}
        try:
            with writer() as conn:
                store_orders.correct_address(conn, number, fields, reason)
        except ValueError as exc:
            return order_page(request, cookie, number, 400, error=str(exc), address_form=fields)
        log.info("Order %s: shipping address changed from the admin", number)
        return RedirectResponse(order_url(number, done="address"), status_code=303)

    # --- products and stock (Phase 3, step 5) ---------------------------------------------------

    def product_url(product_id: int, **params) -> str:
        query = f"?{urlencode(params)}" if params else ""
        return f"/products/{product_id}{query}"

    def product_page(request: Request, cookie: str, product_id: int, status_code: int = 200, **context) -> Response:
        with db() as conn:
            product = products_view.get_product(conn, product_id, market)
            entries = product_changes.history(conn, product_id) if product else []
            pictures = product_details.photos(conn, product_id) if product else []
        if product is None:
            return PlainTextResponse("Not found.", status_code=404)
        history = [{**e, "what": products_view.describe(e)} for e in entries]
        ids = [i for i in product["tcgplayer_product_ids"].split("|") if i]
        details = context.pop("typed_details", None) or {
            **{key: product[key] or "" for key in ("title", "product_type", "set_name", "gtin", "description_md")},
            "language": product["language"] or "", "tcgplayer_ids": ", ".join(ids)}
        return page(request, "product.html", cookie, status_code=status_code, product=product, history=history,
                    photos=pictures, details=details, found=product_details.lookups(ids, market),
                    types=product_details.TYPES, type_labels=PRODUCT_TYPE_LABELS,
                    languages=product_details.LANGUAGES, max_photos=product_details.MAX_PHOTOS,
                    active="products", stock_kinds=products_view.STOCK_FORM_KINDS,
                    store_url=f"{settings.base_url.rstrip('/')}/p/{product['slug']}",
                    done=PRODUCT_DONE.get(request.query_params.get("done", "")), **context)

    @app.get("/products", response_class=HTMLResponse)
    def product_list(request: Request, view: str = products_view.DEFAULT_VIEW, q: str = ""):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        view = view if view in products_view.VIEWS else products_view.DEFAULT_VIEW
        q = q.strip()[:100]
        with db() as conn:
            rows = products_view.list_products(conn, view, q, market)
            counts = products_view.counts(conn)
        return page(request, "products.html", cookie, rows=rows, counts=counts, view=view, q=q,
                    views=products_view.VIEWS, active="products", page_size=products_view.PAGE_SIZE,
                    market_date=market.data().get("latest_date"))

    @app.get("/products/{product_id:int}", response_class=HTMLResponse)
    def product_detail(request: Request, product_id: int):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        return product_page(request, cookie, product_id)

    @app.post("/products/{product_id:int}/price")
    def product_price(request: Request, product_id: int, form_token: str = Form(""), price: str = Form(""),
                      compare_at: str = Form(""), reason: str = Form(""), confirm: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        typed = {"price": price, "compare_at": compare_at, "reason": reason}
        try:
            wanted = {"price_cents": product_changes.parse_money(price),
                      "compare_at_cents": product_changes.parse_money(compare_at)}
            with writer() as conn:
                changed = product_changes.update_product(conn, product_id, wanted, reason, confirmed=bool(confirm))
        except ValueError as exc:
            return product_page(request, cookie, product_id, 400, price_error=str(exc), typed=typed)
        log.info("Product %s: %s changed from the admin", product_id, ", ".join(changed) or "nothing")
        return RedirectResponse(product_url(product_id, done="price" if changed else "same"), status_code=303)

    @app.post("/products/{product_id:int}/settings")
    def product_settings(request: Request, product_id: int, form_token: str = Form(""), visible: str = Form(""),
                         featured: str = Form(""), max_per_order: str = Form(""), reason: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        limit = max_per_order.strip()
        if visible not in ("0", "1") or featured not in ("0", "1") or (limit and not limit.isdigit()):
            return product_page(request, cookie, product_id, 400,
                                settings_error="The per-order limit must be a whole number, or empty.")
        wanted = {"visible": int(visible), "featured": int(featured),
                  "max_per_order": int(limit) if limit else None}
        try:
            with writer() as conn:
                changed = product_changes.update_product(conn, product_id, wanted, reason)
        except ValueError as exc:
            return product_page(request, cookie, product_id, 400, settings_error=str(exc))
        log.info("Product %s: %s changed from the admin", product_id, ", ".join(changed) or "nothing")
        return RedirectResponse(product_url(product_id, done="settings" if changed else "same"), status_code=303)

    @app.post("/products/{product_id:int}/stock")
    def product_stock(request: Request, product_id: int, form_token: str = Form(""), kind: str = Form(""),
                      quantity: str = Form(""), note: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        amount = quantity.strip()
        try:
            if not amount.isdigit():
                raise ValueError("Type how many (a whole number).")
            with writer() as conn:
                change = product_changes.adjust_stock(conn, product_id, kind, int(amount), note)
        except ValueError as exc:
            return product_page(request, cookie, product_id, 400, stock_error=str(exc),
                                typed_stock={"kind": kind, "quantity": quantity, "note": note})
        log.info("Product %s: stock %+d (%s) from the admin", product_id, change, kind)
        return RedirectResponse(product_url(product_id, done="stock" if change else "count_same"), status_code=303)

    # --- adding products, details and photos (Phase 3, step 5b) ---------------------------------

    def detail_fields(**typed) -> dict:
        return {key: (typed.get(key) or "") for key in
                ("sku", "title", "product_type", "language", "set_name", "tcgplayer_ids", "gtin", "description_md")}

    def new_product_page(request: Request, cookie: str, form: dict, status_code: int = 200, **context) -> Response:
        try:
            ids = product_details.parse_ids(form["tcgplayer_ids"])
        except ValueError:
            ids = []
        with db() as conn:
            listed = product_details.already_listed(conn, ids)
        return page(request, "product_new.html", cookie, status_code=status_code, form=form,
                    found=product_details.lookups(ids, market), listed=listed,
                    active="products", types=product_details.TYPES, type_labels=PRODUCT_TYPE_LABELS,
                    languages=product_details.LANGUAGES, **context)

    @app.get("/products/new", response_class=HTMLResponse)
    def product_new(request: Request):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        return new_product_page(request, cookie, {**detail_fields(), "language": "en", "price": "", "stock": "0"})

    @app.post("/products/new")
    def product_create(request: Request, form_token: str = Form(""), action: str = Form("save"),
                       sku: str = Form(""), title: str = Form(""), product_type: str = Form(""),
                       language: str = Form(""), set_name: str = Form(""), tcgplayer_ids: str = Form(""),
                       gtin: str = Form(""), description_md: str = Form(""), price: str = Form(""),
                       stock: str = Form("0")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        form = {**detail_fields(sku=sku, title=title, product_type=product_type, language=language,
                                set_name=set_name, tcgplayer_ids=tcgplayer_ids, gtin=gtin,
                                description_md=description_md), "price": price, "stock": stock}
        if action == "check":
            # Look the TCGplayer IDs up and fill in whatever is still empty; nothing is saved.
            try:
                ids = product_details.parse_ids(tcgplayer_ids)
            except ValueError as exc:
                return new_product_page(request, cookie, form, 400, error=str(exc))
            hint = product_details.suggest(ids, market)
            for key, value in hint.items():
                if not form.get(key) and value:
                    form[key] = value
            entry = market.summary("|".join(ids))
            if entry and not price:   # a starting point: the market price
                form["price"] = f"{entry.price:.2f}"
            message = ("Found it: check the details below, then add the product." if hint
                       else "None of those IDs are in the market data, so fill the details in by hand.")
            return new_product_page(request, cookie, form, checked=message)
        try:
            amount = stock.strip() or "0"
            if not amount.isdigit():
                raise ValueError("Stock must be a whole number, 0 or more.")
            fields = {**form, "tcgplayer_ids": tcgplayer_ids}
            with writer() as conn:
                product_id = product_details.create_product(
                    conn, fields, market, price_cents=product_changes.parse_money(price), stock=int(amount))
        except ValueError as exc:
            return new_product_page(request, cookie, form, 400, error=str(exc))
        log.info("Product %s (%s) added from the admin", product_id, sku)
        return RedirectResponse(product_url(product_id, done="created"), status_code=303)

    @app.post("/products/{product_id:int}/details")
    def product_edit_details(request: Request, product_id: int, form_token: str = Form(""), title: str = Form(""),
                             product_type: str = Form(""), language: str = Form(""), set_name: str = Form(""),
                             tcgplayer_ids: str = Form(""), gtin: str = Form(""), description_md: str = Form(""),
                             reason: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        fields = detail_fields(title=title, product_type=product_type, language=language, set_name=set_name,
                               tcgplayer_ids=tcgplayer_ids, gtin=gtin, description_md=description_md)
        try:
            with writer() as conn:
                changed = product_details.update_details(conn, product_id, fields, market, reason)
        except ValueError as exc:
            return product_page(request, cookie, product_id, 400, details_error=str(exc), typed_details=fields)
        log.info("Product %s: %s changed from the admin", product_id, ", ".join(changed) or "nothing")
        return RedirectResponse(product_url(product_id, done="details" if changed else "same"), status_code=303)

    @app.post("/products/{product_id:int}/photos")
    async def product_add_photos(request: Request, product_id: int):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        # Read by hand (not Form()) to cap the number of files; each file's size is capped when it's read.
        form = await request.form(max_files=product_details.MAX_PHOTOS, max_fields=10)
        if not form_ok(cookie, str(form.get("form_token") or "")):
            return expired_form()
        uploads = [item for item in form.getlist("photos") if hasattr(item, "read") and item.filename]
        try:
            if not uploads:
                raise ValueError("Choose one or more photos first.")
            data = [await item.read(product_details.MAX_PHOTO_BYTES + 1) for item in uploads]
            with writer() as conn:
                added = product_details.add_photos(conn, settings.images_dir, product_id, data)
        except ValueError as exc:
            return product_page(request, cookie, product_id, 400, photo_error=str(exc))
        log.info("Product %s: %d photo(s) added from the admin", product_id, added)
        return RedirectResponse(product_url(product_id, done="photos" if added else "photo_same") + "#photos",
                                status_code=303)

    @app.post("/products/{product_id:int}/photos/{image_id:int}/{what}")
    def product_photo_action(request: Request, product_id: int, image_id: int, what: str, form_token: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        if what not in ("main", "remove"):
            return PlainTextResponse("Not found.", status_code=404)
        try:
            with writer() as conn:
                if what == "main":
                    product_details.make_main_photo(conn, product_id, image_id)
                else:
                    product_details.remove_photo(conn, settings.images_dir, product_id, image_id)
        except ValueError as exc:
            return product_page(request, cookie, product_id, 400, photo_error=str(exc))
        return RedirectResponse(product_url(product_id, done=f"photo_{what}") + "#photos", status_code=303)

    # --- pricing (Phase 3, step 6) ---------------------------------------------------------------

    rules_file = settings.data_dir.parent / "store_price_rules.csv"

    def pricing_plan(conn: sqlite3.Connection) -> dict:
        return pricing_view.load(conn, config.costs_file or Path("/nonexistent"), rules_file,
                                 settings.market_reference_path)

    @app.get("/pricing", response_class=HTMLResponse)
    def pricing(request: Request):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        with db() as conn:
            result = pricing_plan(conn)
        query = request.query_params
        done = None
        if query.get("done") == "applied":
            done = f"Applied {query.get('n', '0')} price change(s). Each is in its product's history as “repricing”."
            if query.get("stale", "0") != "0":
                done += (f" {query.get('stale')} ticked line(s) had changed since the page loaded and were left "
                         "alone: check them below.")
        return page(request, "pricing.html", cookie, result=result, active="pricing", done=done,
                    pick_key=pricing_view.pick_key)

    @app.post("/pricing/apply")
    async def pricing_apply(request: Request):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        form = await request.form(max_files=0, max_fields=500)
        if not form_ok(cookie, str(form.get("form_token") or "")):
            return expired_form()
        picks = [str(value) for value in form.getlist("pick")]
        with db() as conn:
            result = pricing_plan(conn)
        if result.get("error"):
            return RedirectResponse("/pricing", status_code=303)
        keep, stale = pricing_view.chosen(result, picks)
        changed = 0
        if keep:
            with writer() as conn:
                changed = pricing_view.reprice.apply(conn, keep, market_date=result["market_date"])
        stale += len(keep) - changed
        log.info("Pricing: %d price(s) applied from the admin, %d skipped as changed", changed, stale)
        return RedirectResponse(f"/pricing?{urlencode({'done': 'applied', 'n': changed, 'stale': stale})}",
                                status_code=303)

    @app.exception_handler(404)
    async def not_found(request: Request, _exc):
        return PlainTextResponse("Not found.", status_code=404)

    return app


def __getattr__(name: str):
    # `uvicorn storefront.admin.main:app` builds the app on first use, so importing this module
    # (tests, the password tool) never reads .env or opens the database.
    if name == "app":
        globals()["app"] = create_admin_app()
        return globals()["app"]
    raise AttributeError(name)
