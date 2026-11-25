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

import requests
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..app import notify
from ..app import orders as store_orders
from ..app import product_changes, product_details, support
from ..app.classify import PRODUCT_TYPE_LABELS
from ..app.db import connect, latest_migration, migrate
from ..app.market import MarketReference
from ..app.settings import Settings, load_settings
from ..app.stripe_api import StripeClient, StripeError
from . import (orders_view, overview, packing_slip, pricing_view, products_view, refunds, reports, shipping,
               supplier_profit, supplier_quotes)
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

# The screenshot reader on Supplier Pricing (static/supplier-ocr.js) runs tesseract.js from
# static/vendor/tesseract/. Its worker compiles WebAssembly, which a policy must allow with
# 'wasm-unsafe-eval' (it allows compiling WebAssembly only, not JavaScript eval). Only those
# files get it; they're fixed, pinned copies (README there), cached a day so the 7 MB isn't
# fetched again for every screenshot.
OCR_PATH = "/static/vendor/tesseract/"
OCR_CSP = SECURITY_HEADERS["Content-Security-Policy"].replace("script-src 'self'", "script-src 'self' 'wasm-unsafe-eval'")


# Order-number and tracking-number patterns live in shipping.py (shared with the Shipping page).
ORDER_NUMBER, TRACKING = shipping.ORDER_NUMBER, shipping.TRACKING
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
    "refunded": "Refunded. The customer has the refund email; banks take 5 to 10 business days to show it.",
    "refunded_outbox": "Refunded. Email isn't set up, so the refund email was saved to the outbox.",
    "refunded_email_failed": "Refunded, but the refund email failed. It will retry every 15 minutes (see Discord).",
    "refunded_no_email": "Refunded. This order has no email address, so no email went out.",
    "put_back": "Stock put back.",
    "refunded_put_back": "Refunded and stock put back. The customer has the refund email.",
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
    "packed": "Packed size saved. The Pirate Ship file uses it from now on.",
}


@dataclass
class AdminConfig:
    password_hash: str = ""
    session_secret: str = ""
    # Nightly backups (storefront/tools/backup_store.py), read for the Overview page.
    backup_dir: Path | None = None
    # The owner's private costs file, for the Pricing page (pricing_view.py). Read-only mount.
    costs_file: Path | None = None
    # Supplier Pricing writes the quote ledger and the name-to-SKU list (supplier_quotes.py), so both
    # live in data/storefront/, the one folder the admin can write. Supplier Profitability
    # (supplier_profit.py) reads the ledger and the market targets (data/, read-only here).
    supplier_quotes_file: Path | None = None
    supplier_mapping_file: Path | None = None
    bug_reports_db: Path | None = None
    market_targets_file: Path | None = None
    # The admin's own restricted Stripe key (Charges and Refunds write), for refunds (refunds.py).
    # The public store never gets it (docker-compose passes it to the admin only).
    stripe_refund_key: str = ""

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
        supplier_quotes_file=settings.data_dir / "supplier_quotes.csv",
        supplier_mapping_file=settings.data_dir / "supplier_name_mapping.csv",
        market_targets_file=settings.data_dir.parent / "market_prices_latest.csv",
        # The market site's bug report queue, read-only (Overview, owner home).
        bug_reports_db=settings.data_dir.parent / "processed" / "tracking.sqlite3",
        stripe_refund_key=os.getenv("STOREFRONT_ADMIN_STRIPE_KEY", "").strip(),
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


def create_admin_app(settings: Settings | None = None, config: AdminConfig | None = None,
                     refund_client=None, fx_rate: supplier_profit.FxRate | None = None) -> FastAPI:
    """`refund_client`: tests pass a fake with create_refund(); otherwise a StripeClient on the
    admin's refund key, or None when that key isn't set (the refund form then says so).
    `fx_rate`: tests pass one with a fake fetch; otherwise the live Frankfurter rate."""
    settings = settings or load_settings()
    config = config or load_config(settings)
    if refund_client is None and config.stripe_refund_key:
        refund_client = StripeClient(config.stripe_refund_key)
    fx_rate = fx_rate or supplier_profit.FxRate()
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
    # A form number without a pointless ".0" (13800.0 -> 13800, 2.9 -> 2.9).
    templates.env.filters["num"] = lambda value: f"{value:g}" if isinstance(value, float) else value
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
            if request.url.path.startswith(OCR_PATH) and response.status_code == 200:
                response.headers["Content-Security-Policy"] = OCR_CSP
                response.headers["Cache-Control"] = "private, max-age=86400"
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
            data = overview.build(conn, settings, config.backup_dir or Path("/nonexistent"),
                                  bug_db=config.bug_reports_db)
        return page(request, "overview.html", cookie, data=data, active="overview",
                    now=datetime.now(UTC).isoformat())

    # --- orders (Phase 3, step 2) -------------------------------------------------------------

    @app.post("/support/{message_id:int}/answered")
    def support_answered(request: Request, message_id: int, form_token: str = Form("")):
        """Overview: a Contact-page message was answered (by email); it leaves the list."""
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        with writer() as conn:
            support.mark_answered(conn, message_id)
        log.info("Support message #%s marked answered", message_id)
        return RedirectResponse("/#support", status_code=303)

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
            returnable = refunds.returnable(conn, order) if order else []
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
                    refund_blocked=refunds.can_refund(order), refund_left=refunds.remaining_cents(order),
                    refund_ready=refund_client is not None, refund_reasons=refunds.REASONS,
                    returnable=returnable, **context)

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

    @app.post("/orders/{number}/refund")
    async def order_refund(request: Request, number: str):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        form = await request.form(max_files=0, max_fields=60)
        if not form_ok(cookie, str(form.get("form_token") or "")):
            return expired_form()
        if not ORDER_NUMBER.match(number):
            return PlainTextResponse("Not found.", status_code=404)
        amount = str(form.get("amount") or "")
        put_back = {}
        for key, value in form.items():
            if key.startswith("back_") and key[5:].isdigit() and str(value).isdigit():
                put_back[int(key[5:])] = int(value)
        typed = {"amount": amount, "reason": str(form.get("reason") or ""), "note": str(form.get("note") or "")}
        try:
            if amount.strip() and amount.strip() not in ("0", "0.00") and refund_client is None:
                raise ValueError("Refunds aren't set up: STOREFRONT_ADMIN_STRIPE_KEY is missing (storefront/README.md, "
                                 "“Refunds”).")
            with writer() as conn:
                result = refunds.refund(conn, settings, refund_client, number, amount, typed["reason"], put_back,
                                        typed["note"], form.get("confirm") == "1")
        except ValueError as exc:
            return order_page(request, cookie, number, 400, refund_error=str(exc), refund_typed=typed)
        log.info("Order %s: refunded %s, %d unit(s) put back, from the admin", number, result["refunded"],
                 result["put_back"])
        # The combined message only when the email really went; otherwise the refund message says
        # what happened to the email (the units put back are in the order's notes either way).
        if result["refunded"] and result["put_back"] and result["email_sent"] and settings.email_transport != "outbox":
            done = "refunded_put_back"
        elif result["refunded"]:
            done = ("refunded_no_email" if result["email_sent"] is None else "refunded_email_failed"
                    if not result["email_sent"] else "refunded_outbox" if settings.email_transport == "outbox"
                    else "refunded")
        else:
            done = "put_back"
        return RedirectResponse(order_url(number, done=done), status_code=303)

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
            "language": product["language"] or "", "tcgplayer_ids": ", ".join(ids),
            "oversize": "1" if product["ship_class"] == "oversize" else ""}
        packed = context.pop("typed_packed", None) or {
            key: f"{product[column]:g}" if product[column] else ""
            for key, column in (("weight", "weight_oz"), ("length", "length_in"), ("width", "width_in"),
                                ("height", "height_in"))}
        return page(request, "product.html", cookie, status_code=status_code, product=product, history=history,
                    packed=packed,
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
                      compare_at: str = Form(""), reason: str = Form(""), confirm: str = Form(""),
                      price_manual: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        typed = {"price": price, "compare_at": compare_at, "reason": reason, "price_manual": bool(price_manual)}
        try:
            # "Keep this price" (ticked by default): the nightly repricing leaves it alone.
            wanted = {"price_cents": product_changes.parse_money(price),
                      "compare_at_cents": product_changes.parse_money(compare_at),
                      "price_manual": 1 if price_manual else 0}
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
                ("sku", "title", "product_type", "language", "set_name", "tcgplayer_ids", "gtin", "description_md",
                 "oversize")}

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
                       stock: str = Form("0"), oversize: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        form = {**detail_fields(sku=sku, title=title, product_type=product_type, language=language,
                                set_name=set_name, tcgplayer_ids=tcgplayer_ids, gtin=gtin,
                                description_md=description_md, oversize=oversize), "price": price, "stock": stock}
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
                             oversize: str = Form(""), reason: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        fields = detail_fields(title=title, product_type=product_type, language=language, set_name=set_name,
                               tcgplayer_ids=tcgplayer_ids, gtin=gtin, description_md=description_md,
                               oversize=oversize)
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

    @app.post("/products/{product_id:int}/packed")
    def product_packed(request: Request, product_id: int, form_token: str = Form(""), weight: str = Form(""),
                       length: str = Form(""), width: str = Form(""), height: str = Form(""),
                       reason: str = Form("")):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        if not form_ok(cookie, form_token):
            return expired_form()
        typed = {"weight": weight, "length": length, "width": width, "height": height}
        try:
            with writer() as conn:
                changed = shipping.set_packed_size(conn, product_id, weight, length, width, height, reason)
        except ValueError as exc:
            return product_page(request, cookie, product_id, 400, packed_error=str(exc), typed_packed=typed)
        return RedirectResponse(product_url(product_id, done="packed" if changed else "same") + "#packed",
                                status_code=303)

    # --- shipping through Pirate Ship (Phase 3, step 3) ---------------------------------------------

    def shipping_page(request: Request, cookie: str, status_code: int = 200, **context) -> Response:
        with db() as conn:
            orders = shipping.unshipped(conn)
        query = request.query_params
        done = None
        if query.get("done") == "shipped":
            n, failed = query.get("n", "0"), query.get("failed", "0")
            done = f"Marked {n} order(s) shipped. "
            done += ("The tracking emails were saved to the outbox (email isn't set up)."
                     if settings.email_transport == "outbox" else "The customers have their tracking emails.")
            if failed != "0":
                done += f" {failed} email(s) failed and will retry every 15 minutes (see Discord)."
            if query.get("skipped", "0") != "0":
                done += f" {query.get('skipped')} ticked line(s) were left alone: the order had changed meanwhile."
        return page(request, "shipping.html", cookie, status_code=status_code, orders=orders, active="shipping",
                    missing=shipping.missing_sizes(orders), done=done, describe_size=shipping.describe_size, **context)

    @app.get("/shipping", response_class=HTMLResponse)
    def shipping_home(request: Request):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        return shipping_page(request, cookie)

    @app.post("/shipping/export")
    async def shipping_export(request: Request):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        form = await request.form(max_files=0, max_fields=500)
        if not form_ok(cookie, str(form.get("form_token") or "")):
            return expired_form()
        picked = {str(v) for v in form.getlist("order") if ORDER_NUMBER.match(str(v))}
        with db() as conn:
            orders = [o for o in shipping.unshipped(conn) if o["number"] in picked]
        if not orders:
            return shipping_page(request, cookie, 400, error="Tick at least one order first.")
        with writer() as conn:
            shipping.mark_exported(conn, [o["number"] for o in orders])
        log.info("Shipping: %d order(s) exported for Pirate Ship", len(orders))
        stamp = datetime.now(UTC).strftime("%Y-%m-%d-%H%M")
        return Response(shipping.export_csv(orders), media_type="text/csv; charset=utf-8",
                        headers={"Content-Disposition": f'attachment; filename="pirate-ship-{stamp}.csv"'})

    @app.post("/shipping/tracking")
    async def shipping_tracking(request: Request):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        form = await request.form(max_files=1, max_fields=5)
        if not form_ok(cookie, str(form.get("form_token") or "")):
            return expired_form()
        upload = form.get("file")
        try:
            if not hasattr(upload, "read") or not upload.filename:
                raise ValueError("Choose Pirate Ship's CSV file first.")
            found = shipping.read_tracking(await upload.read(shipping.MAX_UPLOAD_BYTES + 1))
        except ValueError as exc:
            return shipping_page(request, cookie, 400, upload_error=str(exc))
        with db() as conn:
            entries = shipping.check(conn, found["rows"])
        return page(request, "shipping_tracking.html", cookie, entries=entries, other=found["other"],
                    problems=found["problems"], file_name=upload.filename[:80], active="shipping",
                    pick_key=shipping.pick_key)

    @app.post("/shipping/tracking/apply")
    async def shipping_tracking_apply(request: Request):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        form = await request.form(max_files=0, max_fields=500)
        if not form_ok(cookie, str(form.get("form_token") or "")):
            return expired_form()
        picks = [p for p in (shipping.parse_pick(str(v)) for v in form.getlist("pick")) if p]
        shipped = failed = skipped = 0
        for number, carrier, tracking in picks:
            try:
                with writer() as conn:
                    order = store_orders.mark_shipped(conn, settings, number, carrier, tracking, settings.base_url)
            except ValueError:
                skipped += 1      # shipped, refunded or gone since the preview
                continue
            shipped += 1
            failed += 0 if order["email_sent"] else 1
        log.info("Shipping: %d order(s) marked shipped from a Pirate Ship file (%d skipped)", shipped, skipped)
        return RedirectResponse("/shipping?" + urlencode({"done": "shipped", "n": shipped, "failed": failed,
                                                          "skipped": skipped}), status_code=303)

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

    @app.get("/supplier-profitability", response_class=HTMLResponse)
    def supplier_profitability(request: Request):
        """Supplier quotes against store, market and target prices (supplier_profit.py). A GET
        form: every assumption is a query parameter, so nothing here changes anything."""
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        query = request.query_params
        typed_rate = supplier_profit.number(query.get("jpy_per_usd"), None)
        fx = fx_rate.get()
        a = supplier_profit.read_assumptions(query, fx["rate"] if fx else None)
        quotes, unmatched = supplier_profit.latest_quotes(config.supplier_quotes_file or Path("/nonexistent"))
        market_rows = supplier_profit.market_targets(config.market_targets_file or Path("/nonexistent"))
        with db() as conn:
            store = supplier_profit.store_prices(conn)
        result = supplier_profit.build(quotes, unmatched, market_rows, store, a)
        decision, needle = str(query.get("decision") or ""), str(query.get("q") or "")
        # View choices (all GET parameters, so a view can be bookmarked): judge on one channel,
        # a minimum margin, and the sort order (supplier_profit.SORTS, or "ch:<channel>").
        channel = str(query.get("channel") or "")
        channel = channel if channel in a.channels else ""
        min_margin = supplier_profit.number(query.get("min_margin"), None)
        sort = str(query.get("sort") or "best")
        if channel:
            result = {**result, "rows": supplier_profit.judged_on(result["rows"], channel)}
            result["counts"] = {d: sum(1 for row in result["rows"] if row["decision"] == d)
                                for d in supplier_profit.DECISIONS}
            profits = [row["best_profit"] for row in result["rows"] if row["best_profit"] is not None]
            result["avg_best_profit"] = supplier_profit.money(sum(profits) / len(profits)) if profits else None
        shown = supplier_profit.sort_rows(supplier_profit.visible(result["rows"], decision, needle, min_margin), sort)

        def sort_url(key: str) -> str:
            # The current view with another sort: every other parameter kept.
            params = [(k, v) for k, v in query.multi_items() if k != "sort"] + [("sort", key)]
            return "/supplier-profitability?" + urlencode(params)

        return page(request, "supplier_profitability.html", cookie, active="suppliers", a=a, fx=fx,
                    typed_rate=typed_rate, result=result, shown=shown,
                    decision=decision, needle=needle, decisions=supplier_profit.DECISIONS,
                    channel=channel, min_margin=min_margin, sort=sort, sorts=supplier_profit.SORTS,
                    sort_url=sort_url,
                    references=supplier_profit.REFERENCES, quick=supplier_profit.quick_quote(query, a),
                    query=query)

    # --- reports for the books (Phase 3, step 7; rules in reports.py) ----------------------------
    # GETs only: nothing here changes anything. The Stripe download asks Stripe for fees with the
    # admin's key (the same client as refunds; it may read charges, not payouts).

    @app.get("/reports", response_class=HTMLResponse)
    def reports_page(request: Request):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        month = reports.parse_month(request.query_params.get("month"))
        with db() as conn:
            orders = reports.sales(conn, month)
            utah_month = reports.utah_summary(conn, *reports.month_bounds(month))
            utah_year = reports.utah_summary(conn, *reports.year_bounds(month))
            tcgplayer = reports.tcgplayer_sales(conn, month)
        return page(request, "reports.html", cookie, active="reports", month=month,
                    rows=[{**reports.worksheet_row(o), "number": o["number"]} for o in orders],
                    utah_month=utah_month, utah_year=utah_year, tcgplayer=tcgplayer,
                    stripe_ready=refund_client is not None, error=request.query_params.get("error"))

    @app.get("/reports/sales.csv")
    def reports_sales_csv(request: Request):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        month = reports.parse_month(request.query_params.get("month"))
        with db() as conn:
            body = reports.sales_csv(conn, month)
        return Response(body, media_type="text/csv; charset=utf-8", headers={
            "Content-Disposition": f'attachment; filename="poke6s-sales-{month}.csv"'})

    @app.get("/reports/stripe.csv")
    def reports_stripe_csv(request: Request):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        month = reports.parse_month(request.query_params.get("month"))
        if refund_client is None or not hasattr(refund_client, "list_charges"):
            return RedirectResponse(f"/reports?{urlencode({'month': month, 'error': 'nokey'})}", status_code=303)
        try:
            with db() as conn:
                rows = reports.money_rows(conn, refund_client, month)
        except StripeError as exc:
            log.warning("Reports: Stripe refused the fee lookup for %s: %s", month, exc)
            return RedirectResponse(f"/reports?{urlencode({'month': month, 'error': 'stripe'})}", status_code=303)
        except requests.RequestException as exc:
            log.warning("Reports: Stripe unreachable for %s: %s", month, type(exc).__name__)
            return RedirectResponse(f"/reports?{urlencode({'month': month, 'error': 'stripe'})}", status_code=303)
        return Response(reports.money_csv(rows), media_type="text/csv; charset=utf-8", headers={
            "Content-Disposition": f'attachment; filename="poke6s-stripe-{month}.csv"'})

    # --- Supplier Pricing (moved from the market site 2026-11-22; supplier_quotes.py) -------------

    quotes_file = config.supplier_quotes_file or Path("/nonexistent")
    mapping_file = config.supplier_mapping_file or Path("/nonexistent")

    def quote_page(request: Request, cookie: str, form: dict, rows: list[dict] | None = None,
                   status_code: int = 200, **context) -> Response:
        return page(request, "supplier_pricing.html", cookie, status_code=status_code, active="suppliers",
                    form=form, rows=rows, summary=supplier_quotes.summary(rows) if rows else None,
                    recent=supplier_quotes.recent_quotes(quotes_file), **context)

    @app.get("/supplier-pricing", response_class=HTMLResponse)
    def supplier_pricing(request: Request):
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        query = request.query_params
        done = None
        if query.get("saved"):
            done = (f"Saved quote {query.get('saved')}: {query.get('n', '0')} row(s), "
                    f"{query.get('m', '0')} with a SKU.")
            if query.get("r", "0") != "0":
                done += f" {query.get('r')} new name-to-SKU match(es) remembered."
        # Today in the store's time zone (the owner's), not the server's UTC.
        today = datetime.now(UTC).astimezone(notify.MOUNTAIN).date().isoformat()
        form = {"supplier": supplier_quotes.DEFAULT_SUPPLIER, "quote_date": today,
                "source": "", "text": "", "remember": True, "only_sku": False}
        return quote_page(request, cookie, form, done=done)

    @app.post("/supplier-pricing")
    async def supplier_pricing_post(request: Request):
        """One form, three buttons (`action`): "read" splits the pasted text into rows, "check"
        matches the edited rows again, "save" appends them to the ledger and redirects."""
        cookie = signed_in(request)
        if not cookie:
            return to_login()
        data = await request.form(max_files=0, max_fields=5 * supplier_quotes.MAX_ROWS + 20)
        if not form_ok(cookie, str(data.get("form_token") or "")):
            return expired_form()
        form = {"supplier": str(data.get("supplier") or "").strip() or supplier_quotes.DEFAULT_SUPPLIER,
                "quote_date": str(data.get("quote_date") or "").strip(),
                "source": str(data.get("source") or "").strip()[:200],
                "text": str(data.get("text") or "")[:supplier_quotes.MAX_TEXT],
                "remember": bool(data.get("remember")),
                # Save only the rows with a SKU: ticked by default for a shop page (most of its
                # products aren't ones the store sells), unticked for a short list.
                "only_sku": bool(data.get("only_sku"))}
        action = str(data.get("action") or "check")
        if action == "read":
            rows = supplier_quotes.parse_text(form["text"])
            form["only_sku"] = any(row["note"] for row in rows)
            if form["supplier"] == supplier_quotes.DEFAULT_SUPPLIER and form["only_sku"]:
                form["supplier"] = supplier_quotes.shop_name(form["text"]) or form["supplier"]
        else:
            rows = supplier_quotes.form_rows(data.getlist("item"), data.getlist("cost"), data.getlist("sku"),
                                             data.getlist("note"))
        with db() as conn:
            store = supplier_profit.store_prices(conn)
        market = supplier_profit.market_targets(config.market_targets_file or Path("/nonexistent"))
        checked = supplier_quotes.check(rows, supplier_quotes.load_mapping(mapping_file), store, market)
        if action == "read" and not checked:
            return quote_page(request, cookie, form, status_code=400,
                              error="No rows found: paste a whole shop page, or a list with one item per line and "
                                    "its yen price at the end.")
        if action != "save":
            return quote_page(request, cookie, form, checked)
        quote_date = supplier_quotes.valid_date(form["quote_date"])
        if form["only_sku"]:
            checked = [row for row in checked if row["sku"]]
        bad = sum(1 for row in checked if row["status"] == "bad")
        error = ("Nothing to save yet." if not checked and not form["only_sku"] else
                 "No row has a SKU yet: type some, or untick “only the rows with a SKU”." if not checked else
                 "Pick the quote's date." if not quote_date else
                 f"{bad} row(s) need a name and a whole yen price (or clear both to drop the row)." if bad else None)
        if error:
            return quote_page(request, cookie, form, checked, status_code=400, error=error)
        result = supplier_quotes.save(quotes_file, mapping_file, checked, supplier=form["supplier"],
                                      quote_date=quote_date, source=form["source"], remember=form["remember"])
        log.info("Supplier Pricing: quote %s saved, %d row(s), %d with a SKU, %d match(es) remembered",
                 result["quote_id"], result["saved"], result["matched"], result["remembered"])
        return RedirectResponse("/supplier-pricing?" + urlencode(
            {"saved": result["quote_id"], "n": result["saved"], "m": result["matched"], "r": result["remembered"]}),
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
