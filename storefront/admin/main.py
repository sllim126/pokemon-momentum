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
import sqlite3
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ..app import notify
from ..app.db import connect, latest_migration, migrate
from ..app.settings import Settings, load_settings
from . import overview
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


@dataclass
class AdminConfig:
    password_hash: str = ""
    session_secret: str = ""
    # Nightly backups (storefront/tools/backup_store.py), read for the Overview page.
    backup_dir: Path | None = None

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
        # Read-only for now: step 1 only shows things. Later steps open a writer per change.
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
