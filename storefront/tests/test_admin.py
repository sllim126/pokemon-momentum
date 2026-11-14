"""The store admin (Phase 3, step 1): the walls that keep it off the internet, sign-in, and the
Overview page. Uses the synthetic store from conftest.py; nothing touches the network."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from storefront.admin import auth, overview
from storefront.admin.main import COOKIE_NAME, AdminConfig, create_admin_app, refuse_reason
from storefront.admin.passwd import update_env
from storefront.app.db import connect

PASSWORD = "correct horse battery staple"
SECRET = "s" * 64
HOME = ("192.168.1.50", 50000)          # a phone or laptop on the home network


@pytest.fixture(scope="module")
def password_hash() -> str:
    # Fewer rounds than the real 600,000, to keep the tests quick; the format is the same.
    return auth.hash_password(PASSWORD, iterations=1000)


@pytest.fixture()
def config(tmp_path: Path, password_hash: str) -> AdminConfig:
    return AdminConfig(password_hash=password_hash, session_secret=SECRET, backup_dir=tmp_path / "backups")


@pytest.fixture()
def admin(imported, config):
    with TestClient(create_admin_app(imported, config), client=HOME, follow_redirects=False) as test_client:
        yield test_client


def sign_in(client: TestClient) -> TestClient:
    response = client.post("/login", data={"password": PASSWORD})
    assert response.status_code == 303 and COOKIE_NAME in response.cookies
    return client


def add_order(settings, number: str, status: str = "paid", *, mode: str = "live", paid_hours_ago: float = 2,
              total: int = 9199, name: str = "Ash Ketchum") -> None:
    paid = (datetime.now(UTC) - timedelta(hours=paid_hours_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    with connect(settings.db_path) as conn:
        cur = conn.execute(
            "INSERT INTO orders (number, access_token, status, mode, email, name, ship_class, "
            "subtotal_cents, shipping_cents, total_cents, created_at, paid_at) "
            "VALUES (?, 'tok', ?, ?, 'buyer@example.com', ?, 'large-sealed', ?, 2000, ?, ?, ?)",
            (number, status, mode, name, total - 2000, total, paid, paid))
        conn.execute("INSERT INTO order_items (order_id, sku, title, unit_price_cents, quantity) "
                     "VALUES (?, 'JP-AE-BB', 'Booster box', ?, 1)", (cur.lastrowid, total - 2000))


# --- the walls -----------------------------------------------------------------------------

@pytest.mark.parametrize("headers, host, refused", [
    ({}, "192.168.1.50", False),
    ({}, "127.0.0.1", False),
    ({}, "172.18.0.1", False),                       # Docker's gateway, when Docker proxies the port
    ({}, "::ffff:192.168.1.50", False),
    ({}, "8.8.8.8", True),                           # anywhere on the internet
    ({}, "100.87.1.2", True),                        # Tailscale's range isn't "private": not allowed (yet)
    ({}, None, True),
    ({"cf-connecting-ip": "8.8.8.8"}, "172.18.0.5", True),   # through the tunnel, even from inside Docker
    ({"cf-ray": "abc"}, "192.168.1.50", True),
])
def test_refuse_reason(headers, host, refused):
    assert bool(refuse_reason(headers, host)) is refused


def test_internet_and_cloudflare_requests_are_refused(imported, config):
    with TestClient(create_admin_app(imported, config), client=("8.8.8.8", 1234)) as outside:
        assert outside.get("/login").status_code == 403
        assert outside.get("/healthz").status_code == 403
    with TestClient(create_admin_app(imported, config), client=HOME) as home:
        assert home.get("/login", headers={"CF-Connecting-IP": "8.8.8.8"}).status_code == 403
        assert home.get("/login").status_code == 200


def test_pages_need_sign_in(admin):
    response = admin.get("/")
    assert response.status_code == 303 and response.headers["location"] == "/login"
    assert admin.get("/", cookies={COOKIE_NAME: "123.abc.forged"}).status_code == 303


def test_security_headers(admin):
    headers = admin.get("/login").headers
    assert headers["cache-control"] == "no-store"
    assert headers["x-frame-options"] == "DENY"
    assert "frame-ancestors 'none'" in headers["content-security-policy"]
    assert "noindex" in headers["x-robots-tag"]


@pytest.mark.parametrize("origin, allowed", [
    ("https://evil.example", False),
    ("null", False),                         # sandboxed frames and some privacy modes
    ("http://testserver", True),             # the admin's own page (what browsers send)
    (None, True),                            # older clients send no Origin; the form token still guards
])
def test_form_posts_must_come_from_the_admin(admin, origin, allowed):
    headers = {"Origin": origin} if origin else {}
    response = admin.post("/login", data={"password": PASSWORD}, headers=headers)
    assert (response.status_code == 303) is allowed


def test_referrer_policy_lets_browsers_name_the_admin_as_origin(admin):
    # no-referrer would make browsers send `Origin: null` on the admin's own forms (found in the
    # browser check, 2026-11-14), and every sign-in would be refused.
    assert admin.get("/login").headers["referrer-policy"] == "same-origin"


def test_no_password_set_means_no_sign_in(imported, tmp_path):
    with TestClient(create_admin_app(imported, AdminConfig(backup_dir=tmp_path)), client=HOME,
                    follow_redirects=False) as client:
        page = client.get("/login")
        assert "isn't set up yet" in page.text and "storefront.admin.passwd" in page.text
        assert client.post("/login", data={"password": ""}).status_code == 503
        assert client.get("/").status_code == 303
        assert client.get("/healthz").json()["sign_in_ready"] is False


# --- sign-in -------------------------------------------------------------------------------

def test_password_hashes():
    stored = auth.hash_password("hunter2hunter2", iterations=1000)
    assert stored.startswith("pbkdf2_sha256:1000:") and "$" not in stored    # compose-safe in .env
    assert auth.check_password("hunter2hunter2", stored)
    assert not auth.check_password("hunter2hunter3", stored)
    assert not auth.check_password("x", "garbage")
    assert auth.hash_password("same", iterations=1000) != auth.hash_password("same", iterations=1000)   # salted


def test_sign_in_and_out(admin):
    sign_in(admin)
    page = admin.get("/")
    assert page.status_code == 200 and "To ship" in page.text
    cookie = admin.cookies.get(COOKIE_NAME)
    # Signing out needs the page's form token.
    assert admin.post("/logout", data={"form_token": "wrong"}).status_code == 403
    form_token = page.text.split('name="form_token" value="')[1].split('"')[0]
    out = admin.post("/logout", data={"form_token": form_token})
    assert out.status_code == 303 and out.headers["location"] == "/login"
    assert cookie   # the old cookie existed; the browser is told to delete it
    assert 'p6_admin=""' in out.headers["set-cookie"] or "Max-Age=0" in out.headers["set-cookie"]


def test_cookie_flags(admin):
    set_cookie = admin.post("/login", data={"password": PASSWORD}).headers["set-cookie"].lower()
    assert "httponly" in set_cookie and "samesite=strict" in set_cookie and "max-age=2592000" in set_cookie


def test_wrong_passwords_lock_out_and_alert(imported, config, monkeypatch):
    alerts = []
    monkeypatch.setattr("storefront.app.notify.discord", lambda settings, text: alerts.append(text))
    with TestClient(create_admin_app(imported, config), client=HOME, follow_redirects=False) as client:
        for _ in range(auth.MAX_FAILURES):
            assert client.post("/login", data={"password": "nope"}).status_code == 401
        # Locked now: even the right password is refused for a while.
        assert client.post("/login", data={"password": PASSWORD}).status_code == 429
    assert len(alerts) == 1 and "locked" in alerts[0]


def test_sessions_expire_and_follow_the_password(password_hash):
    sessions = auth.Sessions(SECRET, password_hash)
    cookie = sessions.new()
    assert sessions.valid(cookie)
    assert not sessions.valid(cookie, now=datetime.now(UTC).timestamp() + (auth.SESSION_DAYS + 1) * 86400)
    assert not sessions.valid(cookie + "0")
    assert not sessions.valid("ünïcode.cookie.value")
    # A new password (or secret) signs every device out.
    assert not auth.Sessions(SECRET, auth.hash_password("another password", iterations=1000)).valid(cookie)
    assert not auth.Sessions("t" * 64, password_hash).valid(cookie)


def test_lockout_window():
    lock = auth.Lockout()
    start = 1_000_000.0
    for i in range(auth.MAX_FAILURES - 1):
        assert not lock.failed("a", now=start + i)
    # Old failures drop out of the window, so slow typos never lock anyone out.
    assert not lock.failed("a", now=start + auth.FAILURE_WINDOW_SECONDS + 10)
    assert not lock.locked("a", now=start + auth.FAILURE_WINDOW_SECONDS + 10)


def test_password_tool_updates_env(tmp_path):
    env = tmp_path / ".env"
    env.write_text("STRIPE_SECRET_KEY=keep-me\nSTOREFRONT_ADMIN_PASSWORD_HASH=old\n")
    os.chmod(env, 0o600)
    update_env(env, {"STOREFRONT_ADMIN_PASSWORD_HASH": "new", "STOREFRONT_ADMIN_SESSION_SECRET": "first"},
               keep_existing=("STOREFRONT_ADMIN_SESSION_SECRET",))
    update_env(env, {"STOREFRONT_ADMIN_PASSWORD_HASH": "newer", "STOREFRONT_ADMIN_SESSION_SECRET": "second"},
               keep_existing=("STOREFRONT_ADMIN_SESSION_SECRET",))
    lines = env.read_text().splitlines()
    assert "STRIPE_SECRET_KEY=keep-me" in lines
    assert "STOREFRONT_ADMIN_PASSWORD_HASH=newer" in lines
    assert "STOREFRONT_ADMIN_SESSION_SECRET=first" in lines     # the secret is kept once set
    assert env.stat().st_mode & 0o777 == 0o600


# --- overview ------------------------------------------------------------------------------

def test_overview_lists_orders_to_ship_and_flags_late_ones(admin, imported):
    add_order(imported, "P6-2026-0101", paid_hours_ago=30, name="Late Lucy")
    add_order(imported, "P6-2026-0102", paid_hours_ago=2, name="Prompt Pete")
    add_order(imported, "P6-2026-0103", status="shipped")
    add_order(imported, "P6T-2026-0104", mode="test")
    add_order(imported, "P6-2026-0105", status="expired")
    sign_in(admin)
    page = admin.get("/").text
    ship_section = page.split("<h2>To ship</h2>")[1].split("</section>")[0]
    assert ship_section.index("Late Lucy") < ship_section.index("Prompt Pete")      # oldest first
    assert "P6-2026-0103" not in ship_section and "P6T-2026-0104" in ship_section
    assert "30 h waiting" in ship_section
    assert "P6-2026-0105" not in page                                               # never a sale


def test_month_totals_leave_out_test_and_unpaid_orders(imported):
    add_order(imported, "P6-2026-0201", total=10000)
    add_order(imported, "P6-2026-0202", status="shipped", total=5000)
    add_order(imported, "P6T-2026-0203", mode="test", total=99999)
    add_order(imported, "P6-2026-0204", status="expired", total=77777)
    with connect(imported.db_path, readonly=True) as conn:
        month = overview.month_totals(conn)
    assert (month["orders"], month["sales"]) == (2, 15000)


def test_overview_flags_disputes_failed_emails_and_old_backups(admin, imported, config):
    add_order(imported, "P6-2026-0301", status="disputed")
    failed = imported.outbox_dir / "failed"
    failed.mkdir(parents=True)
    (failed / "x.json").write_text("{}")
    old = config.backup_dir / "2026-01-01"
    old.mkdir(parents=True)
    (old / "manifest.json").write_text(json.dumps({"taken_at": "2026-01-01T03:15:00+00:00",
                                                  "files": {"store.db": {"rows": {"orders": 3}}}}))
    sign_in(admin)
    page = admin.get("/").text
    assert "Dispute on P6-2026-0301" in page
    assert "1 order email gave up" in page
    assert "The last backup is" in page and "days old" in page


def test_recent_backup_is_all_clear(imported, config):
    day = datetime.now(UTC)
    folder = config.backup_dir / day.date().isoformat()
    folder.mkdir(parents=True)
    (folder / "manifest.json").write_text(json.dumps({"taken_at": (day - timedelta(hours=3)).isoformat(),
                                                     "files": {"store.db": {"rows": {"orders": 4, "products": 9}}}}))
    info = overview.last_backup(config.backup_dir)
    assert info["found"] and not info["stale"] and info["rows"]["products"] == 9
    assert overview.last_backup(config.backup_dir / "missing") == {"found": False, "stale": True}


def test_stock_lists(imported):
    with connect(imported.db_path) as conn:
        conn.execute("UPDATE products SET stock = 0 WHERE sku = 'JP-PACK-A'")
    with connect(imported.db_path, readonly=True) as conn:
        sold_out = {row["sku"] for row in overview.sold_out(conn)}
        last = overview.last_one(conn)
    assert sold_out == {"JP-PACK-A"}    # not JP-BB-BOX: hidden; not 111: stock not tracked
    assert last == 1                    # ENG-BB-BB, visible with 1 left


def test_month_is_net_of_refunds(imported):
    add_order(imported, "P6-2026-0401", status="refunded", total=2073)
    with connect(imported.db_path) as conn:
        conn.execute("UPDATE orders SET refunded_cents = 2073 WHERE number = 'P6-2026-0401'")
    with connect(imported.db_path, readonly=True) as conn:
        month = overview.month_totals(conn)
    assert (month["orders"], month["sales"], month["refunded"], month["net"]) == (1, 2073, 2073, 0)


def test_healthz(admin):
    body = admin.get("/healthz").json()
    assert body["ok"] and body["sign_in_ready"] and body["schema"]
