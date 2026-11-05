"""Count visitors and page views per day, privately, for the Grafana "Visitors" dashboard.

No cookies, no tracking script, nothing sent to anyone: the server counts what it already
sees. The market app has its own copy of this file (scripts/dashboards/visitors.py), because
this container can see only storefront/; keep the two in step.

What counts:

* A **page view** is a GET that returns a full HTML page (status 200, text/html). API calls,
  images, health checks, redirects and errors don't count, and neither do bots, crawlers,
  link previews (Discord, Facebook...), monitoring probes or browser prefetches.
* A **visitor** is one person per UTC day. The server never stores who they are. It keeps a
  SHA-256 hash of (today's random salt + IP address + browser). The salt is new every day
  and deleted with that day's hashes, after which the hashes can't be linked to anyone, or to
  the same person on another day. Only the daily totals are kept (400 days).
* For each new visitor it also notes where they came from (the other site's domain from the
  Referer header, or "(direct)") and the kind of device (phone, tablet, desktop). For each
  page view it notes the page path (no query string).

The real visitor address: public traffic comes through the Caddy proxy (192.168.1.30), which
puts it last in X-Forwarded-For. That header is trusted only when the request itself comes
from the private network, since nobody on the internet can reach this server directly.

Limits worth knowing: people behind the same router with the same browser count as one;
someone who switches from Wi-Fi to phone data counts twice. Fine for "how many people came
today", not for anything exact.
"""

from __future__ import annotations

import hashlib
import ipaddress
import re
import secrets
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

from prometheus_client.core import REGISTRY, GaugeMetricFamily

KEEP_DAYS = 400
TOP_N = 20  # rows per breakdown sent to Prometheus (pages, referrers)
CACHE_SECONDS = 60  # how often a scrape re-reads the database

_BOT = re.compile(
    r"bot|crawl|spider|slurp|archiver|scrap|fetch|preview|monitor|probe|exporter|uptime|"
    r"pingdom|headless|lighthouse|curl|wget|python|httpx|aiohttp|go-http|java/|okhttp|axios|"
    r"node-fetch|libwww|facebookexternalhit|whatsapp/|embedly|"
    r"bingpreview|google-inspectiontool|chrome-lighthouse|^$",
    re.I,
)
_PRIVATE = [ipaddress.ip_network(n) for n in
            ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "127.0.0.0/8", "::1/128", "fc00::/7")]
# Our own sites: moving between them is not a referral.
_OWN_DOMAINS = ("poke6s.com",)


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def device_of(user_agent: str) -> str:
    ua = user_agent.lower()
    if "ipad" in ua or "tablet" in ua or ("android" in ua and "mobile" not in ua):
        return "tablet"
    if any(m in ua for m in ("iphone", "ipod", "mobile", "android")):
        return "phone"
    return "desktop"


def referrer_of(referer: str, own_host: str) -> str:
    """The referring site's domain ('google', 'reddit.com'...), or '(direct)'."""
    host = (urlsplit(referer).hostname or "").lower() if referer else ""
    if host.startswith("www."):
        host = host[4:]
    if not host or host == own_host or any(host == d or host.endswith("." + d) for d in _OWN_DOMAINS):
        return "(direct)"
    # google.com, google.co.uk, ... are all Google.
    if re.match(r"^google\.[a-z.]+$", host):
        return "google"
    return host[:80]


def client_ip(scope) -> str:
    peer = (scope.get("client") or ("", 0))[0]
    headers = dict(scope.get("headers") or [])
    forwarded = headers.get(b"x-forwarded-for", b"").decode("latin-1")
    try:
        from_private = any(ipaddress.ip_address(peer) in net for net in _PRIVATE)
    except ValueError:
        from_private = False
    if forwarded and from_private:
        # Caddy appends the address it saw, so the last entry is the one to trust.
        return forwarded.split(",")[-1].strip()
    return peer


class VisitorTracker:
    """Owns the small SQLite file; every method is safe to call from any request."""

    def __init__(self, db_path: Path, app_name: str):
        self.db_path = Path(db_path)
        self.app_name = app_name
        self._lock = threading.Lock()
        self._day = None
        self._salt = b""
        self._cache = (0.0, [])
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS salts (day TEXT PRIMARY KEY, salt BLOB NOT NULL);
                -- Today's visitor hashes only; older days are deleted at the first visit of a new day.
                CREATE TABLE IF NOT EXISTS seen (day TEXT NOT NULL, hash TEXT NOT NULL,
                                                 PRIMARY KEY (day, hash));
                -- Daily totals: kind is 'pageviews', 'visitors', 'bot_pageviews', 'page',
                -- 'referrer' or 'device'; key is '' for the totals.
                CREATE TABLE IF NOT EXISTS daily (day TEXT NOT NULL, kind TEXT NOT NULL,
                                                  key TEXT NOT NULL, count INTEGER NOT NULL,
                                                  PRIMARY KEY (day, kind, key));
                """
            )

    @contextmanager
    def _db(self):
        """One short-lived connection: committed on success, always closed."""
        with closing(sqlite3.connect(self.db_path, timeout=5)) as con:
            con.execute("PRAGMA journal_mode=WAL")
            with con:  # commits, or rolls back on error
                yield con

    def _salt_for(self, con: sqlite3.Connection, day: str) -> bytes:
        if self._day != day:
            row = con.execute("SELECT salt FROM salts WHERE day = ?", (day,)).fetchone()
            if row is None:
                # A new day: forget yesterday's salt and hashes, and old totals.
                con.execute("DELETE FROM salts WHERE day < ?", (day,))
                con.execute("DELETE FROM seen WHERE day < ?", (day,))
                con.execute("DELETE FROM daily WHERE day < date(?, ?)", (day, f"-{KEEP_DAYS} days"))
                salt = secrets.token_bytes(32)
                con.execute("INSERT INTO salts (day, salt) VALUES (?, ?)", (day, salt))
            else:
                salt = row[0]
            self._day, self._salt = day, salt
        return self._salt

    @staticmethod
    def _bump(con, day, kind, key=""):
        con.execute(
            "INSERT INTO daily (day, kind, key, count) VALUES (?, ?, ?, 1) "
            "ON CONFLICT (day, kind, key) DO UPDATE SET count = count + 1",
            (day, kind, key),
        )

    def record(self, *, ip: str, user_agent: str, path: str, referer: str, own_host: str) -> None:
        day = _today()
        with self._lock, self._db() as con:
            if _BOT.search(user_agent or ""):
                self._bump(con, day, "bot_pageviews")
                return
            self._bump(con, day, "pageviews")
            self._bump(con, day, "page", path[:120])
            salt = self._salt_for(con, day)
            digest = hashlib.sha256(salt + f"{ip}|{user_agent}".encode()).hexdigest()
            new = con.execute("INSERT OR IGNORE INTO seen (day, hash) VALUES (?, ?)", (day, digest)).rowcount
            if new:
                self._bump(con, day, "visitors")
                self._bump(con, day, "referrer", referrer_of(referer, own_host))
                self._bump(con, day, "device", device_of(user_agent))

    # --- Prometheus ------------------------------------------------------------------------

    def collect(self):
        """Called by prometheus_client on every scrape; re-reads the database at most once a
        minute. Series: per-day totals for the last 35 days (label day), and the last 7 days'
        breakdowns (top pages, referrers, devices)."""
        now = time.time()
        if now - self._cache[0] > CACHE_SECONDS:
            try:
                self._cache = (now, self._read_families())
            except sqlite3.Error:
                pass  # keep the last good numbers rather than break the scrape
        yield from self._cache[1]

    def describe(self):
        return []  # skip the registry's describe-time collect (it would open the database)

    def _read_families(self):
        app = self.app_name
        with self._db() as con:
            daily = con.execute(
                "SELECT day, kind, count FROM daily WHERE key = '' AND day >= date('now', '-35 days')"
            ).fetchall()
            breakdown = {
                kind: con.execute(
                    "SELECT key, sum(count) AS n FROM daily WHERE kind = ? AND day >= date('now', '-6 days') "
                    "GROUP BY key ORDER BY n DESC LIMIT ?", (kind, TOP_N)).fetchall()
                for kind in ("page", "referrer", "device")
            }
        today = _today()
        per_day = GaugeMetricFamily("site_daily_total", "Visitors and page views per UTC day (last 35 days)",
                                    labels=["app", "kind", "day"])
        today_f = GaugeMetricFamily("site_today_total", "Visitors and page views so far today (UTC)",
                                    labels=["app", "kind"])
        seen_today = {"pageviews": 0, "visitors": 0, "bot_pageviews": 0}
        for day, kind, count in daily:
            per_day.add_metric([app, kind, day], count)
            if day == today:
                seen_today[kind] = count
        for kind, count in seen_today.items():
            today_f.add_metric([app, kind], count)
        # Sums over whole days, so Grafana needn't add up the day-labelled series. A person who
        # comes on two days is two visits in the 7- and 30-day sums.
        periods = GaugeMetricFamily("site_period_total",
                                    "Visitors and page views: yesterday, last 7 and last 30 days including today",
                                    labels=["app", "kind", "period"])
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        week_start = (datetime.now(timezone.utc) - timedelta(days=6)).strftime("%Y-%m-%d")
        month_start = (datetime.now(timezone.utc) - timedelta(days=29)).strftime("%Y-%m-%d")
        for kind in ("visitors", "pageviews"):
            rows = [(d, c) for d, k, c in daily if k == kind]
            periods.add_metric([app, kind, "yesterday"], sum(c for d, c in rows if d == yesterday))
            periods.add_metric([app, kind, "7d"], sum(c for d, c in rows if d >= week_start))
            periods.add_metric([app, kind, "30d"], sum(c for d, c in rows if d >= month_start))
        week = GaugeMetricFamily("site_week_breakdown",
                                 "Last 7 days: page views per page, new visitors per referrer and device (top 20)",
                                 labels=["app", "kind", "key"])
        for kind, rows in breakdown.items():
            for key, n in rows:
                week.add_metric([app, kind, key], n)
        return [per_day, today_f, periods, week]


class _PageViewMiddleware:
    """Pure ASGI: looks at the response status and content type, then records page views."""

    def __init__(self, app, tracker: VisitorTracker):
        self.app = app
        self.tracker = tracker

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("method") != "GET":
            await self.app(scope, receive, send)
            return
        seen = {}

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                seen["status"] = message["status"]
                seen["html"] = any(k.lower() == b"content-type" and v.startswith(b"text/html")
                                   for k, v in message.get("headers", []))
            await send(message)

        await self.app(scope, receive, send_wrapper)
        if seen.get("status") != 200 or not seen.get("html"):
            return
        ip = client_ip(scope)
        try:
            ipaddress.ip_address(ip)
        except ValueError:
            # Real requests always come from an address. The test suite's client calls
            # itself "testclient"; its requests must not land in the real counts.
            return
        headers = dict(scope.get("headers") or [])
        # Browsers announce link prefetches; those aren't views.
        if b"prefetch" in headers.get(b"sec-purpose", b"") or b"prefetch" in headers.get(b"purpose", b""):
            return
        try:
            self.tracker.record(
                ip=ip,
                user_agent=headers.get(b"user-agent", b"").decode("latin-1"),
                path=scope.get("path", "/"),
                referer=headers.get(b"referer", b"").decode("latin-1"),
                own_host=headers.get(b"host", b"").decode("latin-1").split(":")[0].lower().removeprefix("www."),
            )
        except Exception:  # counting must never break a page
            pass


class _LatestTracker:
    """The one collector registered per app name. It reads from whichever tracker was
    installed last, so building the app again (as the tests do) doesn't add duplicates."""

    def __init__(self):
        self.tracker = None

    def collect(self):
        if self.tracker is not None:
            yield from self.tracker.collect()

    def describe(self):
        return []


_COLLECTORS: dict[str, _LatestTracker] = {}


def install_visitor_counting(app, app_name: str, db_path: Path) -> VisitorTracker:
    """Count page views and visitors for this app and publish them on /metrics."""
    tracker = VisitorTracker(db_path, app_name)
    app.add_middleware(_PageViewMiddleware, tracker=tracker)
    if app_name not in _COLLECTORS:
        _COLLECTORS[app_name] = _LatestTracker()
        REGISTRY.register(_COLLECTORS[app_name])
    _COLLECTORS[app_name].tracker = tracker
    return tracker
