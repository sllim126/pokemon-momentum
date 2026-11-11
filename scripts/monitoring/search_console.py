"""Google Search Console numbers for poke6s.com, for the "Google search" row of Grafana's Store dashboard.

collect_status.py calls collect() every minute. Most runs only turn the cached answers into
metrics; the API is called only when the cache is old:

- every REFRESH_SECONDS (6 hours): clicks, impressions and position per day for the last
  DAYS days, the top searches, pages and devices over those days, and the sitemap status;
- every run, up to INSPECT_PER_RUN pages from the store's sitemap whose index status is
  older than INSPECT_EVERY (a day) go through the URL Inspection API. So each page is checked
  about once a day, spread over the day, well inside Google's 2,000-a-day quota.

After a failed call the next attempt waits RETRY_SECONDS, so a Google outage costs one call
an hour, not one a minute. The answers live in CACHE_FILE (under data/, never committed).

Signing in: a Google Cloud service account (project market-poke6s) that the owner added to
the Search Console property as a Restricted (read-only) user. Its JSON key is KEY_FILE,
mode 600, in the git-ignored secrets/ folder. The host has no Google libraries, so the
sign-in token (an RS256 JWT) is signed with the openssl command: the private key goes to
openssl through a pipe and is never written to disk, and neither the key nor the access token
is logged or cached.

Google's numbers run 2-3 days behind; rows for the newest days are partial (dataState "all").
Searches made by very few people are hidden by Google, so top searches don't add up to the
totals. Free API; no cost.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SITE = os.environ.get("MOMENTUM_SEARCH_CONSOLE_SITE", "https://poke6s.com/")
KEY_FILE = Path(os.environ.get("MOMENTUM_SEARCH_CONSOLE_KEY", str(ROOT / "secrets" / "search-console.json")))
CACHE_FILE = Path(os.environ.get("MOMENTUM_SEARCH_CONSOLE_CACHE",
                                 str(ROOT / "data" / "search_console" / "cache.json")))
# The store's own sitemap, read on this host, lists the pages to inspect.
SITEMAP_URL = os.environ.get("MOMENTUM_SEARCH_CONSOLE_SITEMAP", "http://127.0.0.1:8010/sitemap.xml")
SCOPE = "https://www.googleapis.com/auth/webmasters.readonly"
API = "https://www.googleapis.com/webmasters/v3"
INSPECT_API = "https://searchconsole.googleapis.com/v1/urlInspection/index:inspect"

DAYS = 28
TOP = 20                     # rows kept for top searches and top pages
REFRESH_SECONDS = 6 * 3600
RETRY_SECONDS = 3600
INSPECT_PER_RUN = 2         # each inspection takes ~6 s; collect_status.py runs every minute
INSPECT_EVERY = 24 * 3600
TIMEOUT = 20


class SearchConsoleError(Exception):
    """An API call failed. The message holds the HTTP status and Google's reason, never a token."""


# ---------------------------------------------------------------- signing in

def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _sign_rs256(message: bytes, private_key_pem: str) -> bytes:
    """RS256 signature from the openssl command, with the key passed through a pipe (fd 3)."""
    read_fd, write_fd = os.pipe()
    try:
        proc = subprocess.Popen(["openssl", "dgst", "-sha256", "-sign", f"/dev/fd/{read_fd}"],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                pass_fds=(read_fd,))
        os.close(read_fd)
        read_fd = -1
        with os.fdopen(write_fd, "w") as key_pipe:  # the key is small; it fits in the pipe buffer
            write_fd = -1
            key_pipe.write(private_key_pem)
        signature, err = proc.communicate(message, timeout=TIMEOUT)
    finally:
        for fd in (read_fd, write_fd):
            if fd >= 0:
                os.close(fd)
    if proc.returncode != 0 or not signature:
        raise SearchConsoleError(f"openssl could not sign the sign-in token (exit {proc.returncode})")
    return signature


def access_token(key: dict, now: float | None = None) -> str:
    """Trade a signed JWT for a one-hour access token (Google's service-account flow)."""
    now = int(now or time.time())
    header = _b64(json.dumps({"alg": "RS256", "typ": "JWT", "kid": key.get("private_key_id", "")}).encode())
    claims = _b64(json.dumps({"iss": key["client_email"], "scope": SCOPE, "aud": key["token_uri"],
                              "iat": now, "exp": now + 3600}).encode())
    unsigned = f"{header}.{claims}".encode()
    assertion = f"{header}.{claims}.{_b64(_sign_rs256(unsigned, key['private_key']))}"
    body = urllib.parse.urlencode({"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                                   "assertion": assertion}).encode()
    answer = _http("POST", key["token_uri"], body, {"Content-Type": "application/x-www-form-urlencoded"})
    return answer["access_token"]


def _http(method: str, url: str, body: bytes | None, headers: dict) -> dict:
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            return json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        try:
            reason = json.loads(exc.read()).get("error", {})
            reason = reason.get("message", reason) if isinstance(reason, dict) else reason
        except ValueError:
            reason = ""
        raise SearchConsoleError(f"HTTP {exc.code} from {urllib.parse.urlsplit(url).path}: {str(reason)[:200]}") from None


class Client:
    def __init__(self, token: str):
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def get(self, url: str) -> dict:
        return _http("GET", url, None, self.headers)

    def post(self, url: str, payload: dict) -> dict:
        return _http("POST", url, json.dumps(payload).encode(), self.headers)


def _site_path() -> str:
    return f"{API}/sites/{urllib.parse.quote(SITE, safe='')}"


# ---------------------------------------------------------------- fetching

def fetch_summary(client: Client, today: date) -> dict:
    """Daily totals, top searches, pages and devices for the last DAYS days, and the sitemaps."""
    start, end = (today - timedelta(days=DAYS)).isoformat(), today.isoformat()

    def query(dimensions: list[str], limit: int = 1000) -> list[dict]:
        answer = client.post(f"{_site_path()}/searchAnalytics/query",
                             {"startDate": start, "endDate": end, "dimensions": dimensions,
                              "rowLimit": limit, "dataState": "all"})
        return answer.get("rows", [])

    def rows(dimension: str, limit: int) -> list[dict]:
        return [{"key": r["keys"][0], "clicks": r["clicks"], "impressions": r["impressions"],
                 "position": r["position"]} for r in query([dimension], limit)]

    daily = {r["key"]: {k: r[k] for k in ("clicks", "impressions", "position")} for r in rows("date", 1000)}
    pages = rows("page", TOP)
    for page in pages:  # the path is enough: every page is on SITE
        page["key"] = urllib.parse.urlsplit(page["key"]).path or "/"
    sitemaps = [{"path": s.get("path", ""), "last_read": s.get("lastDownloaded"),
                 "pending": bool(s.get("isPending")), "errors": int(s.get("errors", 0) or 0),
                 "warnings": int(s.get("warnings", 0) or 0),
                 "submitted": sum(int(c.get("submitted", 0) or 0) for c in s.get("contents", []))}
                for s in client.get(f"{_site_path()}/sitemaps").get("sitemap", [])]
    return {"start": start, "end": end, "daily": daily, "queries": rows("query", TOP), "pages": pages,
            "devices": rows("device", 10), "sitemaps": sitemaps}


def sitemap_paths(url: str = SITEMAP_URL) -> list[str]:
    """Page paths in the store's sitemap (read locally, so it's the new store's list)."""
    with urllib.request.urlopen(url, timeout=TIMEOUT) as response:
        tree = ET.fromstring(response.read())
    locs = [el.text.strip() for el in tree.iter() if el.tag.endswith("loc") and el.text]
    return [urllib.parse.urlsplit(loc).path or "/" for loc in locs]


def inspect(client: Client, path: str) -> dict:
    """Google's index status for one page: its coverage state, verdict and last crawl."""
    answer = client.post(INSPECT_API, {"inspectionUrl": urllib.parse.urljoin(SITE, path), "siteUrl": SITE})
    status = answer.get("inspectionResult", {}).get("indexStatusResult", {})
    return {"verdict": status.get("verdict", "VERDICT_UNSPECIFIED"),
            "coverage": status.get("coverageState", "unknown"),
            "last_crawl": status.get("lastCrawlTime")}


# ---------------------------------------------------------------- the cache

def load_cache() -> dict:
    try:
        return json.loads(CACHE_FILE.read_text())
    except (OSError, ValueError):
        return {}


def save_cache(cache: dict) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_FILE.with_name(f".{CACHE_FILE.name}.tmp")
    tmp.write_text(json.dumps(cache, indent=1, sort_keys=True))
    tmp.replace(CACHE_FILE)


def _due(pages: dict, now: float) -> list[str]:
    """The pages to inspect this run: never checked or checked over a day ago, oldest first."""
    stale = [p for p, info in pages.items() if now - info.get("checked_at", 0) >= INSPECT_EVERY]
    return sorted(stale, key=lambda p: pages[p].get("checked_at", 0))[:INSPECT_PER_RUN]


def update(cache: dict, now: float, token_for=None, sitemap=sitemap_paths) -> dict:
    """Call the API for whatever is due and record the result in cache. Returns cache.

    token_for(key) -> access token; tests replace it. Errors are recorded in cache["error"]
    and re-raised, so collect_status.py reports the section as failed.
    """
    summary_due = now - cache.get("summary_at", 0) >= REFRESH_SECONDS
    pages = cache.setdefault("pages", {})
    due = _due(pages, now)
    if not summary_due and not due:
        return cache
    if now - cache.get("failed_at", 0) < RETRY_SECONDS:
        return cache
    try:
        key = json.loads(KEY_FILE.read_text())
        client = Client((token_for or access_token)(key))
        if summary_due:
            today = datetime.fromtimestamp(now, timezone.utc).date()
            cache["summary"] = fetch_summary(client, today)
            # The page list follows the store's sitemap: new pages join, removed ones drop out.
            listed = set(sitemap())
            for path in listed - set(pages):
                pages[path] = {}
            for path in set(pages) - listed:
                del pages[path]
            cache["summary_at"] = now
            due = _due(pages, now)
        for path in due:
            pages[path] = {**inspect(client, path), "checked_at": now}
        cache.pop("error", None)
        cache.pop("failed_at", None)
    except Exception as exc:
        cache["error"] = f"{type(exc).__name__}: {exc}"[:300]
        cache["failed_at"] = now
        raise
    return cache


# ---------------------------------------------------------------- metrics

def _ts(value: str | None) -> float | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def metrics(m, cache: dict, now: float) -> None:
    """Turn the cache into momentum_search_* metrics (m is collect_status.Metrics)."""
    summary = cache.get("summary")
    m.add("momentum_search_key_present", 1 if KEY_FILE.exists() else 0,
          "1 when the Search Console service-account key file exists")
    m.add("momentum_search_last_error", 1 if cache.get("error") else 0,
          "1 when the last Search Console API call failed (details in the collector log)")
    if cache.get("summary_at"):
        m.add("momentum_search_updated_timestamp_seconds", cache["summary_at"],
              "When the Search Console totals were last fetched")
    if not summary:
        return

    # Every day of the window gets a value (0 with no impressions) so the bars have no gaps.
    first = date.fromisoformat(summary["start"])
    last = date.fromisoformat(summary["end"])
    daily = summary["daily"]
    days = [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]
    newest = max(daily, default=None)
    for day in days:
        if newest and day > newest:
            break  # Google hasn't reported these days yet; no bar rather than a false zero
        row = daily.get(day, {"clicks": 0, "impressions": 0})
        m.add("momentum_search_daily_clicks", row["clicks"], "Google search clicks per day (UTC)", day=day)
        m.add("momentum_search_daily_impressions", row["impressions"],
              "Times a poke6s.com page appeared in Google results, per day", day=day)
        if row.get("impressions"):
            m.add("momentum_search_daily_position", row["position"],
                  "Average position in Google results per day (1 = top)", day=day)
    if newest:
        m.add("momentum_search_data_through_timestamp_seconds", _ts(newest + "T00:00:00Z"),
              "The newest day Google has reported (partial for the last 2-3 days)")

    # Window totals, because PromQL can't tell which day label is the newest.
    for window, n in (("7d", 7), ("28d", DAYS)):
        recent = [daily[d] for d in days[-n:] if d in daily]
        clicks = sum(r["clicks"] for r in recent)
        shown = sum(r["impressions"] for r in recent)
        m.add("momentum_search_clicks", clicks, "Google search clicks, by window", window=window)
        m.add("momentum_search_impressions", shown, "Google search impressions, by window", window=window)
        if shown:
            m.add("momentum_search_position", sum(r["position"] * r["impressions"] for r in recent) / shown,
                  "Average position weighted by impressions, by window", window=window)

    for kind, rows in (("query", summary["queries"]), ("page", summary["pages"]), ("device", summary["devices"])):
        for r in rows:
            labels = {kind: r["key"]}
            m.add(f"momentum_search_top_{kind}_clicks", r["clicks"], f"Clicks over {DAYS} days by {kind}", **labels)
            m.add(f"momentum_search_top_{kind}_impressions", r["impressions"],
                  f"Impressions over {DAYS} days by {kind}", **labels)
            m.add(f"momentum_search_top_{kind}_position", r["position"],
                  f"Average position over {DAYS} days by {kind}", **labels)

    for s in summary["sitemaps"]:
        labels = {"sitemap": urllib.parse.urlsplit(s["path"]).path or s["path"]}
        m.add("momentum_search_sitemap_submitted", s["submitted"], "Pages listed in a submitted sitemap", **labels)
        m.add("momentum_search_sitemap_errors", s["errors"], "Errors Google found reading the sitemap", **labels)
        m.add("momentum_search_sitemap_warnings", s["warnings"], "Warnings Google found reading the sitemap",
              **labels)
        if s["last_read"]:
            m.add("momentum_search_sitemap_last_read_timestamp_seconds", _ts(s["last_read"]),
                  "When Google last read the sitemap", **labels)

    pages = cache.get("pages", {})
    checked = {p: info for p, info in pages.items() if info.get("checked_at")}
    m.add("momentum_search_pages_in_sitemap", len(pages), "Pages in the store's sitemap")
    m.add("momentum_search_pages_checked", len(checked), "Sitemap pages whose index status has been checked")
    m.add("momentum_search_pages_indexed", sum(1 for i in checked.values() if i["verdict"] == "PASS"),
          "Checked sitemap pages that Google has indexed")
    for path, info in checked.items():
        m.add("momentum_search_page_indexed", 1 if info["verdict"] == "PASS" else 0,
              "1 when Google has indexed this page; coverage is Google's own wording",
              path=path, coverage=info["coverage"])
        if info.get("last_crawl"):
            m.add("momentum_search_page_last_crawl_timestamp_seconds", _ts(info["last_crawl"]),
                  "When Google last crawled this page", path=path)


def collect(m, now: float | None = None) -> None:
    """The collect_status.py section: refresh what's due, then write the metrics.

    Metrics are written from the cache even when the API call fails, so a Google outage
    leaves the last good numbers on the dashboard (with the "Search data updated" tile ageing).
    """
    now = now or time.time()
    if not KEY_FILE.exists():
        m.add("momentum_search_key_present", 0, "1 when the Search Console service-account key file exists")
        return
    cache = load_cache()
    try:
        update(cache, now)
    finally:
        save_cache(cache)
        metrics(m, cache, now)
