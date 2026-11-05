"""Private visitor counting (scripts/dashboards/visitors.py)."""

import sqlite3
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.testclient import TestClient

from scripts.dashboards import visitors

BROWSER = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36"
PHONE = "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148"


def totals(db, kind):
    con = sqlite3.connect(db)
    try:
        return {key: n for key, n in con.execute("SELECT key, count FROM daily WHERE kind = ?", (kind,))}
    finally:
        con.close()


class VisitorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "visitors.sqlite3"
        app = FastAPI()

        @app.get("/")
        def home():
            return HTMLResponse("<html>home</html>")

        @app.get("/api/data")
        def data():
            return JSONResponse({"ok": True})

        self.tracker = visitors.install_visitor_counting(app, "test-app", self.db)
        # Requests arrive from the Caddy proxy on the LAN, which names the real visitor.
        self.client = TestClient(app, client=("192.168.1.30", 40000))

    def tearDown(self):
        self.tmp.cleanup()

    def get(self, path="/", ip="203.0.113.5", ua=BROWSER, **headers):
        return self.client.get(path, headers={"User-Agent": ua, "X-Forwarded-For": ip, **headers})

    def test_one_person_many_pages_is_one_visitor(self):
        for _ in range(3):
            self.get()
        self.get(ip="198.51.100.7", ua=PHONE)
        self.assertEqual(totals(self.db, "pageviews")[""], 4)
        self.assertEqual(totals(self.db, "visitors")[""], 2)
        self.assertEqual(totals(self.db, "device"), {"desktop": 1, "phone": 1})

    def test_bots_api_calls_and_prefetches_are_not_page_views(self):
        self.get(ua="Mozilla/5.0 (compatible; Googlebot/2.1)")
        self.get(ua="Blackbox Exporter/0.24.0")
        self.get("/api/data")
        self.get(**{"Sec-Purpose": "prefetch"})
        self.assertNotIn("", totals(self.db, "pageviews"))
        self.assertEqual(totals(self.db, "bot_pageviews")[""], 2)

    def test_referrer_is_the_other_sites_domain(self):
        self.get(Referer="https://www.google.co.uk/search?q=pokemon")
        self.get(ip="198.51.100.8", Referer="https://www.reddit.com/r/PokemonTCG/")
        self.get(ip="198.51.100.9", Referer="https://market.poke6s.com/research")
        self.assertEqual(totals(self.db, "referrer"), {"google": 1, "reddit.com": 1, "(direct)": 1})

    def test_no_addresses_are_stored(self):
        self.get(ip="203.0.113.77")
        raw = self.db.read_bytes() + (self.db.with_name(self.db.name + "-wal").read_bytes()
                                      if self.db.with_name(self.db.name + "-wal").exists() else b"")
        self.assertNotIn(b"203.0.113.77", raw)

    def test_forwarded_header_is_ignored_from_the_internet(self):
        # A request straight from a public address can't choose its own identity.
        self.assertEqual(visitors.client_ip({"client": ("203.0.113.9", 1), "headers": [(b"x-forwarded-for", b"1.2.3.4")]}),
                         "203.0.113.9")

    def test_metrics_show_today(self):
        self.get()
        families = {f.name: f for f in self.tracker._read_families()}
        today = {s.labels["kind"]: s.value for s in families["site_today_total"].samples}
        self.assertEqual(today["visitors"], 1)
        self.assertEqual(today["pageviews"], 1)


if __name__ == "__main__":
    unittest.main()
