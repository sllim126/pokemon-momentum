"""Host-side monitoring scripts (scripts/monitoring/, deploy/monitoring/zfs/): todo counts,
job status files, Plex, PBS, the storefront, Search Console and ZFS pool parsing."""

import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

MONITORING = Path(__file__).resolve().parents[1] / "scripts" / "monitoring"


def _load(name):
    spec = importlib.util.spec_from_file_location(name, MONITORING / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_from(folder, name):
    path = Path(__file__).resolve().parents[1] / folder / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


collect_status = _load("collect_status")
run_job = _load("run_job")
search_console = _load("search_console")


class TodoCountTests(unittest.TestCase):
    def test_counts_open_items_but_not_the_legend_or_recently_completed(self):
        text = (
            "Status tags used below:\n\n  [next]  pick these up first\n\n"
            "Recently Completed\n------------------\n\n- [next] finished already\n\n"
            "Website\n-------\n\n- [next] one\n  - [next] nested two\n- [waiting] three\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "docs").mkdir()
            (root / "docs" / "todo.txt").write_text("Last updated: 2026-11-05\n\n" + text)
            original, collect_status.ROOT = collect_status.ROOT, root
            try:
                metrics = collect_status.Metrics()
                collect_status.todo(metrics)
            finally:
                collect_status.ROOT = original
        out = metrics.text()
        self.assertIn('momentum_todo_items{status="next"} 2.0', out)
        self.assertIn('momentum_todo_items{status="waiting"} 1.0', out)


class PlexTests(unittest.TestCase):
    def test_counts_streams_transcodes_and_libraries_without_a_real_server(self):
        replies = {
            "/status/sessions": {"Metadata": [
                {"Player": {"state": "playing"}, "TranscodeSession": {}},
                {"Player": {"state": "paused"}},
            ]},
            "/library/sections": {"Directory": [{"key": "1", "type": "movie", "title": "Movies"},
                                                {"key": "2", "type": "show", "title": "TV Shows"}]},
            "/library/sections/1/all": {"totalSize": 10},
            "/library/sections/2/all": {"totalSize": 3},
            "/library/sections/2/all?type=4": {"totalSize": 40},
        }
        with tempfile.TemporaryDirectory() as tmp:
            token = Path(tmp) / "plex-token"
            token.write_text("not-a-real-token\n")
            saved = collect_status.PLEX_TOKEN_FILE, collect_status._plex_get
            collect_status.PLEX_TOKEN_FILE = token
            collect_status._plex_get = lambda path, tok, count_only=False: replies[path]
            try:
                metrics = collect_status.Metrics()
                collect_status.plex(metrics)
            finally:
                collect_status.PLEX_TOKEN_FILE, collect_status._plex_get = saved
        out = metrics.text()
        self.assertIn('momentum_plex_streams{state="playing"} 1.0', out)
        self.assertIn('momentum_plex_streams{state="paused"} 1.0', out)
        self.assertIn("momentum_plex_transcodes 1.0", out)
        self.assertIn('momentum_plex_library_items{library="Movies",type="movie"} 10.0', out)
        self.assertIn('momentum_plex_library_episodes{library="TV Shows"} 40.0', out)
        self.assertNotIn("not-a-real-token", out)

    def test_publishes_nothing_without_a_token_file(self):
        saved = collect_status.PLEX_TOKEN_FILE
        collect_status.PLEX_TOKEN_FILE = Path("/nonexistent/plex-token")
        try:
            metrics = collect_status.Metrics()
            collect_status.plex(metrics)
        finally:
            collect_status.PLEX_TOKEN_FILE = saved
        self.assertEqual(metrics.lines, [])


class PbsTests(unittest.TestCase):
    NOW = 1_800_000_000

    def _run(self, replies):
        with tempfile.TemporaryDirectory() as tmp:
            token = Path(tmp) / "pbs-token"
            token.write_text("monitor@pbs!grafana:not-a-real-secret\n")
            saved = collect_status.PBS_TOKEN_FILE, collect_status._pbs_get
            collect_status.PBS_TOKEN_FILE = token
            collect_status._pbs_get = lambda path, tok: replies[path.split("?")[0]]
            try:
                metrics = collect_status.Metrics()
                collect_status.pbs(metrics, now=self.NOW)
            finally:
                collect_status.PBS_TOKEN_FILE, collect_status._pbs_get = saved
        return metrics.text()

    def test_backups_verify_state_and_task_results(self):
        out = self._run({
            "/status/datastore-usage": [{"store": "main", "total": 100, "used": 40, "avail": 60}],
            "/admin/datastore/main/snapshots": [
                {"backup-type": "vm", "backup-id": "101", "backup-time": 1, "verification": {"state": "failed"}},
                {"backup-type": "vm", "backup-id": "101", "backup-time": 2, "verification": {"state": "ok"}},
                {"backup-type": "ct", "backup-id": "105", "backup-time": 3},
            ],
            "/admin/datastore/main/groups": [
                {"backup-type": "vm", "backup-id": "101", "last-backup": 2, "backup-count": 2, "comment": "grafana"},
                {"backup-type": "ct", "backup-id": "105", "last-backup": 3, "backup-count": 1},
            ],
            "/nodes/localhost/tasks": [
                {"worker_type": "backup", "endtime": 10, "status": "OK"},
                {"worker_type": "backup", "endtime": 20, "status": "backup failed: connection lost"},
                {"worker_type": "verify", "endtime": 15, "status": "WARNINGS: 1"},
                {"worker_type": "prune", "starttime": 30},  # still running
            ],
        })
        self.assertIn('momentum_pbs_datastore_bytes{store="main",kind="used"} 40.0', out)
        # The newest snapshot's verify result counts, not an older failed one.
        self.assertIn('momentum_pbs_group_last_verify{store="main",type="vm",id="101",guest="grafana"} 1.0', out)
        self.assertIn('momentum_pbs_group_last_verify{store="main",type="ct",id="105",guest=""} 2.0', out)
        self.assertIn('momentum_pbs_task_failures_7d{type="backup"} 1.0', out)
        self.assertIn('momentum_pbs_task_last_ok{type="backup"} 0.0', out)
        self.assertIn('momentum_pbs_task_last_ok{type="verify"} 1.0', out)
        self.assertNotIn('momentum_pbs_task_last_ok{type="prune"}', out)
        self.assertNotIn("not-a-real-secret", out)


ZPOOL_STATUS = """  pool: zfs
 state: ONLINE
  scan: scrub repaired 0 in 05:12:33 with 2 errors on Sun Nov  9 05:36:34 2025
config:

	NAME                        STATE     READ WRITE CKSUM
	zfs                         ONLINE       0     0     0
	  raidz2-0                  ONLINE       0     0     0
	    ata-DISK_A              ONLINE       0     0     3
	    ata-DISK_B              FAULTED      1     0     0  too many errors

errors: 4 data errors, use '-v' for a list
"""


class ZfsStatusTests(unittest.TestCase):
    zfs_status = _load_from("deploy/monitoring/zfs", "zfs_status")

    def test_parses_scrub_devices_and_data_errors(self):
        status = self.zfs_status.parse_status(ZPOOL_STATUS)
        self.assertEqual(status["scrub_errors"], 2)
        self.assertIsNotNone(status["scrub_end"])
        self.assertIsNone(status["in_progress"])
        self.assertEqual(status["data_errors"], 4)
        names = [d["device"] for d in status["devices"]]
        self.assertEqual(names, ["zfs", "raidz2-0", "ata-DISK_A", "ata-DISK_B"])
        disk_b = status["devices"][3]
        self.assertEqual((disk_b["state"], disk_b["read"]), ("FAULTED", 1))
        self.assertEqual(status["devices"][2]["checksum"], 3)

    def test_scrub_in_progress_and_clean_pool(self):
        text = ZPOOL_STATUS.replace(
            "scan: scrub repaired 0 in 05:12:33 with 2 errors on Sun Nov  9 05:36:34 2025",
            "scan: scrub in progress since Sun Nov  9 00:24:01 2025",
        ).replace("errors: 4 data errors, use '-v' for a list", "errors: No known data errors")
        status = self.zfs_status.parse_status(text)
        self.assertEqual(status["in_progress"], "scrub")
        self.assertIsNone(status["scrub_end"])
        self.assertEqual(status["data_errors"], 0)


class StorefrontTests(unittest.TestCase):
    """The storefront section against a scratch database built from the store's own migrations."""

    NOW = 1_800_000_000  # 2027-01-15 08:00 UTC = 01:00 in Denver, so "today" there is the 15th

    def _db(self, tmp):
        import sqlite3

        path = Path(tmp) / "store.db"
        con = sqlite3.connect(path)
        migrations = Path(__file__).resolve().parents[1] / "storefront" / "app" / "migrations"
        for sql in sorted(migrations.glob("*.sql")):
            con.executescript(sql.read_text())
        t = "2027-01-01T00:00:00Z"
        products = [  # sku, kind, visible, track_stock, stock, price_cents
            ("BOX", "sealed", 1, 1, 0, 10000), ("ETB", "sealed", 1, 1, 2, 5000),
            ("TIN", "sealed", 1, 1, 9, 2000), ("HIDDEN", "sealed", 0, 1, 4, 1000),
            ("SLEEVES", "accessory", 1, 0, 0, 500)]
        for i, (sku, kind, visible, tracked, stock, price) in enumerate(products, 1):
            con.execute("INSERT INTO products (id, sku, slug, title, kind, visible, track_stock, stock, price_cents, "
                        "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (i, sku, sku.lower(), f"{sku} title", kind, visible, tracked, stock, price, t, t))
        orders = [  # number, mode, status, total, refunded, paid_at
            ("L1", "live", "shipped", 12000, 0, "2027-01-15T07:30:00Z"),            # 00:30 Denver, the 15th
            ("L2", "live", "paid", 5000, 0, "2027-01-15T06:30:00Z"),                # 23:30 Denver, the 14th
            ("L3", "live", "partially_refunded", 8000, 3000, "2026-12-30T12:00:00Z"),  # 16 days back
            ("L4", "live", "expired", 9000, 0, None),
            ("T1", "test", "shipped", 99900, 0, "2027-01-15T07:00:00Z")]
        for i, (number, mode, status, total, refunded, paid_at) in enumerate(orders, 1):
            con.execute("INSERT INTO orders (id, number, access_token, status, mode, ship_class, total_cents, "
                        "refunded_cents, created_at, paid_at) VALUES (?, ?, 'k', ?, ?, 'single', ?, ?, ?, ?)",
                        (i, number, status, mode, total, refunded, "2027-01-01T00:00:00Z", paid_at))
            con.execute("INSERT INTO order_items (order_id, sku, title, unit_price_cents, quantity) "
                        "VALUES (?, 'X', 'x', 100, 2)", (i,))
        con.commit()
        con.close()
        return path

    def _run(self):
        with tempfile.TemporaryDirectory() as tmp:
            collect_status.STORE_DB = self._db(tmp)
            m = collect_status.Metrics()
            collect_status.storefront(m, now=self.NOW)
        return [line for line in m.lines if not line.startswith("#")]

    def test_sales_use_local_days_net_of_refunds_and_keep_test_orders_apart(self):
        lines = self._run()
        self.assertIn('momentum_store_sales_dollars{mode="live",window="today"} 120.0', lines)
        self.assertIn('momentum_store_daily_sales_dollars{mode="live",day="2027-01-14"} 50.0', lines)
        self.assertIn('momentum_store_sales_dollars{mode="live",window="7d"} 170.0', lines)
        self.assertIn('momentum_store_sales_dollars{mode="live",window="30d"} 220.0', lines)  # 120 + 50 + (80 - 30 refunded)
        self.assertIn('momentum_store_paid_orders{mode="live",window="30d"} 3.0', lines)  # expired never counts
        self.assertIn('momentum_store_units_sold{mode="live",window="30d"} 6.0', lines)
        self.assertIn('momentum_store_sales_dollars{mode="test",window="today"} 999.0', lines)
        self.assertIn('momentum_store_orders_to_ship{mode="live"} 1.0', lines)
        self.assertIn('momentum_store_oldest_unshipped_seconds{mode="live"} 5400.0', lines)
        self.assertIn('momentum_store_orders{mode="live",status="expired"} 1.0', lines)

    def test_stock_counts_and_low_stock_list_only_visible_tracked_products(self):
        lines = self._run()
        self.assertIn('momentum_store_products_in_stock{kind="sealed",state="visible"} 2.0', lines)
        self.assertIn('momentum_store_stock_units{kind="sealed",state="visible"} 11.0', lines)
        self.assertIn('momentum_store_stock_value_dollars{kind="sealed",state="visible"} 280.0', lines)
        self.assertIn('momentum_store_products{kind="accessory",state="visible",stock="untracked"} 1.0', lines)
        low = [line for line in lines if line.startswith("momentum_store_low_stock")]
        self.assertEqual(low, ['momentum_store_low_stock{sku="BOX",title="BOX title"} 0.0',
                               'momentum_store_low_stock{sku="ETB",title="ETB title"} 2.0'])

    def test_no_customer_details_in_the_metrics(self):
        text = "\n".join(self._run())
        for field in ("email", "name=", "shipping", "access_token"):
            self.assertNotIn(field, text)


class SearchConsoleTests(unittest.TestCase):
    """search_console.py: signing, what each run asks Google for, and the metrics. No real API."""

    NOW = 1794355200.0  # 2026-11-11 00:00 UTC

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        key = Path(self.tmp.name) / "key.json"
        key.write_text(json.dumps({"client_email": "x@example.iam.gserviceaccount.com",
                                   "token_uri": "https://oauth2.example/token", "private_key": "unused"}))
        self.saved = {n: getattr(search_console, n) for n in ("KEY_FILE", "fetch_summary", "inspect")}
        self.addCleanup(lambda: [setattr(search_console, n, v) for n, v in self.saved.items()])
        search_console.KEY_FILE = key
        self.calls = []

        def fetch_summary(client, today):
            self.calls.append(("summary", today.isoformat()))
            return {"start": "2026-11-01", "end": "2026-11-11",
                    "daily": {"2026-11-02": {"clicks": 1, "impressions": 10, "position": 4.0},
                              "2026-11-08": {"clicks": 3, "impressions": 30, "position": 8.0}},
                    "queries": [{"key": 'a "quoted" search', "clicks": 1, "impressions": 5, "position": 2.0}],
                    "pages": [{"key": "/", "clicks": 4, "impressions": 40, "position": 7.0}],
                    "devices": [], "sitemaps": [{"path": "https://poke6s.com/sitemap.xml",
                                                 "last_read": "2026-11-10T02:24:48.823Z", "pending": False,
                                                 "errors": 0, "warnings": 0, "submitted": 3}]}

        def inspect(client, path):
            self.calls.append(("inspect", path))
            if path == "/broken":
                raise search_console.SearchConsoleError("HTTP 500")
            return {"verdict": "PASS" if path == "/" else "NEUTRAL",
                    "coverage": "Submitted and indexed" if path == "/" else "URL is unknown to Google",
                    "last_crawl": "2026-11-03T00:18:56Z" if path == "/" else None}

        search_console.fetch_summary = fetch_summary
        search_console.inspect = inspect

    def _update(self, cache, now, pages=("/", "/shop", "/about")):
        return search_console.update(cache, now, token_for=lambda key: "token", sitemap=lambda: list(pages))

    @unittest.skipUnless(shutil.which("openssl"), "needs the openssl command")
    def test_jwt_signature_verifies_with_the_public_key(self):
        folder = Path(self.tmp.name)
        pem = subprocess.run(["openssl", "genrsa", "2048"], capture_output=True, text=True, check=True).stdout
        (folder / "pub.pem").write_text(subprocess.run(["openssl", "rsa", "-pubout"], input=pem, capture_output=True,
                                                       text=True, check=True).stdout)
        (folder / "sig").write_bytes(search_console._sign_rs256(b"header.claims", pem))
        result = subprocess.run(["openssl", "dgst", "-sha256", "-verify", str(folder / "pub.pem"), "-signature",
                                 str(folder / "sig")], input=b"header.claims", capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_summary_every_six_hours_and_two_inspections_per_run(self):
        cache = self._update({}, self.NOW)
        self.assertEqual([c[0] for c in self.calls], ["summary", "inspect", "inspect"])
        self.assertEqual(set(cache["pages"]), {"/", "/shop", "/about"})
        self.calls.clear()
        self._update(cache, self.NOW + 60)  # summary still fresh; the third page's turn
        self.assertEqual([c[0] for c in self.calls], ["inspect"])
        self.calls.clear()
        self._update(cache, self.NOW + 120)  # everything checked within the day: no calls
        self.assertEqual(self.calls, [])
        self._update(cache, self.NOW + search_console.REFRESH_SECONDS, pages=("/", "/new"))
        self.assertEqual(set(cache["pages"]), {"/", "/new"})  # follows the sitemap
        self.assertIn(("inspect", "/new"), self.calls)

    def test_a_failure_waits_an_hour_before_trying_again(self):
        cache = {}
        with self.assertRaises(search_console.SearchConsoleError):
            self._update(cache, self.NOW, pages=("/broken",))
        self.assertIn("HTTP 500", cache["error"])
        self.calls.clear()
        self._update(cache, self.NOW + 600, pages=("/broken",))
        self.assertEqual(self.calls, [])
        search_console.inspect = lambda client, path: {"verdict": "PASS", "coverage": "ok", "last_crawl": None}
        self._update(cache, self.NOW + search_console.RETRY_SECONDS, pages=("/broken",))
        self.assertNotIn("error", cache)

    def test_metrics_fill_missing_days_and_weight_position(self):
        cache = self._update({}, self.NOW)
        self._update(cache, self.NOW + 60)
        m = collect_status.Metrics()
        search_console.metrics(m, cache, self.NOW + 60)
        text = m.text()
        self.assertIn('momentum_search_daily_impressions{day="2026-11-05"} 0', text)  # gap filled
        self.assertNotIn('day="2026-11-09"', text)  # after Google's newest day: no false zero
        self.assertIn('momentum_search_impressions{window="28d"} 40', text)
        self.assertIn('momentum_search_position{window="28d"} 7.0', text)  # (4*10 + 8*30) / 40
        self.assertIn('momentum_search_top_query_clicks{query="a \\"quoted\\" search"} 1', text)
        self.assertIn("momentum_search_pages_indexed 1", text)
        self.assertIn('momentum_search_page_indexed{path="/shop",coverage="URL is unknown to Google"} 0', text)


class RunJobTests(unittest.TestCase):
    def test_records_exit_code_and_keeps_last_success_after_a_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            run_job.TEXTFILE_DIR = Path(tmp)
            self.assertEqual(run_job.main(["demo", "--", sys.executable, "-c", "pass"]), 0)
            first = (Path(tmp) / "momentum_job_demo.prom").read_text()
            success_line = next(l for l in first.splitlines() if "last_success" in l)

            self.assertEqual(run_job.main(["demo", "--", sys.executable, "-c", "raise SystemExit(3)"]), 3)
            second = (Path(tmp) / "momentum_job_demo.prom").read_text()
        self.assertIn('momentum_job_last_exit_code{task="demo"} 3', second)
        self.assertIn(success_line, second)

    def test_job_files_and_collector_declare_the_same_help(self):
        # node-exporter drops a metric when two text files give it different HELP/TYPE
        # lines; that hid the systemd jobs' results on 2026-11-05.
        def help_lines(text):
            return {l.split()[2]: l for l in text.splitlines() if l.startswith(("# HELP momentum_job_", "# TYPE momentum_job_"))
                    and l.split()[2] in run_job.JOB_HELP and l.startswith("# HELP")}

        with tempfile.TemporaryDirectory() as tmp:
            run_job.TEXTFILE_DIR = Path(tmp)
            run_job.main(["demo", "--", sys.executable, "-c", "pass"])
            job_file = help_lines((Path(tmp) / "momentum_job_demo.prom").read_text())
        metrics = collect_status.Metrics()
        for name, text in collect_status.JOB_HELP.items():
            metrics.add(name, 1, text, task="x")
        self.assertEqual(job_file, help_lines(metrics.text()))
        self.assertEqual(len(job_file), len(run_job.JOB_HELP))

    def test_rejects_bad_usage(self):
        self.assertEqual(run_job.main(["Bad Name", "--", "true"]), 2)


if __name__ == "__main__":
    unittest.main()
