import io
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.dashboards import tracking_store
from scripts.utilities import bug_reports


class BugReportsCliTests(unittest.TestCase):
    """bug_reports.py lists open reports, shows one, and resolves or reopens it."""

    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = patch.object(tracking_store, "TRACKING_DB_PATH", Path(tmp.name) / "t.sqlite3")
        db.start()
        self.addCleanup(db.stop)
        tracking_store.ensure_tracking_schema()
        for path, title in (("discord #bug-reports", "Chart won't load"), ("/m-next", "Can't sign in")):
            tracking_store.create_bug_report({"page_path": path, "page_url": path, "title": title,
                                              "details": title, "context": {}})

    def run_cli(self, *args):
        out = io.StringIO()
        with redirect_stdout(out):
            code = bug_reports.main(list(args))
        return code, out.getvalue()

    def test_list_show_resolve_and_reopen(self):
        code, text = self.run_cli("list")
        self.assertEqual(code, 0)
        self.assertIn("Discord", text)
        self.assertIn("Phone app", text)
        self.assertIn("2 report(s) open", text)
        code, text = self.run_cli("show", "1")
        self.assertIn("title: Chart won't load", text)
        self.assertEqual(self.run_cli("resolve", "1")[0], 0)
        self.assertIn("1 report(s) open", self.run_cli("list")[1])
        self.assertIn("2 report(s)", self.run_cli("list", "--all")[1])
        self.run_cli("resolve", "1", "--status", "new")
        self.assertIn("2 report(s) open", self.run_cli("list")[1])
        self.assertEqual(self.run_cli("resolve", "99")[0], 1)
