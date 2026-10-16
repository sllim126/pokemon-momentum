import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.dashboards import tracking_store
from scripts.utilities import import_discord_bug_reports as importer


def message(mid, content="Chart doesn't load\nOn Charizard 4/102", bot=False, mtype=0, attachments=None):
    return {"id": mid, "type": mtype, "content": content, "timestamp": "2026-10-16T12:00:00+00:00",
            "author": {"id": "42", "username": "ash", "global_name": "Ash", "bot": bot},
            "attachments": attachments or []}


class DiscordBugImportTests(unittest.TestCase):
    """New #bug-reports messages become bug reports once, with a ✅; bots and system messages don't."""

    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = patch.object(tracking_store, "TRACKING_DB_PATH", Path(tmp.name) / "t.sqlite3")
        db.start()
        self.addCleanup(db.stop)
        env = patch.object(importer, "setting", side_effect=lambda name, default="": "token" if name.endswith("TOKEN") else default)
        env.start()
        self.addCleanup(env.stop)

    def run_import(self, messages):
        calls = []

        def fake_discord(method, path, token):
            calls.append((method, path))
            if method == "GET" and "/messages" in path:
                after = int(path.split("after=")[1])
                return [m for m in messages if int(m["id"]) > after]
            if method == "GET":
                return {"guild_id": "999"}
            return None

        with patch.object(importer, "discord", side_effect=fake_discord), patch.object(importer.time, "sleep"), \
                patch("sys.argv", ["import"]):
            self.assertEqual(importer.main(), 0)
        return calls

    def test_imports_once_and_reacts(self):
        msgs = [message("101"), message("102", bot=True), message("103", mtype=7), message("104", content="", attachments=[])]
        calls = self.run_import(msgs)
        reports = tracking_store.list_bug_reports()
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0]["title"], "Chart doesn't load")
        self.assertEqual(reports[0]["reporter_username"], "discord:Ash")
        context = json.loads(reports[0]["context_json"])
        self.assertEqual(context["discord_url"], "https://discord.com/channels/999/1489120381993357464/101")
        self.assertEqual([c for c in calls if c[0] == "PUT"], [("PUT", "/channels/1489120381993357464/messages/101/reactions/%E2%9C%85/@me")])
        # A second run sees nothing new and saves nothing twice.
        self.run_import(msgs)
        self.assertEqual(len(tracking_store.list_bug_reports()), 1)

    def test_attachment_only_message_gets_a_title(self):
        self.run_import([message("201", content="", attachments=[{"filename": "shot.png", "url": "https://cdn.discordapp.com/x/shot.png"}])])
        report = tracking_store.list_bug_reports()[0]
        self.assertEqual(report["title"], "Discord bug report (attachment only)")
        self.assertEqual(json.loads(report["context_json"])["attachments"][0]["filename"], "shot.png")


class BugReportsPageEscapingTests(unittest.TestCase):
    def test_report_text_is_escaped(self):
        html = (Path(__file__).resolve().parents[1] / "scripts/dashboards/bug_reports.html").read_text()
        for field in ("esc(report.title)", "esc(report.details)", "esc(contextJson)", "esc(report.expected)"):
            self.assertIn(field, html)
        self.assertNotIn("${report.title}", html)
        self.assertNotIn("${report.details}", html)
