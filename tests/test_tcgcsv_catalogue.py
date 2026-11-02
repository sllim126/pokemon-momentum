"""crawl_tcgcsv_catalogue.py: budget, priority order and once-a-day rule (no network: a fake
TCGCSV answers)."""

import argparse
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from scripts.download import crawl_tcgcsv_catalogue as crawl
from scripts.pipeline import notify_discord


class FakeResponse:
    def __init__(self, payload: dict):
        self.content = json.dumps(payload).encode()


def fake_tcgcsv(calls: list[str]):
    """Two games (Magic 1 and Pokemon 3), two sets each."""
    def get(_session, path):
        calls.append(path)
        if path == "/tcgplayer/categories":
            return FakeResponse({"results": [{"categoryId": 1}, {"categoryId": 3}]})
        if path.endswith("/groups"):
            cat = int(path.split("/")[2])
            return FakeResponse({"results": [{"groupId": cat * 100 + 1}, {"groupId": cat * 100 + 2}]})
        return FakeResponse({"results": []})
    return get


class CatalogueCrawlTests(unittest.TestCase):
    def run_crawl(self, root: Path, calls: list[str], *argv: str) -> str:
        with mock.patch.object(crawl, "CATALOGUE_ROOT", root), \
             mock.patch.object(crawl, "STATE_FILE", root / "state.json"), \
             mock.patch.object(crawl, "tcgcsv_get", fake_tcgcsv(calls)), \
             mock.patch.object(crawl, "build_tcgcsv_session", lambda: None), \
             mock.patch.object(sys, "argv", ["crawl", *argv]), \
             mock.patch("builtins.print") as printed:
            self.assertEqual(crawl.main(), 0)
        return "\n".join(" ".join(map(str, c.args)) for c in printed.call_args_list)

    def test_budget_priority_and_resume(self):
        with TemporaryDirectory() as tmp:
            root, calls = Path(tmp), []
            out = self.run_crawl(root, calls, "--budget", "3")
            # Pokemon (3) comes before Magic (1); the third request is Pokemon's first set.
            self.assertEqual(calls, ["/tcgplayer/categories", "/tcgplayer/3/groups", "/tcgplayer/3/301/products"])
            self.assertIn("Out of budget", out)
            # Same day, budget raised: the rest, each file once.
            calls.clear()
            out = self.run_crawl(root, calls, "--budget", "100")
            self.assertEqual(len(calls), 4)  # 302 products, Magic groups, 101 and 102 products
            self.assertNotIn("/tcgplayer/categories", calls)
            self.assertIn("Catalogue complete", out)
            self.assertTrue((root / "1" / "102" / "products.json").exists())
            # Nothing is fetched twice on one day, and the day's count is kept.
            calls.clear()
            self.run_crawl(root, calls, "--budget", "100")
            self.assertEqual(calls, [])
            self.assertEqual(json.loads((root / "state.json").read_text())["requests"], {crawl.datetime.now(crawl.timezone.utc).date().isoformat(): 7})


class CatalogueSummaryTests(unittest.TestCase):
    def test_progress_posts_and_quiet_night_does_not(self):
        lines = ["Catalogue: 3 of 9 files fresh; 2 games; saved 3 tonight; requests today: 3",
                 "Out of budget for today; 6 files left for the next nights"]
        status, color, fields = notify_discord.catalogue_summary(argparse.Namespace(exit_code=0), lines)
        self.assertIn("continues tomorrow", status)
        self.assertIn("3 of 9", fields[0]["value"])
        quiet = ["Catalogue: 9 of 9 files fresh; 2 games; saved 0 tonight; requests today: 0", "Catalogue complete"]
        self.assertIsNone(notify_discord.catalogue_summary(argparse.Namespace(exit_code=0), quiet)[2])

    def test_block_is_red(self):
        _, color, _ = notify_discord.catalogue_summary(argparse.Namespace(exit_code=3), ["STOPPED: TCGCSV returned 429"])
        self.assertEqual(color, notify_discord.RED)
