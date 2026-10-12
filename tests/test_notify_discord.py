import argparse
import unittest

from scripts.pipeline import notify_discord


class CollectSummaryTests(unittest.TestCase):
    """The other-games archive summary lists what was archived and flags failures."""

    def test_archived_games_listed_without_paths(self):
        lines = [
            "ARCHIVED 71 Disney Lorcana: 21 groups, 6,326 price rows, 0.6 MB in 8s -> /app/data/x.tar.gz",
            "SKIP 2 Yu-Gi-Oh: 2026-10-11 already archived.",
        ]
        status, _, fields = notify_discord.collect_summary(argparse.Namespace(exit_code=0), lines)
        self.assertIn("succeeded", status)
        self.assertEqual(fields[0]["value"], "71 Disney Lorcana: 21 groups, 6,326 price rows, 0.6 MB")
        self.assertIn("Yu-Gi-Oh", fields[1]["value"])

    def test_failure_quotes_errors(self):
        lines = ["ERROR 1: boom"]
        status, color, fields = notify_discord.collect_summary(argparse.Namespace(exit_code=1), lines)
        self.assertIn("FAILED", status)
        self.assertEqual(color, notify_discord.RED)
        self.assertIn("ERROR 1: boom", fields[-1]["value"])
