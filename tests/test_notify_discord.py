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


class DailyUpdateWarningTests(unittest.TestCase):
    """A step that fails without stopping the run turns the message orange (which pings)."""

    def test_failed_step_is_flagged(self):
        from datetime import datetime, timezone
        from unittest.mock import patch

        today = datetime.now(timezone.utc).date().isoformat()
        lines = [
            "=== Download PriceCharting graded prices ===",
            "python scripts/download/fetch_pricecharting.py",
            "PriceCharting download failed: HTTP 404, 21 bytes",
            "--- exit=1 elapsed=2.3s ---",
            "Warning: Download PriceCharting graded prices failed (exit 1); continuing with the last known metadata snapshot",
            "Pipeline finished with warnings:",
        ]
        with patch.object(notify_discord, "read_health", return_value={"latest": today, "rows": "100"}):
            status, color, fields = notify_discord.daily_update_summary(argparse.Namespace(exit_code=0), lines)
        self.assertEqual(color, notify_discord.ORANGE)
        self.assertIn("step failed", status)
        flagged = next(f for f in fields if f["name"].startswith("Steps that failed"))
        self.assertIn("Download PriceCharting graded prices", flagged["value"])
        self.assertIn("HTTP 404", flagged["value"])

    def test_clean_run_stays_green(self):
        from datetime import datetime, timezone
        from unittest.mock import patch

        today = datetime.now(timezone.utc).date().isoformat()
        with patch.object(notify_discord, "read_health", return_value={"latest": today, "rows": "100"}):
            status, color, _ = notify_discord.daily_update_summary(argparse.Namespace(exit_code=0), ["Pipeline finished successfully."])
        self.assertEqual(color, notify_discord.GREEN)


class ArchiveBackfillSummaryTests(unittest.TestCase):
    MISSING = "Missing archive files 2026-09-16 to 2026-10-30: 45; already asked for today: 0; to fetch now: 45"

    def test_offline_is_grey_without_a_ping(self):
        lines = [self.MISSING, "STOPPED at prices-2026-09-16.ppmd.7z: TCGCSV returned 403 ... 'The price archive has been temporarily removed'"]
        status, color, _ = notify_discord.backfill_summary(argparse.Namespace(exit_code=3), lines)
        self.assertEqual(color, notify_discord.GREY)
        self.assertIn("still offline", status)

    def test_other_403_is_red(self):
        lines = [self.MISSING, "STOPPED at prices-2026-09-20.ppmd.7z: TCGCSV returned 429 for ..."]
        _, color, _ = notify_discord.backfill_summary(argparse.Namespace(exit_code=3), lines)
        self.assertEqual(color, notify_discord.RED)

    def test_gap_download_lists_days_and_routine_night_is_silent(self):
        lines = [self.MISSING, "OK prices-2026-09-16.ppmd.7z (3,000,000 bytes)", "OK prices-2026-09-17.ppmd.7z (3,100,000 bytes)"]
        status, color, fields = notify_discord.backfill_summary(argparse.Namespace(exit_code=0), lines)
        self.assertEqual(color, notify_discord.GREEN)
        self.assertIn("prices-2026-09-17.ppmd.7z", fields[1]["value"])
        routine = ["Missing archive files 2026-11-05 to 2026-11-05: 1; already asked for today: 0; to fetch now: 1",
                   "OK prices-2026-11-05.ppmd.7z (3,000,000 bytes)"]
        self.assertIsNone(notify_discord.backfill_summary(argparse.Namespace(exit_code=0), routine)[2])
