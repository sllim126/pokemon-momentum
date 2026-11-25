"""The monthly TCGCSV torrent job: which month it builds, when it waits instead of failing,
and what it posts to Discord. The build, share copy, qBittorrent and email steps need the
server's network and were checked by hand (2026-11-25, see docs/operator_runbook.txt)."""

import argparse
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

from scripts.pipeline import notify_discord
from scripts.torrents import publish_monthly_torrent as job


class MonthTests(unittest.TestCase):
    def test_previous_month(self):
        self.assertEqual(job.previous_month(date(2026, 12, 2)), "2026-11")
        self.assertEqual(job.previous_month(date(2027, 1, 2)), "2026-12")

    def test_missing_days_lists_absent_files(self):
        with tempfile.TemporaryDirectory() as raw, mock.patch.object(job, "RAW_DIR", Path(raw)):
            for day in range(1, 30):   # November without the 30th
                (Path(raw) / f"prices-2026-11-{day:02d}.ppmd.7z").touch()
            self.assertEqual(job.missing_days("2026-11"), [date(2026, 11, 30)])


class WaitOrFailTests(unittest.TestCase):
    """A missing day is a wait (exit 3) until the 5th, then a failure; done months stop at once."""

    def run_main(self, today: date, argv: list[str], torrents: Path) -> int:
        with mock.patch.object(job, "TORRENTS_DIR", torrents), \
                mock.patch.object(job, "missing_days", return_value=[date(2026, 11, 30)]):
            return job.main(argv, today=today)

    def test_waits_early_in_the_month_then_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(self.run_main(date(2026, 12, 2), [], Path(folder)), job.EXIT_WAITING)
            self.assertEqual(self.run_main(date(2026, 12, 5), [], Path(folder)), 1)
            # An explicit --month is a manual run: report the gap rather than wait.
            self.assertEqual(self.run_main(date(2026, 12, 2), ["--month", "2026-11"], Path(folder)), 1)

    def test_published_month_is_skipped(self):
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / "tcgcsv-archive-2026-11.published").write_text("{}")
            self.assertEqual(self.run_main(date(2026, 12, 3), [], Path(folder)), 0)


class DiscordSummaryTests(unittest.TestCase):
    def summary(self, code: int, lines: list[str]):
        return notify_discord.monthly_torrent_summary(argparse.Namespace(exit_code=code), lines)

    def test_already_published_posts_nothing(self):
        self.assertIsNone(self.summary(0, ["[t] Already published: x (x.published); nothing to do"])[2])

    def test_waiting_is_grey(self):
        _, color, fields = self.summary(3, ["[t] WAITING: 1 day(s) of 2026-11 not downloaded yet (2026-11-30)"])
        self.assertEqual(color, notify_discord.GREY)
        self.assertIn("2026-11-30", fields[0]["value"])

    def test_failure_quotes_the_reason(self):
        status, color, fields = self.summary(1, ["[t] FAILED: qBittorrent ... stuck. Restart qBittorrent on the PC"])
        self.assertIn("FAILED", status)
        self.assertEqual(color, notify_discord.RED)
        self.assertIn("Restart qBittorrent", fields[0]["value"])

    def test_success_gives_magnet_and_next_step(self):
        _, color, fields = self.summary(0, [
            "[t] Emailed the .torrent and the post to admin@poke6s.com",
            "[t] PUBLISHED tcgcsv-archive-2026-11: 30 days, 150 MB, info hash abc",
            "Magnet: magnet:?xt=urn:btih:abc"])
        self.assertEqual(color, notify_discord.GREEN)
        names = [field["name"] for field in fields]
        self.assertEqual(names, ["Torrent", "Next step (owner)", "Magnet"])
        self.assertIn("urn:btih:abc", fields[2]["value"])


if __name__ == "__main__":
    unittest.main()
