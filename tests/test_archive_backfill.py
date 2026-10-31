"""backfill_tcgcsv_archive.py: which days it would fetch (no network: dry runs only)."""

import json
import sys
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from scripts.download import backfill_tcgcsv_archive as backfill


class ArchiveBackfillTests(unittest.TestCase):
    def run_dry(self, raw: Path, argv: list[str]) -> str:
        with mock.patch.object(backfill, "ARCHIVES_DIR", raw), \
             mock.patch.object(backfill, "ATTEMPTS_FILE", raw / "backfill_attempts.json"), \
             mock.patch.object(sys, "argv", ["backfill", "--dry-run", *argv]), \
             mock.patch("builtins.print") as printed:
            self.assertEqual(backfill.main(), 0)
        return "\n".join(" ".join(map(str, c.args)) for c in printed.call_args_list)

    def test_starts_after_newest_file_and_skips_held_days(self):
        with TemporaryDirectory() as tmp:
            raw = Path(tmp)
            (raw / "prices-2026-09-15.ppmd.7z").touch()
            (raw / "prices-2026-09-17.ppmd.7z").touch()  # already held: not fetched again
            out = self.run_dry(raw, ["--start", "2026-09-16", "--end", "2026-09-18"])
            self.assertIn("would fetch prices-2026-09-16.ppmd.7z", out)
            self.assertIn("would fetch prices-2026-09-18.ppmd.7z", out)
            self.assertNotIn("2026-09-17.ppmd.7z", out)
            with mock.patch.object(backfill, "ARCHIVES_DIR", raw):
                self.assertEqual(backfill.default_start(), date(2026, 9, 18))

    def test_once_per_file_per_day(self):
        with TemporaryDirectory() as tmp:
            raw = Path(tmp)
            today = datetime.now(timezone.utc).date().isoformat()
            (raw / "backfill_attempts.json").write_text(json.dumps({"2026-09-16": today}))
            out = self.run_dry(raw, ["--start", "2026-09-16", "--end", "2026-09-17"])
            self.assertIn("already asked for today: 1", out)
            self.assertNotIn("would fetch prices-2026-09-16", out)
            self.assertIn("would fetch prices-2026-09-17", out)


if __name__ == "__main__":
    unittest.main()
