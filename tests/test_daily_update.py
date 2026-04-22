import argparse
import unittest
from unittest.mock import patch

from scripts.pipeline import run_daily_update


class DailyUpdatePipelineTests(unittest.TestCase):
    def test_metadata_failures_fall_back_to_cached_snapshot(self):
        args = argparse.Namespace(
            category_id=3,
            dry_run=False,
            continue_on_error=False,
        )
        steps = [
            ("Load new price data into DuckDB", ["python", "load_prices.py"]),
            ("Refresh group metadata", ["python", "groups.py"]),
            ("Refresh product metadata", ["python", "products.py"]),
            ("Build Pokemon product signal snapshot", ["python", "signals.py"]),
        ]

        with patch.object(run_daily_update, "parse_args", return_value=args), patch.object(
            run_daily_update, "build_steps", return_value=steps
        ), patch.object(
            run_daily_update, "run_step", side_effect=[0, 1, 1, 0]
        ) as run_step_mock:
            result = run_daily_update.main()

        self.assertEqual(result, 0)
        self.assertEqual(run_step_mock.call_count, 4)

    def test_non_metadata_failure_still_stops_pipeline(self):
        args = argparse.Namespace(
            category_id=3,
            dry_run=False,
            continue_on_error=False,
        )
        steps = [
            ("Load new price data into DuckDB", ["python", "load_prices.py"]),
            ("Refresh group metadata", ["python", "groups.py"]),
        ]

        with patch.object(run_daily_update, "parse_args", return_value=args), patch.object(
            run_daily_update, "build_steps", return_value=steps
        ), patch.object(
            run_daily_update, "run_step", side_effect=[2, 0]
        ) as run_step_mock:
            result = run_daily_update.main()

        self.assertEqual(result, 1)
        self.assertEqual(run_step_mock.call_count, 1)

    def test_only_live_metadata_steps_are_soft_failures(self):
        self.assertTrue(run_daily_update.can_fall_back_to_cached_metadata("Refresh group metadata"))
        self.assertTrue(run_daily_update.can_fall_back_to_cached_metadata("Refresh product metadata"))
        self.assertFalse(run_daily_update.can_fall_back_to_cached_metadata("Rebuild joined price/name export"))


if __name__ == "__main__":
    unittest.main()
