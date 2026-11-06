"""Market insights (2026-11-06): Market mood, Track record, price records, nicknames,
the release calendar's hand-kept file and the nightly mood step."""

import json
import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import duckdb

from scripts.common import card_nicknames, market_mood
from scripts.dashboards import pick_tracker, tracking_store
from scripts.pipeline import run_daily_update

REPO = Path(__file__).resolve().parents[1]


class MoodSqlTests(unittest.TestCase):
    """mood_sql on a tiny in-memory series: two cards over 100 days."""

    def setUp(self):
        self.con = duckdb.connect()
        days = [date.fromordinal(date(2026, 1, 1).toordinal() + i).isoformat() for i in range(100)]
        rising = [10.0 + i * 0.1 for i in range(100)]  # steady climb: above its average, new highs
        falling = [30.0 - i * 0.1 for i in range(100)]  # steady fall
        flat_sealed = [50.0] * 100  # sealed: must be left out
        def sma30(prices):
            return [sum(prices[max(0, i - 29): i + 1]) / len(prices[max(0, i - 29): i + 1]) for i in range(len(prices))]
        self.con.execute("CREATE TABLE series (latest_date DATE, categoryId INT, productId INT, subTypeName VARCHAR, dates_json VARCHAR, prices_json VARCHAR, sma30_json VARCHAR)")
        self.con.execute("CREATE TABLE signal (categoryId INT, productId INT, subTypeName VARCHAR, productKind VARCHAR)")
        for pid, prices, kind in ((1, rising, "card"), (2, falling, "card"), (3, flat_sealed, "sealed")):
            self.con.execute("INSERT INTO series VALUES (?, 3, ?, 'Holofoil', ?, ?, ?)",
                             [days[-1], pid, json.dumps(days), json.dumps(prices), json.dumps(sma30(prices))])
            self.con.execute("INSERT INTO signal VALUES (3, ?, 'Holofoil', ?)", [pid, kind])

    def rows(self):
        cur = self.con.execute(market_mood.mood_sql("series", "signal", 3))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def test_counts_singles_only_and_balanced_market_is_neutral(self):
        rows = self.rows()
        self.assertEqual(len(rows), 100)
        last = rows[-1]
        self.assertEqual(last["cards"], 2)  # the sealed product is out
        self.assertEqual((last["above_avg"], last["below_avg"]), (1, 1))
        self.assertEqual((last["up_7d"], last["down_7d"]), (1, 1))
        self.assertEqual((last["new_highs"], last["new_lows"]), (1, 1))
        reading = market_mood.score_rows(rows)[-1]
        self.assertEqual(reading["mood"], 50.0)
        self.assertEqual(market_mood.mood_band(reading["mood"])["label"], "Neutral")

    def test_forward_baseline(self):
        rows = self.rows()
        baseline = market_mood.baseline_by_date(rows)
        # Day 0: both cards are >= $5; 7 days later one is up and one down.
        first = baseline[rows[0]["date"].isoformat()]
        self.assertEqual(first["fwd7_up"], 0.5)
        self.assertEqual(first["fwd30_hit"], 0.5)  # 10.0 -> 13.0 is +30%
        # The last day has no future yet, so no baseline.
        self.assertNotIn(rows[-1]["date"].isoformat(), baseline)


class MoodScoringTests(unittest.TestCase):
    def test_unknown_parts_are_skipped(self):
        rows = [{"date": "2026-01-01", "cards": 10, "above_avg": 6, "below_avg": 2, "up_7d": 3, "down_7d": 1,
                 "new_highs": 0, "new_lows": 0}]
        reading = market_mood.score_rows(rows)[0]
        self.assertIsNone(reading["new_highs"])
        self.assertEqual(reading["mood"], 75.0)  # average of 75% and 75%

    def test_day_without_movers_is_dropped(self):
        rows = [{"date": "2026-01-01", "cards": 10, "above_avg": 1, "below_avg": 1, "up_7d": 0, "down_7d": 0,
                 "new_highs": 0, "new_lows": 0}]
        self.assertEqual(market_mood.score_rows(rows), [])
        self.assertEqual(market_mood.mood_summary([]), {"available": False})

    def test_bands(self):
        self.assertEqual(market_mood.mood_band(5)["label"], "Cold")
        self.assertEqual(market_mood.mood_band(85)["label"], "Hot")
        self.assertEqual(market_mood.mood_band(None)["label"], "No reading")


class TrackRecordTests(unittest.TestCase):
    def pick(self, day, pid=1, list_key="breakouts", price=10.0):
        return {"list_key": list_key, "price_date": day, "rank": 1, "product_id": pid, "sub_type_name": "Holofoil", "entry_price": price}

    def test_repeat_days_are_one_pick_until_the_gap(self):
        picks = [self.pick("2026-01-01"), self.pick("2026-01-02"), self.pick("2026-01-03"),
                 self.pick("2026-03-01"),  # back after more than 30 days: a new pick
                 self.pick("2026-01-02", list_key="top_movers")]  # another list counts separately
        fresh = pick_tracker.first_appearances(picks)
        self.assertEqual([(p["list_key"], p["price_date"]) for p in fresh],
                         [("breakouts", "2026-01-01"), ("top_movers", "2026-01-02"), ("breakouts", "2026-03-01")])

    def test_price_on_uses_last_known_price_and_not_the_future(self):
        series = (["2026-01-01", "2026-01-02", "2026-01-05"], [1.0, 2.0, 5.0])
        self.assertEqual(pick_tracker.price_on(series, date(2026, 1, 4)), 2.0)
        self.assertEqual(pick_tracker.price_on(series, date(2026, 1, 5)), 5.0)
        self.assertIsNone(pick_tracker.price_on(series, date(2026, 1, 6)))
        self.assertIsNone(pick_tracker.price_on(series, date(2025, 12, 31)))

    def test_scoring_and_summary_against_the_market(self):
        days = [date.fromordinal(date(2026, 1, 1).toordinal() + i).isoformat() for i in range(40)]
        series = {
            (1, "Holofoil"): (days, [10.0 + i * 0.5 for i in range(40)]),  # +15% by day 30... and more
            (2, "Holofoil"): (days, [10.0 - i * 0.1 for i in range(40)]),  # down
        }
        picks = [self.pick("2026-01-01", pid=1), self.pick("2026-01-01", pid=2),
                 {**self.pick("2026-01-01", pid=2, list_key="budget"), "entry_price": None}]
        baseline = {"2026-01-01": {"fwd7_up": 0.5, "fwd7_hit": 0.1, "fwd7_median": 0.0,
                                   "fwd30_up": 0.6, "fwd30_hit": 0.2, "fwd30_median": 1.5}}
        payload = pick_tracker.build_track_record(picks, series, date(2026, 2, 9), baseline,
                                                  {"breakouts": "Breakouts", "budget": "Budget Builder"})
        breakouts = next(l for l in payload["lists"] if l["key"] == "breakouts")
        self.assertEqual(breakouts["picks"], 2)
        self.assertEqual(breakouts["d30"]["scored"], 2)
        self.assertEqual(breakouts["d30"]["up_share"], 50.0)
        self.assertEqual(breakouts["d30"]["hit_share"], 50.0)  # card 1: 10 -> 25 is +150%
        self.assertEqual(breakouts["d30"]["market_up_share"], 60.0)
        self.assertIsNone(breakouts["d14"]["market_up_share"])  # no 14-day baseline given
        self.assertEqual(breakouts["open"], 0)
        # The budget pick had no stored price: it's read from the history (10.0 on day one).
        budget = next(l for l in payload["lists"] if l["key"] == "budget")
        self.assertEqual(budget["picks"], 1)
        self.assertEqual(payload["recent"][0]["entry_price"], 10.0)

    def test_young_picks_are_open(self):
        days = ["2026-01-01", "2026-01-02", "2026-01-03"]
        series = {(1, "Holofoil"): (days, [10.0, 11.0, 12.0])}
        payload = pick_tracker.build_track_record([self.pick("2026-01-01")], series, date(2026, 1, 3), None, {"breakouts": "Breakouts"})
        self.assertEqual(payload["all"]["open"], 1)
        self.assertEqual(payload["all"]["open_median_pct"], 20.0)
        self.assertIsNone(payload["all"]["d7"]["up_share"])


class PickStorageTests(unittest.TestCase):
    def test_record_once_and_load(self):
        with TemporaryDirectory() as tmp, mock.patch.object(tracking_store, "TRACKING_DB_PATH", Path(tmp) / "t.sqlite3"):
            picks = [{"list_key": "breakouts", "rank": 1, "product_id": 5, "sub_type_name": "Normal", "entry_price": 7.5}]
            self.assertEqual(tracking_store.record_list_picks(3, "2031-01-01", picks), 1)
            changed = [{**picks[0], "entry_price": 99.0}]
            self.assertEqual(tracking_store.record_list_picks(3, "2031-01-01", changed), 0)  # first record wins
            rows = tracking_store.load_list_picks(3)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["entry_price"], 7.5)
            self.assertEqual(tracking_store.recorded_pick_dates(3), {"2031-01-01"})
            self.assertEqual(tracking_store.recorded_pick_dates(85), set())
            tracking_store.record_budget_picks("raw", 3, "2031-01-02", ["5||Normal", "bad-key"])
            budget = tracking_store.load_budget_picks("raw", 3)
            self.assertEqual([(b["product_id"], b["sub_type_name"]) for b in budget], [(5, "Normal")])


class PriceRecordTests(unittest.TestCase):
    def test_high_low_and_changes(self):
        from scripts.dashboards.api import price_records
        history = [["2024-02-10", 100.0], ["2025-01-01", 300.0], ["2025-11-05", 150.0], ["2026-11-05", 200.0], ["2026-11-06", None]]
        rec = price_records(history)
        self.assertEqual(rec["high"], {"date": "2025-01-01", "price": 300.0, "vs_now_pct": -33.3})
        self.assertEqual(rec["low"]["date"], "2024-02-10")
        self.assertEqual(rec["changes"]["1y"], 33.3)  # 150 -> 200
        self.assertIsNone(rec["changes"]["3y"])  # history starts in 2024
        self.assertEqual(price_records([]), {})


class NicknameTests(unittest.TestCase):
    def test_fold_and_expand(self):
        self.assertEqual(card_nicknames.expand_nickname("Moon-breon"), "umbreon vmax 215")
        self.assertEqual(card_nicknames.expand_nickname("  Van Gogh Pikachu "), "pikachu with grey felt hat")
        self.assertIsNone(card_nicknames.expand_nickname("umbreon"))

    def test_keys_are_folded(self):
        for key, query in card_nicknames.NICKNAMES.items():
            self.assertEqual(key, card_nicknames.fold(key))
            self.assertTrue(query.strip())


class ReleaseFileAndPipelineTests(unittest.TestCase):
    def test_upcoming_sets_file_shape(self):
        data = json.loads((REPO / "scripts" / "common" / "upcoming_sets.json").read_text())
        self.assertIn("sets", data)
        for entry in data["sets"]:
            date.fromisoformat(entry["release_date"])
            self.assertIn(entry["market"], {"EN", "JP"})
            self.assertIn(entry["status"], {"confirmed", "reported"})
            self.assertTrue(entry["name"])

    def test_mood_step_is_soft(self):
        self.assertTrue(run_daily_update.can_fall_back_to_cached_metadata("Build Pokemon market mood snapshot"))
        self.assertFalse(run_daily_update.can_fall_back_to_cached_metadata("Build Pokemon series snapshot"))


if __name__ == "__main__":
    unittest.main()
