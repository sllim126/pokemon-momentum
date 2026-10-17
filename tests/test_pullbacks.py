"""The Pullbacks rules (query_support.build_pullback_sql) on a small made-up snapshot."""

import unittest

import duckdb

from scripts.dashboards.query_support import build_pullback_sql

COLUMNS = "productId, groupId, productKind, rarity, latest_price, high_90d, latest_sma7, roc_30d_pct, roc_365d_pct, recent_distinct_prices_30d"


def card(pid, group, price, high, sma7, roc30, roc365=20.0, changes=8, rarity="Ultra Rare"):
    return (pid, group, "card", rarity, price, high, sma7, roc30, roc365, changes)


class PullbackRulesTests(unittest.TestCase):
    def run_rules(self, rows):
        con = duckdb.connect()
        con.execute(f"CREATE TABLE snap AS SELECT * FROM (VALUES {', '.join(map(str, rows))}) t({COLUMNS})")
        result = con.execute(f"SELECT productId, pullback_kind FROM ({build_pullback_sql('snap')}) ORDER BY productId").fetchall()
        return dict(result)

    def test_rules(self):
        # Set 1 holds steady (20 flat cards keep the price band's median at 0), set 2 falls as a
        # whole (-20%).
        steady = [card(100 + i, 1, 30.0, 30.0, 30.0, 0.0) for i in range(20)]
        falling = [card(200 + i, 2, 30.0, 40.0, 30.0, -20.0) for i in range(6)]
        rows = steady + falling + [
            card(1, 1, 30.0, 40.0, 31.0, -20.0),  # own dip: 25% off its high, behind band and set
            card(2, 1, 38.0, 40.0, 38.0, -20.0),  # only 5% below its high: not a pullback
            card(3, 1, 30.0, 40.0, 39.0, -20.0),  # 7-day average still near the high: a one-day dip
            card(4, 1, 20.0, 40.0, 20.0, -20.0),  # 50% below its high: a collapse, not a dip
            card(5, 1, 30.0, 40.0, 30.0, -20.0, roc365=-5.0),  # down over the year
            card(6, 1, 30.0, 40.0, 30.0, -20.0, changes=3),  # too few price changes
            card(7, 1, 30.0, 40.0, 30.0, -20.0, rarity="Common"),  # not a premium rarity
            card(8, 1, 3.0, 4.0, 3.0, -20.0),  # under $5
        ]
        kinds = self.run_rules(rows)
        self.assertEqual(kinds.get(1), "own")
        for pid in (2, 3, 4, 5, 6, 7, 8):
            self.assertNotIn(pid, kinds, pid)
        # Set 2's cards are 25% off their highs and behind the price band, but so is their set.
        self.assertEqual({kinds.get(200 + i) for i in range(6)}, {"set"})
        self.assertFalse(any(100 + i in kinds for i in range(20)))


if __name__ == "__main__":
    unittest.main()
