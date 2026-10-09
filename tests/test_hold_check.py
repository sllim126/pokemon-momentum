import unittest
from datetime import date, timedelta

from scripts.common.selling_costs import CHANNELS, break_even_price, net_proceeds, shipping_cost
from scripts.dashboards.hold_check import hold_check

TCGPLAYER = CHANNELS["tcgplayer"]


def daily(start_price: float, daily_growth: float, days: int, end: date = date(2026, 10, 8)):
    """A synthetic daily price history ending on `end` that compounds at `daily_growth`."""
    start = end - timedelta(days=days - 1)
    return [(start + timedelta(days=i), start_price * (1 + daily_growth) ** i) for i in range(days)]


class SellingCostTests(unittest.TestCase):
    def test_break_even_returns_exactly_the_buy_price(self):
        for buy in (0.5, 1, 5, 20, 28, 30, 34, 42.52, 49, 100, 500):
            be = break_even_price(buy, TCGPLAYER)
            self.assertAlmostEqual(net_proceeds(be, TCGPLAYER), buy, places=6, msg=buy)
            self.assertLess(net_proceeds(be - 0.01, TCGPLAYER), buy, msg=buy)

    def test_shipping_steps_up_at_tracked_and_signature_tiers(self):
        self.assertEqual(shipping_cost(34.99)[1], "plain envelope")
        self.assertEqual(shipping_cost(35.00)[1], "tracked")
        self.assertEqual(shipping_cost(50.00)[1], "tracked with signature")


class HoldCheckTests(unittest.TestCase):
    def test_steady_strong_riser_is_likely(self):
        result = hold_check(daily(20, 0.004, 400), None, TCGPLAYER)  # ~+13% a month

        self.assertEqual(result["verdict"], "likely")
        self.assertGreater(result["projected_price"], result["break_even_price"])
        self.assertGreater(result["cleared_break_even_pct"], 50)

    def test_flat_card_is_not_a_flip(self):
        result = hold_check(daily(20, 0.0, 400), None, TCGPLAYER)

        self.assertEqual(result["verdict"], "unlikely")
        self.assertEqual(result["cleared_break_even_pct"], 0)
        self.assertLess(result["projected_profit"], 0)

    def test_short_history_says_so(self):
        result = hold_check(daily(20, 0.004, 60), None, TCGPLAYER)

        self.assertEqual(result["verdict"], "not_enough_history")

    def test_buy_price_defaults_to_latest_market_price(self):
        history = daily(20, 0.0, 400)
        self.assertEqual(hold_check(history, None, TCGPLAYER)["buy_price"], 20.0)
        self.assertEqual(hold_check(history, 15.0, TCGPLAYER)["buy_price"], 15.0)

    def test_empty_history(self):
        self.assertEqual(hold_check([], None, TCGPLAYER)["verdict"], "no_data")


if __name__ == "__main__":
    unittest.main()
