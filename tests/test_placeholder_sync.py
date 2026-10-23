import unittest

from scripts.utilities.sync_placeholder_library import describe_promo, nearest_set, promo_number

SETS = {"Pitch Black": {}, "30th Celebration": {}, "Mega Evolution": {}}


class DescribePromoTests(unittest.TestCase):
    def test_set_in_parentheses(self):
        self.assertEqual(describe_promo("Zapdos - 098 (30th Celebration)", SETS), ("Zapdos", "30th Celebration", "Black Star Promo"))

    def test_stamped_gives_the_set_and_a_stamp_variant(self):
        self.assertEqual(describe_promo("Slowbro - 083 (Pitch Black Stamped)", SETS), ("Slowbro", "Pitch Black", "Pitch Black Stamp"))

    def test_pokemon_center_exclusive(self):
        self.assertEqual(
            describe_promo("Nidorina - 101 (30th Celebration) (Pokemon Center Exclusive)", SETS),
            ("Nidorina", "30th Celebration", "Pokemon Center Stamp"),
        )

    def test_staff_copy(self):
        self.assertEqual(describe_promo("Miraidon - 082 [Staff]", SETS), ("Miraidon", None, "Staff Stamp"))

    def test_unknown_note_leaves_the_set_open(self):
        self.assertEqual(describe_promo("Celebratory Fanfare (Ace Trainer)", SETS), ("Celebratory Fanfare", None, "Black Star Promo"))


class HelperTests(unittest.TestCase):
    def test_promo_number_reads_both_spellings(self):
        self.assertEqual(promo_number("MEP", "MEP089"), 89)
        self.assertEqual(promo_number("MEP", "089"), 89)
        self.assertIsNone(promo_number("MEP", "POR023"))

    def test_nearest_set_prefers_the_number_below(self):
        known = {80: "Chaos Rising", 86: "Pitch Black"}
        self.assertEqual(nearest_set(84, known), "Chaos Rising")
        self.assertEqual(nearest_set(5, known), "Chaos Rising")
        self.assertIsNone(nearest_set(5, {}))
