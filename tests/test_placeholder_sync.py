import unittest

from scripts.utilities.sync_placeholder_library import describe_promo, nearest_set, product_match, promo_number

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


class ProductMatchTests(unittest.TestCase):
    # Added 2026-11-20: MEP115 Spritzee and MEP116 Seel were guessed as 30th Celebration from
    # their neighbours although the catalog lists the Delta Reign blisters they come in.
    PRODUCTS = [
        ("Delta Reign", "Delta Reign Single Pack Blister [Spritzee]"),
        ("Delta Reign", "Delta Reign 3 Pack Blister [Seel]"),
        ("Delta Reign", "Delta Reign Premium Checklane Blister [Delphox Line]"),
        ("Delta Reign", "Code Card - Delta Reign Blister [Seel]"),
        ("Pitch Black", "Pitch Black Premium Checklane Blister [Tyrunt, Tyrantrum] (Target Exclusive)"),
        ("Pitch Black", "Pitch Black Booster Bundle [Mewtwo]"),
    ]

    def test_bracketed_card_gives_set_and_product(self):
        self.assertEqual(product_match("Spritzee", self.PRODUCTS),
                         ("Delta Reign", "Delta Reign Single Pack Blister [Spritzee]"))
        self.assertEqual(product_match("Delphox", self.PRODUCTS)[0], "Delta Reign")
        self.assertEqual(product_match("Tyrunt", self.PRODUCTS)[0], "Pitch Black")

    def test_whole_words_only_and_code_cards_skipped(self):
        self.assertIsNone(product_match("Mew", self.PRODUCTS))
        self.assertEqual(product_match("Seel", self.PRODUCTS), ("Delta Reign", "Delta Reign 3 Pack Blister [Seel]"))

    def test_products_from_two_sets_are_ambiguous(self):
        products = self.PRODUCTS + [("Pitch Black", "Pitch Black Blister [Spritzee]")]
        self.assertIsNone(product_match("Spritzee", products))
