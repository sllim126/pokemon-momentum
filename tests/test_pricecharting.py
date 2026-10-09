import unittest

from scripts.common.pricecharting import choose_subtype, match_rows, parse_money, parse_row, printing_tags


def row(pc_id, name, tcg_id, psa10="$10.00", volume="5"):
    return parse_row({
        "id": str(pc_id), "console-name": "Pokemon Test", "product-name": name, "tcg-id": str(tcg_id),
        "loose-price": "$1.00", "manual-only-price": psa10, "sales-volume": volume,
    })


class PriceChartingParsingTests(unittest.TestCase):
    def test_parse_money(self):
        self.assertEqual(parse_money("$1,234.50"), 1234.5)
        self.assertIsNone(parse_money(""))
        self.assertIsNone(parse_money("$0.00"))

    def test_printing_tags(self):
        self.assertEqual(printing_tags("Regice [Reverse Holo] #45"), ["reverse holo"])
        self.assertEqual(printing_tags("Charizard #4"), [])


class ChooseSubtypeTests(unittest.TestCase):
    def test_tags_pick_matching_printing(self):
        subtypes = {"Normal", "Reverse Holofoil"}
        self.assertEqual(choose_subtype([], subtypes), "Normal")
        self.assertEqual(choose_subtype(["reverse holo"], subtypes), "Reverse Holofoil")

    def test_first_edition(self):
        subtypes = {"1st Edition Holofoil", "Unlimited Holofoil"}
        self.assertEqual(choose_subtype(["1st edition"], subtypes), "1st Edition Holofoil")
        self.assertEqual(choose_subtype([], subtypes), "Unlimited Holofoil")

    def test_untagged_row_is_sole_first_edition_unless_a_sibling_claims_it(self):
        self.assertEqual(choose_subtype([], {"1st Edition"}), "1st Edition")
        self.assertIsNone(choose_subtype([], {"1st Edition"}, sibling_has_first_edition=True))

    def test_printing_we_do_not_track_is_skipped(self):
        self.assertIsNone(choose_subtype(["reverse holo"], {"Holofoil"}))


class MatchRowsTests(unittest.TestCase):
    def test_one_row_per_printing_preferring_fewest_tags(self):
        rows = [row(1, "Pikachu #1", 100), row(2, "Pikachu [Prize Pack] #1", 100, volume="999"), row(3, "Pikachu [Reverse Holo] #1", 100)]

        matched, stats = match_rows(rows, {100: {"Normal", "Reverse Holofoil"}})

        by_subtype = {r["subTypeName"]: r["pc_id"] for r in matched}
        self.assertEqual(by_subtype, {"Normal": 1, "Reverse Holofoil": 3})
        self.assertEqual(stats["duplicates_dropped"], 1)

    def test_rows_outside_catalog_are_counted_not_matched(self):
        matched, stats = match_rows([row(1, "Mew #1", 999)], {100: {"Normal"}})

        self.assertEqual(matched, [])
        self.assertEqual(stats["not_in_catalog"], 1)


if __name__ == "__main__":
    unittest.main()
