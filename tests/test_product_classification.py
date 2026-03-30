import unittest

from scripts.common.product_classification import get_product_class_sql, get_product_kind_sql


class ProductClassificationTests(unittest.TestCase):
    def test_product_class_sql_keeps_booster_boxes_and_packs_distinct(self):
        sql = get_product_class_sql("item")

        self.assertIn("%booster box%", sql)
        self.assertIn("sealed_booster_box", sql)
        self.assertIn("%booster pack%", sql)
        self.assertIn("sealed_booster_pack", sql)
        self.assertLess(sql.index("sealed_booster_box"), sql.index("sealed_booster_pack"))

    def test_product_kind_sql_no_longer_uses_overly_broad_bundle_match(self):
        sql = get_product_kind_sql("item")

        self.assertNotIn("LIKE '%bundle%'", sql)
        self.assertIn("%deck bundle%", sql)
        self.assertIn("%booster bundle%", sql)

    def test_product_kind_sql_uses_safer_tin_patterns(self):
        sql = get_product_kind_sql("item")

        self.assertIn("% tin%", sql)
        self.assertIn("tin %", sql)
        self.assertNotIn("LIKE 'tin%'", sql)

    def test_card_fallback_depends_on_number_or_rarity(self):
        class_sql = get_product_class_sql("card_row")
        kind_sql = get_product_kind_sql("card_row")

        self.assertIn("COALESCE(NULLIF(card_row.number, ''), '') <> ''", class_sql)
        self.assertIn("COALESCE(NULLIF(card_row.rarity, ''), '') <> ''", kind_sql)
        self.assertTrue(class_sql.strip().endswith("END"))
        self.assertTrue(kind_sql.strip().endswith("END"))

