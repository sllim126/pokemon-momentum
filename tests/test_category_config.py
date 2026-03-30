import unittest

from scripts.common.category_config import get_category_config


class CategoryConfigTests(unittest.TestCase):
    def test_known_pokemon_category_uses_expected_names(self):
        category = get_category_config(3)

        self.assertEqual(category.slug, "pokemon")
        self.assertEqual(category.label, "Pokemon")
        self.assertEqual(category.groups_csv, "pokemon_groups.csv")
        self.assertEqual(category.products_csv, "pokemon_products.csv")
        self.assertEqual(category.product_signal_table, "pokemon_product_signal_snapshot")
        self.assertEqual(category.series_snapshot_csv, "pokemon_series_snapshot.csv")

    def test_known_japanese_category_uses_expected_names(self):
        category = get_category_config(85)

        self.assertEqual(category.slug, "pokemon_jp")
        self.assertEqual(category.label, "Pokemon Japanese")
        self.assertEqual(category.groups_table, "pokemon_jp_groups")
        self.assertEqual(category.health_snapshot_table, "pokemon_jp_health_snapshot")

    def test_unknown_category_falls_back_to_stable_names(self):
        category = get_category_config(999)

        self.assertEqual(category.slug, "category_999")
        self.assertEqual(category.label, "Category 999")
        self.assertEqual(category.prices_named_table, "category_999_prices_named")
        self.assertEqual(category.sparkline_snapshot_csv, "category_999_sparkline_snapshot.csv")

    def test_all_naming_properties_return_consistent_values(self):
        category = get_category_config(3)
        expected = {
            "groups_csv": "pokemon_groups.csv",
            "products_csv": "pokemon_products.csv",
            "prices_csv": "pokemon_prices_all_days.csv",
            "prices_named_csv": "pokemon_prices_named.csv",
            "groups_table": "pokemon_groups",
            "products_table": "pokemon_products",
            "prices_named_table": "pokemon_prices_named",
            "product_signal_table": "pokemon_product_signal_snapshot",
            "product_signal_csv": "pokemon_product_signal_snapshot.csv",
            "group_signal_table": "pokemon_group_signal_snapshot",
            "group_signal_csv": "pokemon_group_signal_snapshot.csv",
            "sparkline_snapshot_table": "pokemon_sparkline_snapshot",
            "sparkline_snapshot_csv": "pokemon_sparkline_snapshot.csv",
            "health_snapshot_table": "pokemon_health_snapshot",
            "health_snapshot_csv": "pokemon_health_snapshot.csv",
            "series_snapshot_table": "pokemon_series_snapshot",
            "series_snapshot_csv": "pokemon_series_snapshot.csv",
        }

        for attr, value in expected.items():
            with self.subTest(attr=attr):
                self.assertEqual(getattr(category, attr), value)
