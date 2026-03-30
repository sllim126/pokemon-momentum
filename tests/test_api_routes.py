import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from scripts.dashboards import api


class ApiRouteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(api.app)

    def test_dashboard_route_serves_html(self):
        response = self.client.get("/dashboard")

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Market Signals", response.text)

    def test_dashboard_common_js_serves_javascript(self):
        response = self.client.get("/dashboard-common.js")

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/javascript", response.headers["content-type"])

    def test_images_route_serves_logo_asset(self):
        response = self.client.get("/images/Logo.png")

        self.assertEqual(response.status_code, 200)

    def test_categories_route_exposes_english_and_japanese(self):
        response = self.client.get("/categories")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["items"][0]["category_id"], 3)
        self.assertEqual(payload["items"][1]["category_id"], 85)

    @patch.object(api, "prices_from", return_value="read_parquet('/tmp/mock.parquet')")
    @patch.object(api, "q", return_value=(["rows", "latest"], [(321, "2026-03-30")]))
    def test_health_route_reports_snapshot_details(self, q_mock, _prices_from_mock):
        response = self.client.get("/health?category_id=85")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["rows"], 321)
        self.assertEqual(payload["latest"], "2026-03-30")
        self.assertEqual(payload["source"], "parquet")
        self.assertEqual(payload["category_id"], 85)
        self.assertEqual(payload["category"], "Pokemon Japanese")
        q_mock.assert_called_once()

    def test_search_short_queries_return_empty_payload(self):
        response = self.client.get("/search", params={"query": "a"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["items"], [])
        self.assertEqual(response.json()["total_count"], 0)

    def test_universe_sql_can_filter_specific_product_and_subtype(self):
        with patch.object(api, "q", return_value=(["productId"], [(1,)])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"), patch.object(
            api, "category_config", return_value=api.category_config(3)
        ):
            payload = api.universe(product_id=123, sub_type_name="Normal", limit=10, category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("s.productId = 123", sql)
        self.assertIn("COALESCE(s.subTypeName, '') = 'Normal'", sql)
        self.assertEqual(payload["rows"], [(1,)])

    def test_good_buys_defaults_to_premium_cards(self):
        with patch.object(api, "q", return_value=(["groupName"], [])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            api.good_buys(category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("AND productKind = 'card'", sql)
        self.assertIn("ultra rare", sql.lower())

    def test_good_buys_can_switch_to_sealed(self):
        with patch.object(api, "q", return_value=(["groupName"], [])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "category_config", return_value=api.category_config(85)):
            api.good_buys(product_kind="sealed", category_id=85)

        sql = q_mock.call_args[0][0]
        self.assertIn("AND productKind = 'sealed'", sql)
        self.assertNotIn("ultra rare", sql.lower())

    def test_group_products_honors_product_kind_filter(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "prices_from", return_value="prices_source"
        ), patch.object(api, "product_signal_from", return_value="signal_source"), patch.object(
            api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            api.group_products(groupId=99, product_kind="sealed")

        sql = q_mock.call_args[0][0]
        self.assertIn("AND m.productKind = 'sealed'", sql)

    def test_breakouts_honors_product_kind_filter(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "prices_from", return_value="prices_source"
        ), patch.object(api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"), patch.object(
            api, "category_config", return_value=api.category_config(85)
        ):
            api.breakouts(product_kind="card", category_id=85)

        sql = q_mock.call_args[0][0]
        self.assertIn("AND m.productKind = 'card'", sql)
        self.assertIn("recent_distinct_prices_30d", sql)

    def test_early_uptrends_orders_freshest_setups_first(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            api.early_uptrends(category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("ORDER BY early_streak ASC, pct_vs_sma30 ASC, latest_price DESC", sql)
        self.assertIn("<= 8.0", sql)

