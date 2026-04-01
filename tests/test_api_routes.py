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

    def test_sealed_deals_page_serves_html(self):
        response = self.client.get("/sealed-deals")

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Sealed Deals", response.text)

    def test_set_explorer_page_serves_html(self):
        response = self.client.get("/set-explorer")

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Set Explorer", response.text)

    @patch.object(api, "get_session_user", return_value=type("SessionUser", (), {"username": "sllim126", "user_id": 1})())
    def test_bug_reports_page_serves_html(self, _get_session_user_mock):
        response = self.client.get("/bug-reports", cookies={"pm_tracking_token": "token"})

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Bug Reports", response.text)

    @patch.object(api, "get_session_user", return_value=type("SessionUser", (), {"username": "collector1", "user_id": 2})())
    def test_admin_pages_block_non_admin_users(self, _get_session_user_mock):
        response = self.client.get("/bug-reports", cookies={"pm_tracking_token": "token"})

        self.assertEqual(response.status_code, 403)

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

    def test_universe_sql_can_filter_multiple_keys(self):
        with patch.object(api, "q", return_value=(["productId"], [(1,), (2,)])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"), patch.object(
            api, "category_config", return_value=api.category_config(3)
        ):
            api.universe(keys="123||Normal,456||Reverse Holofoil", limit=10, category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("(s.productId = 123 AND COALESCE(s.subTypeName, '') = 'Normal')", sql)
        self.assertIn("(s.productId = 456 AND COALESCE(s.subTypeName, '') = 'Reverse Holofoil')", sql)

    def test_product_picker_sql_supports_offset(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"), patch.object(
            api, "category_config", return_value=api.category_config(3)
        ):
            api.product_picker(limit=1000, offset=2000, category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("OFFSET 2000", sql)
        self.assertIn("LIMIT 1000", sql)

    def test_groups_sql_supports_offset(self):
        with patch.object(api, "q", return_value=(["groupId"], [])) as q_mock, patch.object(
            api, "prices_from", return_value="prices_source"
        ), patch.object(api, "groups_from", return_value="groups_source"), patch.object(
            api, "category_config", return_value=api.category_config(85)
        ):
            api.groups(limit=500, offset=1000, category_id=85)

        sql = q_mock.call_args[0][0]
        self.assertIn("OFFSET 1000", sql)
        self.assertIn("LIMIT 500", sql)

    @patch.object(api, "create_bug_report", return_value=17)
    def test_bug_report_submission_persists_structured_payload(self, create_bug_report_mock):
        response = self.client.post(
            "/bug_reports",
            json={
                "title": "Tracked prices missing",
                "details": "Tracked items on mobile were rendering n/a after refresh.",
                "expected": "Tracked prices should stay visible.",
                "page_path": "/dashboard",
                "page_url": "https://market.poke6s.com/dashboard",
                "category_id": 3,
                "tab": "tracked_items",
                "segment": "cards",
                "chart_mode": "product",
                "product_key": "123||Normal",
                "group_id": 456,
                "search_query": "venusaur",
                "reporter_username": "adam",
                "user_agent": "pytest",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True, "id": 17})
        payload = create_bug_report_mock.call_args[0][0]
        self.assertEqual(payload["title"], "Tracked prices missing")
        self.assertEqual(payload["tab"], "tracked_items")
        self.assertIn('"discord_status": "not_configured"', payload["context_json"])

    def test_bug_report_submission_rejects_short_details(self):
        response = self.client.post(
            "/bug_reports",
            json={
                "title": "Nope",
                "details": "too short",
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Details must be at least 10 characters", response.text)

    @patch.object(api, "list_bug_reports", return_value=[{"id": 1, "title": "Example"}])
    def test_bug_report_list_route_returns_items(self, list_bug_reports_mock):
        with patch.object(api, "get_session_user", return_value=type("SessionUser", (), {"username": "sllim126", "user_id": 1})()):
            response = self.client.get("/bug_reports?limit=50", cookies={"pm_tracking_token": "token"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"items": [{"id": 1, "title": "Example"}]})
        list_bug_reports_mock.assert_called_once_with(limit=50)

    @patch.object(api, "get_session_user", return_value=type("SessionUser", (), {"username": "collector1", "user_id": 2})())
    def test_bug_report_list_route_blocks_non_admin(self, _get_session_user_mock):
        response = self.client.get("/bug_reports?limit=50", cookies={"pm_tracking_token": "token"})

        self.assertEqual(response.status_code, 403)

    @patch.object(api, "get_session_user", return_value=type("SessionUser", (), {"username": "sllim126", "user_id": 1})())
    def test_tracking_session_status_returns_admin_flag(self, _get_session_user_mock):
        response = self.client.get("/tracking/session", headers={"Authorization": "Bearer token"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["user"]["username"], "sllim126")
        self.assertTrue(response.json()["user"]["is_admin"])

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

    def test_set_baskets_sql_stays_card_only(self):
        with patch.object(api, "q", return_value=(["groupId"], [])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "groups_from", return_value="groups_source"), patch.object(
            api, "build_set_basket_filter", return_value="1=1"
        ), patch.object(api, "build_generation_case", return_value="'SV'"), patch.object(
            api, "category_config", return_value=api.category_config(3)
        ):
            api.set_baskets(category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("COALESCE(s.productKind, '') = 'card'", sql)
        self.assertIn("top3_price", sql)

    def test_sealed_pack_override_matches_moltres_upc(self):
        override = api._find_pack_composition_override("Team Rocket's Moltres ex Ultra-Premium Collection")

        self.assertIsNotNone(override)
        self.assertEqual(override["product_type"], "Ultra Premium Collection")
        self.assertEqual(
            override["packs"],
            [
                {"set": "Destined Rivals", "count": 2},
                {"set": "Journey Together", "count": 4},
                {"set": "Temporal Forces", "count": 2},
                {"set": "Paradox Rift", "count": 2},
                {"set": "Obsidian Flames", "count": 3},
                {"set": "Paldea Evolved", "count": 3},
                {"set": "Scarlet and Violet", "count": 2},
            ],
        )

    def test_named_etb_pack_count_overrides_cover_user_verified_examples(self):
        crown_pc = api._find_pack_count_override_by_name("Pokemon Center Elite Trainer Box Crown Zenith")
        crown_regular = api._find_pack_count_override_by_name("Elite Trainer Box Crown Zenith")
        fusion_pc = api._find_pack_count_override_by_name("Pokemon Center Elite Trainer Box Fusion Strike")
        fusion_regular = api._find_pack_count_override_by_name("Sword & Shield Fusion Strike Elite Trainer Box")
        solgaleo_regular = api._find_pack_count_override_by_name("Sun & Moon Elite Trainer Box Solgaleo")
        celestial_regular = api._find_pack_count_override_by_name("Sun & Moon Celestial Storm Elite Trainer Box")
        brilliant_regular = api._find_pack_count_override_by_name("Sword & Shield Brilliant Stars Elite Trainer Box")
        silver_regular = api._find_pack_count_override_by_name("Sword & Shield Silver Tempest Elite Trainer Box")
        sv_pc = api._find_pack_count_override_by_name("Pokemon Center Elite Trainer Box Scarlet & Violet Miraidon")
        sv_regular = api._find_pack_count_override_by_name("Pokemon Scarlet & Violet Elite Trainer Box Miraidon")
        mega_pc = api._find_pack_count_override_by_name("Pokemon Center Elite Trainer Box Mega Evolution Lucario")
        mega_regular = api._find_pack_count_override_by_name("Pokemon Mega Evolution Elite Trainer Box Lucario")

        self.assertEqual(crown_pc["pack_count"], 12)
        self.assertEqual(crown_regular["pack_count"], 10)
        self.assertEqual(fusion_pc["pack_count"], 8)
        self.assertEqual(fusion_regular["pack_count"], 6)
        self.assertEqual(solgaleo_regular["pack_count"], 6)
        self.assertEqual(celestial_regular["pack_count"], 6)
        self.assertEqual(brilliant_regular["pack_count"], 8)
        self.assertEqual(silver_regular["pack_count"], 8)
        self.assertEqual(sv_pc["pack_count"], 11)
        self.assertEqual(sv_regular["pack_count"], 9)
        self.assertEqual(mega_pc["pack_count"], 11)
        self.assertEqual(mega_regular["pack_count"], 9)

    def test_sealed_deals_prefers_name_based_etb_override_when_product_id_is_unknown(self):
        with patch.object(api, "q", return_value=(
            ["latest_date", "groupId", "groupName", "productId", "productName", "imageUrl", "rarity", "number", "productClass", "productKind", "subTypeName", "latest_price"],
            [("2026-04-01", 1, "Crown Zenith", 999999, "Pokemon Center Elite Trainer Box Crown Zenith", "", None, None, "sealed_etb", "sealed", "", 119.99)],
        )), patch.object(api, "_product_signal_source_resilient", return_value="signal_source"), patch.object(
            api, "category_config", return_value=api.category_config(3)
        ):
            payload = api.sealed_deals(category_id=3, limit=10)

        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["pack_count"], 12)
        self.assertEqual(payload["items"][0]["productType"], "Pokemon Center ETB")
