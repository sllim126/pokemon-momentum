import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
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

    def test_dashboard_route_serves_mobile_page_for_mobile_user_agents(self):
        response = self.client.get(
            "/dashboard",
            headers={
                "user-agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Mobile/15E148"
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Deal Checker", response.text)

    def test_mobile_route_serves_mobile_dashboard(self):
        response = self.client.get("/mobile")

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Deal Checker", response.text)

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
    def test_pricing_upload_page_serves_html(self, _get_session_user_mock):
        response = self.client.get("/pricing-upload", cookies={"pm_tracking_token": "token"})

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Pricing Upload", response.text)

    @patch.object(api, "get_session_user", return_value=type("SessionUser", (), {"username": "sllim126", "user_id": 1})())
    def test_supplier_profitability_page_serves_html(self, _get_session_user_mock):
        response = self.client.get("/supplier-profitability", cookies={"pm_tracking_token": "token"})

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Supplier Profitability", response.text)

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

    def test_search_sql_supports_card_and_set_token_queries(self):
        with patch.object(api, "q", return_value=(["kind"], [])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"), patch.object(
            api, "groups_from", return_value="groups_source"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            api.search(query="zekrom brilliant stars", limit=12, category_id=3)

        sql = "\n".join(call.args[0] for call in q_mock.call_args_list)
        self.assertIn("zekrom", sql.lower())
        self.assertIn("brilliant", sql.lower())
        self.assertIn("COALESCE(m.productName", sql)
        self.assertIn("COALESCE(m.groupName", sql)
        self.assertIn("AND", sql)

    def test_search_sql_supports_set_code_tokens_on_product_matches(self):
        with patch.object(api, "q", return_value=(["kind"], [])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"), patch.object(
            api, "groups_from", return_value="groups_source"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            api.search(query="zekrom jtg", limit=12, category_id=3)

        sql = "\n".join(call.args[0] for call in q_mock.call_args_list)
        self.assertIn("COALESCE(m.groupAbbreviation", sql)
        self.assertIn("jtg", sql.lower())

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

    @patch.object(api, "get_session_user", return_value=type("SessionUser", (), {"username": "sllim126", "user_id": 1})())
    def test_pricing_upload_compare_saves_csv_and_returns_summary(self, _get_session_user_mock):
        source_csv = (
            b"Product ID [Non Editable],SKU,Title,Price,Sale Price\n"
            b"123,ABC-123,Test Product,12.00,0.00\n"
        )
        with TemporaryDirectory() as tmpdir:
            tmp_root = Path(tmpdir)
            export_path = tmp_root / "products.csv"
            archive_dir = tmp_root / "archive"
            summary = {
                "export_rows": 1,
                "covered_rows": 1,
                "target_rows": 1,
                "missing_from_export": [],
                "unmatched_rules": [],
                "preview": [{"sku": "ABC-123"}],
                "total_rules": 1,
                "manual_rules": 0,
                "auto_rules": 1,
            }
            with patch.object(api, "SQUARESPACE_EXPORT_CSV", export_path), patch.object(api, "SQUARESPACE_EXPORT_ARCHIVE_DIR", archive_dir), patch.object(
                api, "summarize_uploaded_export", return_value=summary
            ):
                response = self.client.post(
                    "/pricing-upload/compare",
                    files={"file": ("products.csv", source_csv, "text/csv")},
                    cookies={"pm_tracking_token": "token"},
                )

                self.assertEqual(response.status_code, 200)
                payload = response.json()
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["saved_to"], str(export_path))
                self.assertTrue(export_path.exists())
                self.assertGreater(payload["export_rows"], 0)
                self.assertGreater(payload["covered_rows"], 0)
                self.assertTrue(payload["preview"])

    @patch.object(api, "get_session_user", return_value=type("SessionUser", (), {"username": "sllim126", "user_id": 1})())
    def test_pricing_upload_compare_rejects_non_squarespace_csv(self, _get_session_user_mock):
        response = self.client.post(
            "/pricing-upload/compare",
            files={"file": ("bad.csv", b"sku,price\nABC,1.00\n", "text/csv")},
            cookies={"pm_tracking_token": "token"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Squarespace", response.text)

    @patch.object(api, "load_latest_market_targets", return_value={"JP-TEST-BOX": {"market_price": "120.00", "target_price": "125.00", "title": "Test Set Box"}})
    @patch.object(api, "load_current_store_mapping", return_value={"JP-TEST-BOX": {"current_price": "129.99", "title": "Test Set Box"}})
    @patch.object(api, "load_latest_supplier_quotes", return_value=([{"sku": "JP-TEST-BOX", "cost_jpy": "6500", "quote_date": "2026-04-27", "supplier_name": "Test Supplier", "item_name_raw": "Test Set Box"}], []))
    @patch.object(api, "get_session_user", return_value=type("SessionUser", (), {"username": "sllim126", "user_id": 1})())
    def test_supplier_profitability_data_returns_channel_breakdown(
        self,
        _get_session_user_mock,
        _load_latest_supplier_quotes_mock,
        _load_current_store_mapping_mock,
        _load_latest_market_targets_mock,
    ):
        response = self.client.post(
            "/supplier-profitability/data",
            json={
                "jpy_per_usd": 100,
                "import_duty_pct": 0,
                "inbound_shipping_mode": "manual",
                "inbound_shipping_usd": 5,
                "handling_cost_usd": 1,
                "outbound_shipping_usd": 8,
                "shipping_credit_usd": 0,
                "disbursement_fee_usd": 0,
                "income_tax_pct": 25,
                "target_margin_pct": 15,
                "channels": {
                    "site": {"reference_source": "store", "platform_fee_pct": 0, "payment_fee_pct": 3, "payment_fee_fixed": 0.3},
                    "ebay": {"reference_source": "target", "platform_fee_pct": 13.25, "payment_fee_pct": 0, "payment_fee_fixed": 0.3},
                    "tcgplayer": {"reference_source": "market", "platform_fee_pct": 10.25, "payment_fee_pct": 2.5, "payment_fee_fixed": 0.3},
                },
            },
            cookies={"pm_tracking_token": "token"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["stats"]["rows"], 1)
        row = payload["rows"][0]
        self.assertEqual(row["sku"], "JP-TEST-BOX")
        self.assertEqual(row["best_channel_key"], "site")
        self.assertEqual(row["decision"], "Buy")
        self.assertEqual(row["channels"]["site"]["reference_source"], "store")
        self.assertEqual(row["channels"]["site"]["decision"], "Buy")
        self.assertIn("ebay", row["channels"])
        self.assertIn("tcgplayer", row["channels"])
        self.assertGreater(row["channels"]["ebay"]["required_price_for_target_margin"], row["channels"]["site"]["required_price_for_target_margin"])

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

    @patch.object(api, "GOOGLE_CLIENT_ID", "google-client-id")
    def test_tracking_auth_config_exposes_google_client_state(self):
        response = self.client.get("/tracking/auth_config")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {"google_enabled": True, "google_client_id": "google-client-id"},
        )

    @patch.object(api, "create_session", return_value="session-token")
    @patch.object(api, "get_user_by_username", return_value={"id": 7, "username": "collector@example.com"})
    @patch.object(api, "verify_google_identity_token", return_value={"email": "collector@example.com", "sub": "abc123"})
    def test_tracking_google_session_reuses_existing_user(
        self,
        verify_google_identity_token_mock,
        get_user_by_username_mock,
        create_session_mock,
    ):
        response = self.client.post("/tracking/google_session", json={"credential": "google-token"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["token"], "session-token")
        self.assertEqual(response.json()["user"]["username"], "collector@example.com")
        self.assertFalse(response.json()["user"]["is_admin"])
        verify_google_identity_token_mock.assert_called_once_with("google-token")
        get_user_by_username_mock.assert_called_once_with("collector@example.com")
        create_session_mock.assert_called_once_with(7)

    @patch.object(api, "create_session", return_value="session-token")
    @patch.object(api, "create_google_user", return_value=11)
    @patch.object(api, "get_user_by_username", return_value=None)
    @patch.object(api, "verify_google_identity_token", return_value={"email": "newuser@example.com", "sub": "abc123"})
    def test_tracking_google_session_creates_missing_user(
        self,
        verify_google_identity_token_mock,
        get_user_by_username_mock,
        create_google_user_mock,
        create_session_mock,
    ):
        response = self.client.post("/tracking/google_session", json={"credential": "google-token"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["user"]["username"], "newuser@example.com")
        verify_google_identity_token_mock.assert_called_once_with("google-token")
        get_user_by_username_mock.assert_called_once_with("newuser@example.com")
        create_google_user_mock.assert_called_once_with("newuser@example.com")
        create_session_mock.assert_called_once_with(11)

    @patch.object(api, "delete_user")
    @patch.object(api, "get_session_user", return_value=type("SessionUser", (), {"username": "collector@example.com", "user_id": 9})())
    def test_tracking_account_delete_requires_session_but_not_pin(self, _get_session_user_mock, delete_user_mock):
        response = self.client.request("DELETE", "/tracking/account", headers={"Authorization": "Bearer token"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})
        delete_user_mock.assert_called_once_with(9)

    def test_good_buys_defaults_to_premium_cards(self):
        with patch.object(api, "q", return_value=(["groupName"], [])) as q_mock, patch.object(
            api, "screener_snapshot_from", return_value="screener_snapshot"
        ), patch.object(
            api, "category_config", return_value=api.category_config(3)
        ):
            api.good_buys(category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("AND productKind = 'card'", sql)
        self.assertIn("ultra rare", sql.lower())
        self.assertIn("COALESCE(roc_30d_pct, 0) <= 0.0", sql)
        self.assertIn("COALESCE(roc_90d_pct, 0) <= 20.0", sql)
        self.assertIn("COALESCE(roc_365d_pct, 0) <= 120.0", sql)
        self.assertIn("COALESCE(roc_7d_pct, 0) <= 6.0", sql)
        self.assertIn("COALESCE(price_vs_sma90_pct, 0) <= 20.0", sql)
        self.assertIn("recent_price_points >= 3", sql)
        self.assertIn("floor_observations_7d >= 5", sql)
        self.assertIn("<= 10.0", sql)
        self.assertIn("latest_price >= 80.0", sql)
        self.assertIn("COALESCE(price_vs_sma30_pct, 0) >= -12.0", sql)
        self.assertIn("COALESCE(price_vs_sma30_pct, 0) <= 2.5", sql)
        self.assertIn("FROM screener_snapshot", sql)
        self.assertIn("ORDER BY", sql)
        self.assertIn("COALESCE(recent_distinct_prices_30d, 0) DESC", sql)

    def test_good_buys_keeps_premium_pullbacks_near_support(self):
        with patch.object(api, "q", return_value=(["groupName"], [])) as q_mock, patch.object(
            api, "screener_snapshot_from", return_value="screener_snapshot"
        ), patch.object(
            api, "category_config", return_value=api.category_config(3)
        ):
            api.good_buys(category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("latest_price >= 80.0", sql)
        self.assertIn("COALESCE(roc_30d_pct, 0) <= 0.0", sql)
        self.assertIn("COALESCE(roc_90d_pct, 0) <= 35.0", sql)
        self.assertIn("COALESCE(roc_365d_pct, 0) <= 120.0", sql)
        self.assertIn("COALESCE(roc_7d_pct, 0) <= 2.0", sql)
        self.assertIn("COALESCE(price_vs_sma90_pct, 0) <= 20.0", sql)
        self.assertIn("COALESCE(price_vs_sma30_pct, 0) >= -12.0", sql)
        self.assertIn("COALESCE(price_vs_sma30_pct, 0) <= 2.5", sql)
        self.assertIn("COALESCE(roc_7d_pct, 0) < 0", sql)

    def test_good_buys_can_switch_to_sealed(self):
        with patch.object(api, "q", return_value=(["groupName"], [])) as q_mock, patch.object(
            api, "screener_snapshot_from", return_value="screener_snapshot"
        ), patch.object(
            api, "category_config", return_value=api.category_config(85)
        ):
            api.good_buys(product_kind="sealed", category_id=85)

        sql = q_mock.call_args[0][0]
        self.assertIn("AND productKind = 'sealed'", sql)
        self.assertNotIn("ultra rare", sql.lower())

    def test_good_buys_can_apply_a_max_price_filter(self):
        with patch.object(api, "q", return_value=(["groupName"], [])) as q_mock, patch.object(
            api, "screener_snapshot_from", return_value="screener_snapshot"
        ), patch.object(
            api, "category_config", return_value=api.category_config(3)
        ):
            api.good_buys(category_id=3, min_price=25, max_price=80)

        sql = q_mock.call_args[0][0]
        self.assertIn("AND latest_price >= 25", sql)
        self.assertIn("AND latest_price <= 80", sql)

    def test_good_buys_can_fallback_to_live_history_for_non_default_floor_window(self):
        with patch.object(api, "q", return_value=(["groupName"], [])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "prices_from", return_value="prices_source"), patch.object(
            api, "category_config", return_value=api.category_config(3)
        ):
            api.good_buys(category_id=3, floor_days=9)

        sql = q_mock.call_args[0][0]
        self.assertIn("FROM prices_source", sql)
        self.assertIn("fw.floor_observations", sql)

    def test_top_movers_defaults_require_stronger_recent_activity(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            api.top_movers(category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("COALESCE(top_mover_signal_days, 0) >= 4", sql)
        self.assertIn("COALESCE(top_mover_recent_observations, 0) >= 5", sql)
        self.assertIn("COALESCE(top_mover_recent_distinct_prices, 0) >= 4", sql)
        self.assertIn("top_mover_last_change_date >= latest_date - INTERVAL 3 DAY", sql)
        self.assertIn("ORDER BY", sql)
        self.assertIn("top_mover_recent_distinct_prices DESC", sql)
        self.assertIn("top_mover_signal_days DESC", sql)

    def test_top_movers_live_query_uses_quality_tiebreakers(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "prices_from", return_value="prices_source"
        ), patch.object(api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"), patch.object(
            api, "category_config", return_value=api.category_config(3)
        ):
            api.top_movers(category_id=3, recent_variation_window_days=10)

        sql = q_mock.call_args[0][0]
        self.assertIn("COALESCE(rv.recent_observations, 0) >= 5", sql)
        self.assertIn("COALESCE(rv.recent_distinct_prices, 0) >= 4", sql)
        self.assertIn("ra.last_change_date >= (SELECT max_date FROM d) - INTERVAL 3 DAY", sql)
        self.assertIn("ORDER BY", sql)
        self.assertIn("rv.recent_distinct_prices DESC", sql)
        self.assertIn("a.signal_days DESC", sql)

    def test_time_to_buy_uses_recent_variance_floor_logic(self):
        with patch.object(api, "q", return_value=(["groupName"], [])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "prices_from", return_value="prices_source"), patch.object(
            api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            api.time_to_buy(category_id=3, group_id=123, product_kind="card")

        sql = q_mock.call_args[0][0]
        self.assertIn("latest_price <= latest_sma30", sql)
        self.assertIn("recent_low", sql)
        self.assertIn("recent_high", sql)
        self.assertIn("variance_to_current_pct", sql)
        self.assertIn("recent_distinct_prices_30d", sql)
        self.assertIn(">= 10", sql)
        self.assertIn("AND COALESCE(m.productKind, s.productKind, '') = 'card'", sql)
        self.assertIn("AND s.groupId = 123", sql)

    def test_time_to_buy_requires_a_selected_set(self):
        result = api.time_to_buy(category_id=3)
        self.assertEqual(result.get("rows"), [])
        self.assertEqual(result.get("columns"), [])

    def test_time_to_buy_can_filter_to_one_set(self):
        with patch.object(api, "q", return_value=(["groupName"], [])) as q_mock, patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "prices_from", return_value="prices_source"), patch.object(
            api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"
        ), patch.object(api, "category_config", return_value=api.category_config(85)):
            api.time_to_buy(category_id=85, group_id=606)

        sql = q_mock.call_args[0][0]
        self.assertIn("AND s.groupId = 606", sql)

    def test_group_products_honors_product_kind_filter(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "prices_from", return_value="prices_source"
        ), patch.object(api, "product_signal_from", return_value="signal_source"), patch.object(
            api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            api.group_products(groupId=99, product_kind="sealed")

        sql = q_mock.call_args[0][0]
        self.assertIn("AND m.productKind = 'sealed'", sql)

    def test_group_products_can_apply_server_side_browse_set_filters(self):
        cols = ["productName", "groupName", "rarity", "subTypeName", "productId"]
        rows = [
            ("Common Card", "Black Bolt", "Common", "", 1),
            ("Special Card", "Black Bolt", "Special Illustration Rare", "", 2),
        ]
        with patch.object(api, "q", return_value=(cols, rows)), patch.object(
            api, "prices_from", return_value="prices_source"
        ), patch.object(api, "product_signal_from", return_value="signal_source"), patch.object(
            api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            result = api.group_products(groupId=99, product_kind="card", filters="ir_plus")

        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0][0], "Special Card")
        self.assertIn("common", result["available_filters"])
        self.assertIn("ir_plus", result["available_filters"])

    def test_breakouts_honors_product_kind_filter(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "prices_from", return_value="prices_source"
        ), patch.object(api, "product_signal_from", return_value="product_signal_snapshot"), patch.object(
            api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"
        ), patch.object(
            api, "category_config", return_value=api.category_config(85)
        ):
            api.breakouts(product_kind="card", category_id=85)

        sql = q_mock.call_args[0][0]
        self.assertIn("AND m.productKind = 'card'", sql)
        self.assertIn("recent_distinct_prices_30d", sql)
        self.assertIn("COALESCE(ls.hold_days, 0) <= 7", sql)
        self.assertIn("FROM product_signal_snapshot", sql)

    def test_breakouts_excludes_established_sma30_holds(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "prices_from", return_value="prices_source"
        ), patch.object(api, "product_signal_from", return_value="product_signal_snapshot"), patch.object(
            api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"
        ), patch.object(
            api, "category_config", return_value=api.category_config(85)
        ):
            api.breakouts(category_id=85, max_hold_days=7)

        sql = q_mock.call_args[0][0]
        self.assertIn("COALESCE(ls.hold_days, 0) <= 7", sql)
        self.assertIn("LEFT JOIN latest_signal ls", sql)

    def test_early_uptrends_prefers_quiet_names_just_starting_to_lift(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "screener_snapshot_from", return_value="screener_snapshot"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            api.early_uptrends(category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("COALESCE(s.roc_7d_pct, 0) >= 1.0", sql)
        self.assertIn("COALESCE(s.roc_7d_pct, 0) <= 7.0", sql)
        self.assertIn("COALESCE(s.roc_30d_pct, 0) <= 8.0", sql)
        self.assertIn("COALESCE(s.roc_90d_pct, 0) <= 20.0", sql)
        self.assertIn("COALESCE(s.acceleration_7d_vs_30d, 0) >= 0.5", sql)
        self.assertIn("COALESCE(s.recent_observations_7d, 0) >= 4", sql)
        self.assertIn("COALESCE(s.hold_days, 0) <= 10", sql)
        self.assertIn("s.cross_date >= s.latest_date - INTERVAL 14 DAY", sql)
        self.assertIn("s.recent_price_points >= 3", sql)
        self.assertIn("s.latest_price_1d > s.latest_price_2d", sql)
        self.assertIn("s.latest_price_2d > s.latest_price_3d", sql)
        self.assertIn("FROM screener_snapshot s", sql)
        self.assertIn("ORDER BY", sql)
        self.assertIn("pct_vs_sma30 ASC", sql)
        self.assertIn("s.roc_7d_pct ASC", sql)
        self.assertIn("s.acceleration_7d_vs_30d DESC", sql)
        self.assertIn("s.recent_distinct_prices_7d DESC", sql)
        self.assertIn("<= 8.0", sql)

    def test_early_uptrends_live_query_requires_fresh_cross_and_short_hold(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "screener_snapshot_from", side_effect=api.HTTPException(status_code=404, detail="missing")
        ), patch.object(api, "product_signal_from", return_value="product_signal_snapshot"), patch.object(
            api, "prices_from", return_value="prices_source"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            api.early_uptrends(category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("COALESCE(s.hold_days, 0) <= 10", sql)
        self.assertIn("s.cross_date >= s.latest_date - INTERVAL 14 DAY", sql)
        self.assertIn("rl.latest_price_1d > rl.latest_price_2d", sql)
        self.assertIn("rl.latest_price_2d > rl.latest_price_3d", sql)

    def test_under_the_radar_prefers_quiet_bases_with_fresh_lift(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "screener_snapshot_from", return_value="screener_snapshot"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            api.under_the_radar(category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("COALESCE(s.hold_days, 0) <= 7", sql)
        self.assertIn("s.cross_date >= s.latest_date - INTERVAL 14 DAY", sql)
        self.assertIn("COALESCE(s.roc_7d_pct, 0) >= 2.0", sql)
        self.assertIn("COALESCE(s.roc_7d_pct, 0) <= 8.0", sql)
        self.assertIn("COALESCE(s.roc_30d_pct, 0) <= 8.0", sql)
        self.assertIn("COALESCE(s.roc_90d_pct, 0) <= 15.0", sql)
        self.assertIn("COALESCE(s.acceleration_7d_vs_30d, 0) >= 2.0", sql)
        self.assertIn("COALESCE(s.recent_observations_7d, 0) >= 4", sql)
        self.assertIn("COALESCE(rac.above30_crosses_180d, 0) <= 5", sql)
        self.assertIn("ORDER BY", sql)
        self.assertIn("s.roc_30d_pct ASC", sql)
        self.assertIn("s.roc_90d_pct ASC", sql)
        self.assertIn("pct_vs_sma30 ASC", sql)

    def test_under_the_radar_live_query_uses_recent_lift_checks(self):
        with patch.object(api, "q", return_value=(["productId"], [])) as q_mock, patch.object(
            api, "screener_snapshot_from", side_effect=api.HTTPException(status_code=404, detail="missing")
        ), patch.object(api, "product_signal_from", return_value="product_signal_snapshot"), patch.object(
            api, "prices_from", return_value="prices_source"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            api.under_the_radar(category_id=3)

        sql = q_mock.call_args[0][0]
        self.assertIn("COALESCE(s.hold_days, 0) <= 7", sql)
        self.assertIn("s.cross_date >= s.latest_date - INTERVAL 14 DAY", sql)
        self.assertIn("COALESCE(rac.above30_crosses_180d, 0) <= 5", sql)
        self.assertIn("rl.latest_price_1d > rl.latest_price_2d", sql)
        self.assertIn("rl.latest_price_2d > rl.latest_price_3d", sql)

    def test_time_to_buy_returns_available_browse_set_filters(self):
        cols = ["productName", "groupName", "rarity", "subTypeName", "productId"]
        rows = [
            ("Ball Variant", "White Flare", "Common", "Poke Ball Pattern", 1),
            ("Hit Variant", "White Flare", "Double Rare", "", 2),
        ]
        with patch.object(api, "q", return_value=(cols, rows)), patch.object(
            api, "product_signal_from", return_value="product_signal_snapshot"
        ), patch.object(api, "prices_from", return_value="prices_source"), patch.object(
            api, "build_metadata_cte", return_value="metadata AS (SELECT 1)"
        ), patch.object(api, "category_config", return_value=api.category_config(3)):
            result = api.time_to_buy(category_id=3, group_id=123, product_kind="card", filters="pokeball_holo")

        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0][0], "Ball Variant")
        self.assertIn("pokeball_holo", result["available_filters"])

    def test_tracking_items_resolve_assembles_rows_server_side(self):
        universe_payload = {
            "columns": [
                "productId",
                "subTypeName",
                "groupId",
                "groupName",
                "productName",
                "imageUrl",
                "rarity",
                "number",
                "productClass",
                "productKind",
                "latest_price",
                "latest_date",
            ],
            "rows": [
                [123, "Normal", 77, "Test Set", "Test Card", "img.png", "Rare", "12/99", "card", "card", 9.5, "2026-04-14"],
            ],
        }
        with patch.object(api, "universe", return_value=universe_payload):
            result = api.tracking_items_resolve(
                {
                    "category_id": 3,
                    "segment": "cards",
                    "tracked_tag": "favorite",
                    "tracked_sort": "productName",
                    "items": [
                        {"category_id": 3, "product_id": 123, "sub_type_name": "Normal", "tag": "favorite"},
                        {"category_id": 3, "product_id": 123, "sub_type_name": "Normal", "tag": "watchlist"},
                    ],
                }
            )

        self.assertEqual(result["columns"][0], "productId")
        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0][2], "Test Card")
        self.assertEqual(result["rows"][0][-1], "Favorite, Watchlist")

    def test_browse_species_returns_sorted_species_rows(self):
        search_payload = {
            "items": [
                {
                    "kind": "product",
                    "productId": 2,
                    "groupId": 10,
                    "groupName": "Set B",
                    "productName": "Pikachu - 010/100",
                    "imageUrl": "b.png",
                    "rarity": "Common",
                    "number": "010/100",
                    "productClass": "card",
                    "productKind": "card",
                    "subTypeName": "Normal",
                    "latest_price": 2.5,
                    "latest_date": "2026-04-14",
                    "title": "Pikachu - 010/100",
                },
                {
                    "kind": "product",
                    "productId": 1,
                    "groupId": 9,
                    "groupName": "Set A",
                    "productName": "Pikachu - 002/100",
                    "imageUrl": "a.png",
                    "rarity": "Rare",
                    "number": "002/100",
                    "productClass": "card",
                    "productKind": "card",
                    "subTypeName": "Normal",
                    "latest_price": 4.0,
                    "latest_date": "2026-04-14",
                    "title": "Pikachu - 002/100",
                },
                {"kind": "set", "groupId": 99, "title": "Pikachu Set"},
            ]
        }
        with patch.object(api, "search", return_value=search_payload):
            result = api.browse_species("Pikachu")

        self.assertEqual(result["species_query"], "pikachu")
        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual(result["rows"][0][0], 1)
        self.assertEqual(result["rows"][1][0], 2)

    def test_store_link_prefers_visible_direct_sku_listing(self):
        with patch.object(
            api,
            "load_squarespace_listing_by_sku",
            return_value={"637647": {"sku": "637647", "title": "Prism Energy", "url": "https://www.poke6s.com/shop/p/prism-energy"}},
        ), patch.object(api, "load_tcgplayer_sku_mapping", return_value={}), patch.object(
            api, "_store_link_direct_match_allowed", return_value=True
        ):
            result = api.store_link(product_id=637647)

        self.assertEqual(
            result,
            {
                "listed": True,
                "sku": "637647",
                "title": "Prism Energy",
                "url": "https://www.poke6s.com/shop/p/prism-energy",
            },
        )

    def test_load_squarespace_listing_by_sku_skips_out_of_stock_rows(self):
        export_path = api.SQUARESPACE_EXPORT_CSV
        original_text = export_path.read_text() if export_path.exists() else None
        try:
            export_path.write_text(
                "SKU,Visible,Stock,Product Page,Product URL,Title\n"
                "IN-STOCK,Yes,2,shop,in-stock-item,In Stock Item\n"
                "UNLIMITED,Yes,Unlimited,shop,unlimited-item,Unlimited Item\n"
                "OUT-OF-STOCK,Yes,0,shop,out-of-stock-item,Out Of Stock Item\n"
                "HIDDEN,No,5,shop,hidden-item,Hidden Item\n"
            )
            listing = api.load_squarespace_listing_by_sku()
        finally:
            if original_text is None:
                export_path.unlink(missing_ok=True)
            else:
                export_path.write_text(original_text)

        self.assertIn("IN-STOCK", listing)
        self.assertIn("UNLIMITED", listing)
        self.assertNotIn("OUT-OF-STOCK", listing)
        self.assertNotIn("HIDDEN", listing)
        self.assertEqual(listing["IN-STOCK"]["url"], "https://www.poke6s.com/shop/p/in-stock-item")

    def test_store_link_can_resolve_mapped_product_listing(self):
        with patch.object(
            api,
            "load_squarespace_listing_by_sku",
            return_value={"JP-MD-BOX": {"sku": "JP-MD-BOX", "title": "Mega Dream", "url": "https://www.poke6s.com/shop/p/mega-dream"}},
        ), patch.object(api, "load_tcgplayer_sku_mapping", return_value={"123456": "JP-MD-BOX"}):
            result = api.store_link(product_id=123456)

        self.assertTrue(result["listed"])
        self.assertEqual(result["sku"], "JP-MD-BOX")

    def test_load_tcgplayer_sku_mapping_supports_multiple_product_ids_per_sku(self):
        mapping_path = api.SCRIPT_DIR.parents[1] / "data" / "squarespace_tcgplayer_mapping.csv"
        original_text = mapping_path.read_text() if mapping_path.exists() else None
        try:
            mapping_path.write_text(
                "sku,tcgplayer_product_id,pricing_mode,min_price,note\n"
                "ENG-DR-ART,635053|649711,market_minus_5_pct_99,,Art bundle aliases\n"
            )
            mapping = api.load_tcgplayer_sku_mapping()
        finally:
            if original_text is None:
                mapping_path.unlink(missing_ok=True)
            else:
                mapping_path.write_text(original_text)

        self.assertEqual(mapping["635053"], "ENG-DR-ART")
        self.assertEqual(mapping["649711"], "ENG-DR-ART")

    def test_store_link_hides_direct_numeric_listing_for_non_card_products(self):
        with patch.object(
            api,
            "load_squarespace_listing_by_sku",
            return_value={"637647": {"sku": "637647", "title": "Prism Energy", "url": "https://www.poke6s.com/shop/p/prism-energy"}},
        ), patch.object(api, "load_tcgplayer_sku_mapping", return_value={}), patch.object(
            api, "_store_link_direct_match_allowed", return_value=False
        ):
            result = api.store_link(product_id=637647)

        self.assertEqual(result, {"listed": False})

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
