from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd
from fastapi import HTTPException

from scripts.dashboards import query_support


class FakeCursor:
    def __init__(self, description, rows):
        self.description = description
        self._rows = rows

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class FakeConnection:
    def __init__(self, description=None, rows=None, tables=None):
        self.description = description or [("value",)]
        self.rows = rows or []
        self.tables = tables or []
        self.closed = False

    def execute(self, sql, params=None):
        if sql == "SHOW TABLES":
            return FakeCursor([("name",)], [(table,) for table in self.tables])
        return FakeCursor(self.description, self.rows)

    def close(self):
        self.closed = True


class QuerySupportTests(unittest.TestCase):
    def test_category_config_proxy_returns_real_config(self):
        category = query_support.category_config(85)

        self.assertEqual(category.slug, "pokemon_jp")
        self.assertEqual(category.label, "Pokemon Japanese")

    def test_first_existing_path_returns_first_match_or_none(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            missing = root / "missing.csv"
            present = root / "present.csv"
            later = root / "later.csv"
            present.write_text("a\n", encoding="utf-8")
            later.write_text("b\n", encoding="utf-8")

            self.assertEqual(query_support.first_existing_path(missing, present, later), present)
            self.assertIsNone(query_support.first_existing_path(missing))

    def test_prefer_csv_source_requires_existing_file(self):
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            missing = tmp_path / "missing.csv"
            present = tmp_path / "present.csv"
            present.write_text("ok\n", encoding="utf-8")

            self.assertFalse(query_support.prefer_csv_source(None))
            self.assertFalse(query_support.prefer_csv_source(missing))
            self.assertTrue(query_support.prefer_csv_source(present))

    def test_products_from_prefers_database_table_when_csv_not_preferred(self):
        with patch.object(query_support, "db_has_table", return_value=True), patch.object(
            query_support, "prefer_csv_source", return_value=False
        ), patch.object(query_support, "first_existing_path", return_value=None):
            self.assertEqual(query_support.products_from(3), "pokemon_products")

    def test_groups_from_prefers_database_table_when_csv_not_preferred(self):
        with patch.object(query_support, "db_has_table", return_value=True), patch.object(
            query_support, "prefer_csv_source", return_value=False
        ), patch.object(query_support, "first_existing_path", return_value=None):
            self.assertEqual(query_support.groups_from(85), "pokemon_jp_groups")

    def test_products_from_falls_back_to_csv_when_available(self):
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "pokemon_products.csv"
            csv_path.write_text("productId,name\n1,Pikachu\n", encoding="utf-8")
            with patch.object(query_support, "db_has_table", return_value=False), patch.object(
                query_support, "first_existing_path", return_value=csv_path
            ):
                self.assertEqual(query_support.products_from(3), f"read_csv_auto('{csv_path}')")

    def test_groups_from_falls_back_to_csv_when_available(self):
        with TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "pokemon_jp_groups.csv"
            csv_path.write_text("groupId,name\n1,Test Set\n", encoding="utf-8")
            with patch.object(query_support, "db_has_table", return_value=False), patch.object(
                query_support, "first_existing_path", return_value=csv_path
            ):
                self.assertEqual(query_support.groups_from(85), f"read_csv_auto('{csv_path}')")

    def test_products_from_raises_when_no_source_exists(self):
        with patch.object(query_support, "db_has_table", return_value=False), patch.object(
            query_support, "first_existing_path", return_value=None
        ):
            with self.assertRaises(HTTPException) as ctx:
                query_support.products_from(3)

        self.assertEqual(ctx.exception.status_code, 500)
        self.assertIn("pokemon_products metadata not found", ctx.exception.detail)

    def test_snapshot_source_prefers_table_then_csv_then_error(self):
        with patch.object(query_support, "db_has_table", return_value=True):
            self.assertEqual(query_support.product_signal_from(85), "pokemon_jp_product_signal_snapshot")
            self.assertEqual(query_support.group_signal_from(3), "pokemon_group_signal_snapshot")
            self.assertEqual(query_support.sparkline_snapshot_from(3), "pokemon_sparkline_snapshot")
            self.assertEqual(query_support.health_snapshot_from(3), "pokemon_health_snapshot")
            self.assertEqual(query_support.series_snapshot_from(3), "pokemon_series_snapshot")

        with TemporaryDirectory() as tmpdir:
            extracted = Path(tmpdir)
            csv_path = extracted / "pokemon_product_signal_snapshot.csv"
            csv_path.write_text("productId\n1\n", encoding="utf-8")
            group_csv = extracted / "pokemon_group_signal_snapshot.csv"
            spark_csv = extracted / "pokemon_sparkline_snapshot.csv"
            health_csv = extracted / "pokemon_health_snapshot.csv"
            series_csv = extracted / "pokemon_series_snapshot.csv"
            group_csv.write_text("groupId\n1\n", encoding="utf-8")
            spark_csv.write_text("productId\n1\n", encoding="utf-8")
            health_csv.write_text("rows,latest\n1,2026-03-30\n", encoding="utf-8")
            series_csv.write_text("productId,date\n1,2026-03-30\n", encoding="utf-8")
            with patch.object(query_support, "db_has_table", return_value=False), patch.object(
                query_support, "EXTRACTED_DIR", extracted
            ):
                self.assertEqual(
                    query_support.product_signal_from(3),
                    f"read_csv_auto('{csv_path}')",
                )
                self.assertEqual(query_support.group_signal_from(3), f"read_csv_auto('{group_csv}')")
                self.assertEqual(query_support.sparkline_snapshot_from(3), f"read_csv_auto('{spark_csv}')")
                self.assertEqual(query_support.health_snapshot_from(3), f"read_csv_auto('{health_csv}')")
                self.assertEqual(query_support.series_snapshot_from(3), f"read_csv_auto('{series_csv}')")

        with patch.object(query_support, "db_has_table", return_value=False), patch.object(
            query_support, "EXTRACTED_DIR", Path("/tmp/definitely-missing")
        ):
            with self.assertRaises(HTTPException):
                query_support.series_snapshot_from(3)

    def test_has_parquet_checks_globbed_files(self):
        with TemporaryDirectory() as tmpdir:
            parquet_root = Path(tmpdir)
            (parquet_root / "nested").mkdir()
            (parquet_root / "nested" / "prices.parquet").write_text("placeholder", encoding="utf-8")
            with patch.object(query_support, "PARQUET_ROOT", parquet_root):
                self.assertTrue(query_support.has_parquet())

        with TemporaryDirectory() as tmpdir:
            with patch.object(query_support, "PARQUET_ROOT", Path(tmpdir)):
                self.assertFalse(query_support.has_parquet())

    def test_parquet_has_category_uses_duckdb_query(self):
        fake_con = FakeConnection(rows=[(1,)])
        with patch.object(query_support, "has_parquet", return_value=True), patch.object(
            query_support.duckdb, "connect", return_value=fake_con
        ):
            self.assertTrue(query_support.parquet_has_category(85))
        self.assertTrue(fake_con.closed)

        fake_con = FakeConnection(rows=[])
        with patch.object(query_support, "has_parquet", return_value=True), patch.object(
            query_support.duckdb, "connect", return_value=fake_con
        ):
            self.assertFalse(query_support.parquet_has_category(3))

    def test_prices_from_prefers_parquet_when_available(self):
        with patch.object(query_support, "has_parquet", return_value=True), patch.object(
            query_support, "parquet_has_category", return_value=True
        ):
            self.assertIn("read_parquet", query_support.prices_from(85))

        with patch.object(query_support, "has_parquet", return_value=False):
            self.assertEqual(query_support.prices_from(3), "pokemon_prices")

    def test_db_has_table_checks_show_tables(self):
        fake_con = FakeConnection(tables=["pokemon_prices", "pokemon_groups"])
        with patch.object(query_support, "DB_PATH", Path("/tmp/fake.duckdb")), patch.object(
            Path, "exists", return_value=True
        ), patch.object(query_support.duckdb, "connect", return_value=fake_con):
            self.assertTrue(query_support.db_has_table("pokemon_groups"))
            self.assertFalse(query_support.db_has_table("missing_table"))
        self.assertTrue(fake_con.closed)

    def test_get_con_uses_disk_db_when_present_or_memory_otherwise(self):
        fake_disk = FakeConnection()
        fake_memory = FakeConnection()
        with patch.object(query_support, "DB_PATH", Path("/tmp/fake.duckdb")), patch.object(
            Path, "exists", return_value=True
        ), patch.object(query_support.duckdb, "connect", return_value=fake_disk):
            self.assertIs(query_support.get_con(), fake_disk)

        with patch.object(query_support, "DB_PATH", Path("/tmp/fake.duckdb")), patch.object(
            Path, "exists", return_value=False
        ), patch.object(query_support.duckdb, "connect", return_value=fake_memory):
            self.assertIs(query_support.get_con(), fake_memory)

    def test_q_returns_columns_and_rows_and_closes_connection(self):
        fake_con = FakeConnection(description=[("alpha",), ("beta",)], rows=[(1, 2), (3, 4)])
        with patch.object(query_support, "get_con", return_value=fake_con):
            cols, rows = query_support.q("SELECT 1, 2")

        self.assertEqual(cols, ["alpha", "beta"])
        self.assertEqual(rows, [(1, 2), (3, 4)])
        self.assertTrue(fake_con.closed)

    def test_to_jsonable_normalizes_common_dashboard_values(self):
        ts = pd.Timestamp("2026-03-30T12:00:00Z")
        payload = {
            "timestamp": ts,
            "nan_value": np.nan,
            "tuple_value": (np.int64(5), pd.Timestamp("2026-03-29")),
            "list_value": [np.float64(1.5), {"nested": np.nan}],
        }

        normalized = query_support.to_jsonable(payload)

        self.assertEqual(normalized["timestamp"], ts.isoformat())
        self.assertIsNone(normalized["nan_value"])
        self.assertEqual(normalized["tuple_value"][0], 5)
        self.assertEqual(normalized["tuple_value"][1], "2026-03-29T00:00:00")
        self.assertEqual(normalized["list_value"][0], 1.5)
        self.assertIsNone(normalized["list_value"][1]["nested"])

    def test_build_metadata_cte_optionally_includes_classification_fields(self):
        plain_sql = query_support.build_metadata_cte(3, include_classification=False)
        classified_sql = query_support.build_metadata_cte(3, include_classification=True)

        self.assertIn("COALESCE(g.name, 'Unknown Group') AS groupName", plain_sql)
        self.assertNotIn("AS productClass", plain_sql)
        self.assertIn("AS productClass", classified_sql)
        self.assertIn("AS productKind", classified_sql)

    def test_build_premium_rarity_filter_covers_expected_rarities(self):
        sql = query_support.build_premium_rarity_filter("rarity_col")

        self.assertIn("%double rare%", sql)
        self.assertIn("%special illustration rare%", sql)
        self.assertIn("%secret rare%", sql)

    def test_build_set_basket_filter_supports_all_and_targeted_filters(self):
        self.assertEqual(query_support.build_set_basket_filter([]), "1=1")
        self.assertEqual(query_support.build_set_basket_filter(["all"]), "1=1")

        sql = query_support.build_set_basket_filter(
            ["reverse_holo", "stamped", "illustration_rare"],
            rarity_column="rarity_col",
            subtype_column="subtype_col",
            product_name_column="name_col",
        )
        self.assertIn("lower(COALESCE(subtype_col, '')) LIKE '%reverse holo%'", sql)
        self.assertIn("lower(COALESCE(name_col, '')) LIKE '%stamp%'", sql)
        self.assertIn("lower(COALESCE(rarity_col, '')) LIKE '%illustration rare%'", sql)

    def test_build_generation_case_prioritizes_mega_before_sv(self):
        sql = query_support.build_generation_case()

        self.assertIn("24459 THEN 'MEG'", sql)
        self.assertIn("24499 THEN 'MEG'", sql)
        self.assertIn("M1L:%", sql)
        self.assertIn("M1S:%", sql)
        self.assertIn("START DECK 100 BATTLE COLLECTION", sql)
        self.assertLess(sql.index("THEN 'MEG'"), sql.index("THEN 'SV'"))
        self.assertIn("ELSE 'Legacy'", sql)
