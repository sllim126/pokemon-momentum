from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from scripts import build_store_price_targets


class BuildStorePriceTargetsTests(unittest.TestCase):
    def test_stale_jp_quote_uses_market_plus_25_pct(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            export_csv = root / "export.csv"
            jp_signal_csv = root / "jp_signal.csv"
            en_signal_csv = root / "en_signal.csv"
            rules_csv = root / "rules.csv"
            supplier_quotes_csv = root / "supplier_quotes.csv"
            created_csv = root / "created.csv"

            export_csv.write_text(
                "SKU,Title\n"
                "JP-TF-BB,Pokemon Terastal Festival Booster Box - Japanese\n",
                encoding="utf-8",
            )
            jp_signal_csv.write_text(
                "productId,productName,latest_price\n"
                "123,Terastal Festival Booster Box,130.00\n",
                encoding="utf-8",
            )
            en_signal_csv.write_text("productId,productName,latest_price\n", encoding="utf-8")
            rules_csv.write_text(
                "sku,market_source,lookup_type,lookup_value,pricing_mode,min_price,note\n"
                "JP-TF-BB,jp,name,Terastal Festival Booster Box,market_minus_5_pct_99,,booster box\n",
                encoding="utf-8",
            )
            supplier_quotes_csv.write_text(
                "quote_id,quote_date,supplier_name,source_name,source_type,item_name_raw,sku,cost_jpy,image_name,notes\n"
                "20260501T000000Z,2026-05-01,Test Supplier,Sheet,screenshot,Terastal Festival,JP-TF-BB,17000,test.png,\n",
                encoding="utf-8",
            )
            created_csv.write_text(
                "created_at,sku,product_id,variant_id,canonical_product_id,subtype,title,target_price,quantity,language,condition,url_slug,squarespace_url,store_page_id,visibility,tags,categories,image_url,notes\n",
                encoding="utf-8",
            )

            with patch.object(build_store_price_targets, "JP_SIGNAL_CSV", jp_signal_csv), patch.object(
                build_store_price_targets, "EN_SIGNAL_CSV", en_signal_csv
            ), patch.object(build_store_price_targets, "RULES_CSV", rules_csv), patch.object(
                build_store_price_targets, "SUPPLIER_QUOTES_CSV", supplier_quotes_csv
            ), patch.object(
                build_store_price_targets, "CREATED_SINGLE_LISTINGS_CSV", created_csv
            ):
                rows, unmatched = build_store_price_targets.build_target_rows(
                    export_csv=export_csv,
                    now=datetime(2026, 5, 17, tzinfo=UTC),
                )

        self.assertEqual(unmatched, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sku"], "JP-TF-BB")
        self.assertEqual(rows[0]["market_price"], "130.00")
        self.assertEqual(rows[0]["target_price"], "162.50")
        self.assertEqual(rows[0]["target_source"], "jp_market_plus_25_stale_quote")
        self.assertEqual(rows[0]["profit_floor_price"], "")

    def test_fresh_jp_quote_keeps_landed_markup_flow(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            export_csv = root / "export.csv"
            jp_signal_csv = root / "jp_signal.csv"
            en_signal_csv = root / "en_signal.csv"
            rules_csv = root / "rules.csv"
            supplier_quotes_csv = root / "supplier_quotes.csv"
            created_csv = root / "created.csv"

            export_csv.write_text(
                "SKU,Title\n"
                "JP-TF-BB,Pokemon Terastal Festival Booster Box - Japanese\n",
                encoding="utf-8",
            )
            jp_signal_csv.write_text(
                "productId,productName,latest_price\n"
                "123,Terastal Festival Booster Box,130.00\n",
                encoding="utf-8",
            )
            en_signal_csv.write_text("productId,productName,latest_price\n", encoding="utf-8")
            rules_csv.write_text(
                "sku,market_source,lookup_type,lookup_value,pricing_mode,min_price,note\n"
                "JP-TF-BB,jp,name,Terastal Festival Booster Box,market_minus_5_pct_99,,booster box\n",
                encoding="utf-8",
            )
            supplier_quotes_csv.write_text(
                "quote_id,quote_date,supplier_name,source_name,source_type,item_name_raw,sku,cost_jpy,image_name,notes\n"
                "20260515T000000Z,2026-05-15,Test Supplier,Sheet,screenshot,Terastal Festival,JP-TF-BB,17000,test.png,\n",
                encoding="utf-8",
            )
            created_csv.write_text(
                "created_at,sku,product_id,variant_id,canonical_product_id,subtype,title,target_price,quantity,language,condition,url_slug,squarespace_url,store_page_id,visibility,tags,categories,image_url,notes\n",
                encoding="utf-8",
            )

            with patch.object(build_store_price_targets, "JP_SIGNAL_CSV", jp_signal_csv), patch.object(
                build_store_price_targets, "EN_SIGNAL_CSV", en_signal_csv
            ), patch.object(build_store_price_targets, "RULES_CSV", rules_csv), patch.object(
                build_store_price_targets, "SUPPLIER_QUOTES_CSV", supplier_quotes_csv
            ), patch.object(
                build_store_price_targets, "CREATED_SINGLE_LISTINGS_CSV", created_csv
            ):
                rows, unmatched = build_store_price_targets.build_target_rows(
                    export_csv=export_csv,
                    now=datetime(2026, 5, 17, tzinfo=UTC),
                )

        self.assertEqual(unmatched, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["target_source"], "jp_landed_markup")
        self.assertNotEqual(rows[0]["profit_floor_price"], "")

    def test_created_variant_single_rows_are_appended_for_daily_sync(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            export_csv = root / "export.csv"
            jp_signal_csv = root / "jp_signal.csv"
            en_signal_csv = root / "en_signal.csv"
            rules_csv = root / "rules.csv"
            supplier_quotes_csv = root / "supplier_quotes.csv"
            created_csv = root / "created.csv"

            export_csv.write_text("SKU,Title\n84840-reverse-holofoil,Ditto RH\n", encoding="utf-8")
            jp_signal_csv.write_text(
                "productId,productName,subTypeName,latest_price\n",
                encoding="utf-8",
            )
            en_signal_csv.write_text(
                "productId,productName,groupName,subTypeName,latest_price\n"
                "84840,Ditto - 63/113 (Pikachu),EX Delta Species,Reverse Holofoil,249.00\n",
                encoding="utf-8",
            )
            rules_csv.write_text(
                "sku,market_source,lookup_type,lookup_value,pricing_mode,min_price,note\n",
                encoding="utf-8",
            )
            supplier_quotes_csv.write_text(
                "quote_id,quote_date,supplier_name,source_name,source_type,item_name_raw,sku,cost_jpy,image_name,notes\n",
                encoding="utf-8",
            )
            created_csv.write_text(
                "created_at,sku,product_id,variant_id,canonical_product_id,subtype,title,target_price,quantity,language,condition,url_slug,squarespace_url,store_page_id,visibility,tags,categories,image_url,notes\n"
                "2026-07-29T00:00:00Z,84840-reverse-holofoil,store-prod-1,var-1,84840,Reverse Holofoil,Ditto RH,249.00,1,english,Near Mint,ditto-rh,,,,,,,\n",
                encoding="utf-8",
            )

            with patch.object(build_store_price_targets, "JP_SIGNAL_CSV", jp_signal_csv), patch.object(
                build_store_price_targets, "EN_SIGNAL_CSV", en_signal_csv
            ), patch.object(build_store_price_targets, "RULES_CSV", rules_csv), patch.object(
                build_store_price_targets, "SUPPLIER_QUOTES_CSV", supplier_quotes_csv
            ), patch.object(build_store_price_targets, "CREATED_SINGLE_LISTINGS_CSV", created_csv):
                rows, unmatched = build_store_price_targets.build_target_rows(
                    export_csv=export_csv,
                    now=datetime(2026, 7, 29, tzinfo=UTC),
                )

        self.assertEqual(unmatched, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sku"], "84840-reverse-holofoil")
        self.assertEqual(rows[0]["market_price"], "249.00")
        self.assertEqual(rows[0]["target_price"], "249.00")
        self.assertEqual(rows[0]["market_source"], "en")


if __name__ == "__main__":
    unittest.main()
