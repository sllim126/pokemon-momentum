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

            with patch.object(build_store_price_targets, "JP_SIGNAL_CSV", jp_signal_csv), patch.object(
                build_store_price_targets, "EN_SIGNAL_CSV", en_signal_csv
            ), patch.object(build_store_price_targets, "RULES_CSV", rules_csv), patch.object(
                build_store_price_targets, "SUPPLIER_QUOTES_CSV", supplier_quotes_csv
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

            with patch.object(build_store_price_targets, "JP_SIGNAL_CSV", jp_signal_csv), patch.object(
                build_store_price_targets, "EN_SIGNAL_CSV", en_signal_csv
            ), patch.object(build_store_price_targets, "RULES_CSV", rules_csv), patch.object(
                build_store_price_targets, "SUPPLIER_QUOTES_CSV", supplier_quotes_csv
            ):
                rows, unmatched = build_store_price_targets.build_target_rows(
                    export_csv=export_csv,
                    now=datetime(2026, 5, 17, tzinfo=UTC),
                )

        self.assertEqual(unmatched, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["target_source"], "jp_landed_markup")
        self.assertNotEqual(rows[0]["profit_floor_price"], "")


if __name__ == "__main__":
    unittest.main()
