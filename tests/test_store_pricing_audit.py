from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from scripts.utilities import audit_store_pricing_coverage


class StorePricingAuditTests(unittest.TestCase):
    def test_build_audit_rows_classifies_expected_and_missing_items(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            export_csv = root / "export.csv"
            rules_csv = root / "rules.csv"
            market_csv = root / "market.csv"
            expectations_csv = root / "expectations.csv"

            export_csv.write_text(
                "SKU,Title,Price,Stock,Categories,Visible\n"
                "AUTO-1,Automated Item,10.00,5,/sealed,Yes\n"
                "MANUAL-1,Manual Item,20.00,2,/other,Yes\n"
                "HOLD-1,Hold Item,30.00,1,/booster-box/japanese,Yes\n"
                "MISS-1,Missing Item,40.00,3,/sealed,Yes\n",
                encoding="utf-8",
            )
            rules_csv.write_text(
                "sku,pricing_mode\n"
                "AUTO-1,market\n"
                "MANUAL-1,manual\n",
                encoding="utf-8",
            )
            market_csv.write_text(
                "sku,market_price,target_price\n"
                "AUTO-1,10.00,10.00\n",
                encoding="utf-8",
            )
            expectations_csv.write_text(
                "sku,classification,note\n"
                "HOLD-1,hold_expected,Waiting for release\n",
                encoding="utf-8",
            )

            with patch.object(audit_store_pricing_coverage, "EXPORT_CSV", export_csv), patch.object(
                audit_store_pricing_coverage, "RULES_CSV", rules_csv
            ), patch.object(audit_store_pricing_coverage, "MARKET_CSV", market_csv), patch.object(
                audit_store_pricing_coverage, "EXPECTATIONS_CSV", expectations_csv
            ):
                rows = audit_store_pricing_coverage.build_audit_rows()

        by_sku = {row["sku"]: row for row in rows}
        self.assertEqual(by_sku["AUTO-1"]["classification"], "automated")
        self.assertEqual(by_sku["MANUAL-1"]["classification"], "manual_expected")
        self.assertEqual(by_sku["HOLD-1"]["classification"], "hold_expected")
        self.assertEqual(by_sku["MISS-1"]["classification"], "missing_unexpected")


if __name__ == "__main__":
    unittest.main()
