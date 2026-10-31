"""scripts/build_storefront_market_reference.py: the market prices the storefront shows."""

import csv
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts import build_storefront_market_reference as ref

COLUMNS = ["latest_date", "productId", "productName", "groupName", "number", "productKind",
           "subTypeName", "latest_price", "roc_30d_pct"]


def write_snapshot(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


class StorefrontMarketReferenceTests(unittest.TestCase):
    def test_sealed_and_wanted_cards_with_preferred_printing(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            en, jp = root / "en.csv", root / "jp.csv"
            write_snapshot(en, [
                {"latest_date": "2026-10-29", "productId": "1", "productName": "Box", "groupName": "SV: Black Bolt",
                 "productKind": "sealed", "subTypeName": "Normal", "latest_price": "90.5", "roc_30d_pct": "3.14"},
                # A card the store doesn't list is left out; a wanted one is kept.
                {"latest_date": "2026-10-30", "productId": "2", "productName": "Card", "productKind": "card",
                 "subTypeName": "Holofoil", "latest_price": "5"},
                {"latest_date": "2026-10-30", "productId": "3", "productName": "Wanted card", "productKind": "card",
                 "subTypeName": "Reverse Holofoil", "latest_price": "7", "number": "12/100"},
                {"latest_date": "2026-10-30", "productId": "3", "productName": "Wanted card", "productKind": "card",
                 "subTypeName": "Holofoil", "latest_price": "9", "number": "12/100"},
                # No price: skipped.
                {"latest_date": "2026-10-30", "productId": "4", "productName": "Unpriced", "productKind": "sealed",
                 "subTypeName": "Normal", "latest_price": ""},
            ])
            write_snapshot(jp, [
                {"latest_date": "2026-10-30", "productId": "10", "productName": "JP Box", "groupName": "M5: Abyss Eye",
                 "productKind": "sealed", "subTypeName": "Normal", "latest_price": "70", "roc_30d_pct": ""},
            ])
            data = ref.build(en, jp, wanted={"3"})

        products = data["products"]
        self.assertEqual(set(products), {"1", "3", "10"})
        self.assertEqual(data["latest_date"], "2026-10-30")
        self.assertEqual(products["1"]["price"], 90.5)
        self.assertEqual(products["1"]["roc_30d_pct"], 3.1)
        self.assertEqual(products["1"]["url"], "https://market.poke6s.com/?kind=sealed&product=1%7C%7CNormal")
        # Holofoil is preferred over Reverse Holofoil, whichever comes first in the file.
        self.assertEqual((products["3"]["sub_type"], products["3"]["price"]), ("Holofoil", 9.0))
        self.assertEqual(products["3"]["url"], "https://market.poke6s.com/?product=3%7C%7CHolofoil")
        self.assertEqual(products["10"]["market"], "JP")
        self.assertIsNone(products["10"]["roc_30d_pct"])
        self.assertIn("market=JP", products["10"]["url"])

    def test_store_product_ids_reads_pipe_lists(self):
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "intake.csv"
            path.write_text("sku,tcgplayer_product_id\nA,11|12\nB,\n", encoding="utf-8")
            self.assertEqual(ref.store_product_ids((path,)), {"11", "12"})

    def test_write_is_atomic_json(self):
        with TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "sub" / "market_reference.json"
            ref.write({"products": {"1": {"price": 1.0}}}, out)
            self.assertEqual(json.loads(out.read_text())["products"]["1"]["price"], 1.0)
            self.assertFalse(out.with_suffix(".json.tmp").exists())


if __name__ == "__main__":
    unittest.main()
