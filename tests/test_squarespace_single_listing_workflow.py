from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts import build_squarespace_single_listing_drafts
from scripts import create_squarespace_single_listings


class SquarespaceSingleListingWorkflowTests(unittest.TestCase):
    def test_build_drafts_enriches_ready_japanese_single(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            intake_csv = root / "intake.csv"
            market_csv = root / "market.csv"
            export_csv = root / "export.csv"
            created_csv = root / "created.csv"
            signal_csv = root / "signal.csv"
            groups_csv = root / "groups.csv"

            intake_csv.write_text(
                "sku,product_id,subtype,language,condition,quantity,price_override,title_override,notes\n"
                "602682-special-art-rare,602682,Special Art Rare,japanese,Near Mint,2,,,,\n",
                encoding="utf-8",
            )
            market_csv.write_text(
                "sku,market_price,target_price,title\n"
                "602682-special-art-rare,37.97,37.97,Old Store Title\n",
                encoding="utf-8",
            )
            export_csv.write_text(
                "Product ID [Non Editable],Variant ID [Non Editable],SKU\n",
                encoding="utf-8",
            )
            created_csv.write_text("sku,product_id,variant_id\n", encoding="utf-8")
            signal_csv.write_text(
                "productId,groupId,groupName,productName,imageUrl,rarity,number,productClass,productKind,subTypeName,latest_price\n"
                "602682,23999,SV8a: Terastal Festival ex,Roaring Moon ex,https://example.com/roaring-moon.png,Special Art Rare,218/187,pokemon,card,Special Art Rare,37.97\n",
                encoding="utf-8",
            )
            groups_csv.write_text(
                "groupId,name,abbreviation\n"
                "23999,SV8a: Terastal Festival ex,SV8a\n",
                encoding="utf-8",
            )

            original_datasets = build_squarespace_single_listing_drafts.DATASETS
            build_squarespace_single_listing_drafts.DATASETS = {
                "japanese": build_squarespace_single_listing_drafts.DatasetPaths(
                    language="japanese",
                    signal_csv=signal_csv,
                    groups_csv=groups_csv,
                ),
                "english": build_squarespace_single_listing_drafts.DatasetPaths(
                    language="english",
                    signal_csv=signal_csv,
                    groups_csv=groups_csv,
                ),
            }
            try:
                drafts = build_squarespace_single_listing_drafts.build_drafts(
                    intake_csv=intake_csv,
                    market_csv=market_csv,
                    squarespace_export=export_csv,
                    created_csv=created_csv,
                )
            finally:
                build_squarespace_single_listing_drafts.DATASETS = original_datasets

        self.assertEqual(len(drafts), 1)
        row = drafts[0]
        self.assertEqual(row["draft_status"], "ready")
        self.assertEqual(row["sku"], "602682-special-art-rare")
        self.assertEqual(row["target_price"], "37.97")
        self.assertEqual(row["price_source"], "market_prices_latest.target_price")
        self.assertIn("Roaring Moon ex", row["final_title"])
        self.assertIn("/singles/japanese", row["categories"])
        self.assertIn("/singles/rarity/special-art-rare", row["categories"])
        self.assertEqual(row["visibility"], "hidden")
        self.assertEqual(row["stock_quantity"], "2")

    def test_build_drafts_flags_existing_duplicate_sku(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            intake_csv = root / "intake.csv"
            market_csv = root / "market.csv"
            export_csv = root / "export.csv"
            created_csv = root / "created.csv"
            signal_csv = root / "signal.csv"
            groups_csv = root / "groups.csv"

            intake_csv.write_text(
                "sku,product_id,subtype,language,condition,quantity,price_override,title_override,notes\n"
                "610541-poke-ball-pattern,610541,Poke Ball Pattern,english,Near Mint,1,,,,\n",
                encoding="utf-8",
            )
            market_csv.write_text("sku,market_price,target_price\n", encoding="utf-8")
            export_csv.write_text(
                "Product ID [Non Editable],Variant ID [Non Editable],SKU\n"
                "prod-1,var-1,610541-poke-ball-pattern\n",
                encoding="utf-8",
            )
            created_csv.write_text("sku,product_id,variant_id\n", encoding="utf-8")
            signal_csv.write_text(
                "productId,groupId,groupName,productName,imageUrl,rarity,number,productClass,productKind,subTypeName,latest_price\n"
                "610541,24001,SV: Prismatic Evolutions,Cottonee,https://example.com/cottonee.png,Common,007/131,pokemon,card,Poke Ball Pattern,1.49\n",
                encoding="utf-8",
            )
            groups_csv.write_text(
                "groupId,name,abbreviation\n"
                "24001,SV: Prismatic Evolutions,PRE\n",
                encoding="utf-8",
            )

            original_datasets = build_squarespace_single_listing_drafts.DATASETS
            build_squarespace_single_listing_drafts.DATASETS = {
                "english": build_squarespace_single_listing_drafts.DatasetPaths(
                    language="english",
                    signal_csv=signal_csv,
                    groups_csv=groups_csv,
                ),
                "japanese": build_squarespace_single_listing_drafts.DatasetPaths(
                    language="japanese",
                    signal_csv=signal_csv,
                    groups_csv=groups_csv,
                ),
            }
            try:
                drafts = build_squarespace_single_listing_drafts.build_drafts(
                    intake_csv=intake_csv,
                    market_csv=market_csv,
                    squarespace_export=export_csv,
                    created_csv=created_csv,
                )
            finally:
                build_squarespace_single_listing_drafts.DATASETS = original_datasets

        self.assertEqual(drafts[0]["draft_status"], "error")
        self.assertIn("already exists", drafts[0]["errors"])

    def test_build_product_payload_creates_hidden_physical_product(self):
        row = {
            "sku": "602682",
            "final_title": "Roaring Moon ex - 218/187 - SV8a: Terastal Festival ex (SV8a) Japanese",
            "description_html": "<p>Example</p>",
            "tags": "Singles, Singles Intake",
            "url_slug": "roaring-moon-ex-218-187-sv8a-terastal-festival-ex-sv8a-japanese-602682",
            "target_price": "37.97",
            "stock_quantity": "2",
        }

        payload = create_squarespace_single_listings.build_product_payload(
            row,
            store_page_id="store-page-123",
        )

        self.assertEqual(payload["type"], "PHYSICAL")
        self.assertFalse(payload["isVisible"])
        self.assertEqual(payload["storePageId"], "store-page-123")
        self.assertEqual(payload["variants"][0]["sku"], "602682")
        self.assertEqual(payload["variants"][0]["stock"]["quantity"], 2)
        self.assertEqual(payload["variants"][0]["pricing"]["basePrice"]["currency"], "USD")

    def test_build_drafts_defaults_blank_subtype_to_normal_variant(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            intake_csv = root / "intake.csv"
            market_csv = root / "market.csv"
            export_csv = root / "export.csv"
            created_csv = root / "created.csv"
            signal_csv = root / "signal.csv"
            groups_csv = root / "groups.csv"

            intake_csv.write_text(
                "sku,product_id,subtype,language,condition,quantity,price_override,title_override,notes\n"
                "84840,84840,,english,Near Mint,1,,,,\n",
                encoding="utf-8",
            )
            market_csv.write_text("sku,market_price,target_price\n", encoding="utf-8")
            export_csv.write_text("Product ID [Non Editable],Variant ID [Non Editable],SKU\n", encoding="utf-8")
            created_csv.write_text("sku,product_id,variant_id\n", encoding="utf-8")
            signal_csv.write_text(
                "productId,groupId,groupName,productName,imageUrl,rarity,number,productClass,productKind,subTypeName,latest_price\n"
                "84840,200,EX Delta Species,Ditto - 63/113 (Pikachu),https://example.com/ditto.png,Common,063/113,card,card,Normal,75.66\n"
                "84840,200,EX Delta Species,Ditto - 63/113 (Pikachu),https://example.com/ditto-rh.png,Common,063/113,card,card,Reverse Holofoil,249.00\n",
                encoding="utf-8",
            )
            groups_csv.write_text("groupId,name,abbreviation\n200,EX Delta Species,DS\n", encoding="utf-8")

            original_datasets = build_squarespace_single_listing_drafts.DATASETS
            build_squarespace_single_listing_drafts.DATASETS = {
                "english": build_squarespace_single_listing_drafts.DatasetPaths(
                    language="english",
                    signal_csv=signal_csv,
                    groups_csv=groups_csv,
                ),
                "japanese": build_squarespace_single_listing_drafts.DatasetPaths(
                    language="japanese",
                    signal_csv=signal_csv,
                    groups_csv=groups_csv,
                ),
            }
            try:
                drafts = build_squarespace_single_listing_drafts.build_drafts(
                    intake_csv=intake_csv,
                    market_csv=market_csv,
                    squarespace_export=export_csv,
                    created_csv=created_csv,
                )
            finally:
                build_squarespace_single_listing_drafts.DATASETS = original_datasets

        self.assertEqual(drafts[0]["draft_status"], "ready")
        self.assertEqual(drafts[0]["subtype"], "Normal")
        self.assertIn("inferred subtype from sku: Normal", drafts[0]["warnings"])


if __name__ == "__main__":
    unittest.main()
