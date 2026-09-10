from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from scripts import build_squarespace_single_listing_drafts
from scripts import build_squarespace_sealed_listing_drafts
from scripts import build_store_price_targets
from scripts import card_image_cache
from scripts import convert_collection_csv_to_single_listing_intake
from scripts import create_squarespace_single_listings
from scripts import create_squarespace_sealed_listings
from scripts import scrydex_images


class SquarespaceSingleListingWorkflowTests(unittest.TestCase):
    def test_build_scrydex_card_image_url_handles_prefixed_and_numeric_numbers(self):
        self.assertEqual(
            scrydex_images.build_scrydex_card_image_url(
                "SWSH11: Lost Origin Trainer Gallery",
                "TG05/TG30",
            ),
            "https://images.scrydex.com/pokemon/swsh11tg-TG05/medium",
        )
        self.assertEqual(
            scrydex_images.build_scrydex_card_image_url(
                "EX Delta Species",
                "063/113",
            ),
            "https://images.scrydex.com/pokemon/ex11-63/medium",
        )

    def test_resolve_image_cache_metadata_uses_language_and_sku(self):
        with TemporaryDirectory() as tmpdir:
            metadata = create_squarespace_single_listings.resolve_image_cache_metadata(
                {
                    "sku": "84840-reverse-holofoil",
                    "language": "english",
                    "image_url": "https://tcgplayer-cdn.tcgplayer.com/product/84840_200w.jpg",
                },
                Path(tmpdir),
            )

        self.assertTrue(metadata["image_cache_path"].endswith("english/84840-reverse-holofoil.jpg"))
        self.assertEqual(
            metadata["image_public_url"],
            "/images/cards/english/84840-reverse-holofoil.jpg",
        )

    @patch("scripts.card_image_cache.requests.get")
    def test_cache_card_image_downloads_expected_file(self, mock_get):
        class FakeResponse:
            headers = {"Content-Type": "image/png"}

            def raise_for_status(self):
                return None

            def iter_content(self, chunk_size=8192):
                yield b"png-bytes"

        mock_get.return_value = FakeResponse()

        with TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "english" / "84840.jpg"
            resolved_path, downloaded = card_image_cache.cache_card_image(
                image_url="https://example.com/card.png",
                image_path=target,
                timeout=5,
            )

            self.assertTrue(downloaded)
            self.assertEqual(resolved_path.suffix, ".png")
            self.assertEqual(resolved_path.read_bytes(), b"png-bytes")
            self.assertFalse(target.exists())

    def test_square_pad_image_adds_horizontal_canvas_without_cropping(self):
        from PIL import Image

        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "card.png"
            Image.new("RGBA", (20, 40), (255, 0, 0, 255)).save(path)
            changed = card_image_cache.square_pad_image(path)
            with Image.open(path) as padded:
                self.assertTrue(changed)
                self.assertEqual(padded.size, (40, 40))
                self.assertEqual(padded.getpixel((20, 20)), (255, 0, 0, 255))
                self.assertEqual(padded.getpixel((2, 20)), (255, 255, 255, 255))

    def test_cache_card_image_square_pads_existing_file(self):
        from PIL import Image

        with TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "english" / "84840.png"
            target.parent.mkdir(parents=True, exist_ok=True)
            Image.new("RGBA", (30, 50), (0, 0, 255, 255)).save(target)

            resolved_path, downloaded = card_image_cache.cache_card_image(
                image_url="https://example.com/card.png",
                image_path=target,
                timeout=5,
                square_pad=True,
            )
            with Image.open(resolved_path) as padded:
                self.assertFalse(downloaded)
                self.assertEqual(padded.size, (50, 50))

    def test_cache_script_uses_scrydex_source_url(self):
        row = {
            "set_name": "SWSH: Crown Zenith: Galarian Gallery",
            "card_number": "GG50/GG70",
        }
        self.assertEqual(
            create_squarespace_single_listings.resolve_image_cache_metadata(
                {
                    "sku": "478077-holofoil",
                    "language": "english",
                    "image_url": scrydex_images.build_scrydex_card_image_url(
                        row["set_name"],
                        row["card_number"],
                    ),
                },
                Path("/tmp/card-cache"),
            )["image_cache_path"],
            "/tmp/card-cache/english/478077-holofoil.jpg",
        )

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
        self.assertIn("Japanese", row["final_title"])
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

    def test_build_sealed_drafts_averages_multi_product_ids(self):
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            intake_csv = root / "sealed_intake.csv"
            signal_csv = root / "signal.csv"
            export_csv = root / "export.csv"
            created_csv = root / "created.csv"

            intake_csv.write_text(
                "sku,product_name,language,product_type,quantity,price_override,title_override,tcgplayer_product_id,notes,lookup_status\n"
                "ENG-G25-TIN,Poke Ball Tin G25,english,tin,4,,,688965|688968,random design,ready_for_pricing_mapping\n",
                encoding="utf-8",
            )
            signal_csv.write_text(
                "productId,groupName,productName,imageUrl,latest_price,productKind\n"
                "688965,Miscellaneous Cards & Products,Pokemon - Poke Ball Tin - Ultra Ball (Q4 2025),https://example.com/ultra.jpg,27.12,sealed\n"
                "688968,Miscellaneous Cards & Products,Pokemon - Poke Ball Tin - Repeat Ball (Q4 2025),https://example.com/repeat.jpg,22.78,sealed\n",
                encoding="utf-8",
            )
            export_csv.write_text("Product ID [Non Editable],Variant ID [Non Editable],SKU\n", encoding="utf-8")
            created_csv.write_text("sku,product_id,variant_id\n", encoding="utf-8")

            drafts = build_squarespace_sealed_listing_drafts.build_drafts(
                intake_csv=intake_csv,
                signal_csv=signal_csv,
                squarespace_export=export_csv,
                created_csv=created_csv,
            )

        self.assertEqual(len(drafts), 1)
        row = drafts[0]
        self.assertEqual(row["draft_status"], "ready")
        self.assertEqual(row["target_price"], "24.95")
        self.assertEqual(row["price_source"], "product_signal_snapshot.average_latest_price")
        self.assertIn("market title differs from intake title", row["warnings"])
        self.assertEqual(row["visibility"], "hidden")
        self.assertEqual(row["stock_quantity"], "4")

    def test_build_sealed_payload_creates_hidden_physical_product(self):
        row = {
            "sku": "ENG-PO-ETB",
            "final_title": "Perfect Order Elite Trainer Box - English",
            "description_html": "<p>Example</p>",
            "tags": "Sealed, English, Elite Trainer Box",
            "url_slug": "perfect-order-elite-trainer-box-english-eng-po-etb",
            "target_price": "72.13",
            "stock_quantity": "3",
        }
        payload = create_squarespace_sealed_listings.build_product_payload(row, store_page_id="store-page-1")
        self.assertEqual(payload["type"], "PHYSICAL")
        self.assertFalse(payload["isVisible"])
        self.assertEqual(payload["storePageId"], "store-page-1")
        self.assertEqual(payload["variants"][0]["sku"], "ENG-PO-ETB")
        self.assertEqual(payload["variants"][0]["pricing"]["basePrice"]["value"], "72.13")
        self.assertEqual(payload["variants"][0]["stock"]["quantity"], 3)

    def test_build_target_rows_supports_multi_product_id_rules(self):
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
                "ENG-G25-TIN,Poke Ball Tin G25 - English\n",
                encoding="utf-8",
            )
            jp_signal_csv.write_text(
                "productId,productName,latest_price\n",
                encoding="utf-8",
            )
            en_signal_csv.write_text(
                "productId,productName,latest_price\n"
                "688965,Pokemon - Poke Ball Tin - Ultra Ball (Q4 2025),27.12\n"
                "688968,Pokemon - Poke Ball Tin - Repeat Ball (Q4 2025),22.78\n",
                encoding="utf-8",
            )
            rules_csv.write_text(
                "sku,market_source,lookup_type,lookup_value,pricing_mode,min_price,note\n"
                "ENG-G25-TIN,en,product_id,688965|688968,market_minus_5_pct_99,,Created by sealed Squarespace listing workflow\n",
                encoding="utf-8",
            )
            supplier_quotes_csv.write_text("sku,cost_jpy\n", encoding="utf-8")
            created_csv.write_text("sku,product_id,variant_id\n", encoding="utf-8")

            with patch.object(build_store_price_targets, "JP_SIGNAL_CSV", jp_signal_csv), \
                 patch.object(build_store_price_targets, "EN_SIGNAL_CSV", en_signal_csv), \
                 patch.object(build_store_price_targets, "RULES_CSV", rules_csv), \
                 patch.object(build_store_price_targets, "SUPPLIER_QUOTES_CSV", supplier_quotes_csv), \
                 patch.object(build_store_price_targets, "CREATED_SINGLE_LISTINGS_CSV", created_csv):
                output_rows, unmatched = build_store_price_targets.build_target_rows(export_csv=export_csv)

        self.assertEqual(unmatched, [])
        self.assertEqual(len(output_rows), 1)
        self.assertEqual(output_rows[0]["sku"], "ENG-G25-TIN")
        self.assertEqual(output_rows[0]["market_price"], "24.95")
        self.assertEqual(output_rows[0]["market_title"], "Pokemon - Poke Ball Tin - Ultra Ball (Q4 2025)")

    def test_build_product_payload_creates_hidden_physical_product(self):
        row = {
            "sku": "602682",
            "final_title": "Roaring Moon ex - 218/187 - SV8a: Terastal Festival ex (SV8a) - Japanese",
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

    def test_build_followup_rows_defaults_manual_cleanup_statuses(self):
        rows = create_squarespace_single_listings.build_followup_rows(
            [
                {
                    "created_at": "2026-08-01T00:00:00Z",
                    "sku": "602682",
                    "title": "Roaring Moon ex",
                    "squarespace_url": "https://poke6s.com/shop/p/roaring-moon",
                    "product_id": "prod-1",
                    "variant_id": "var-1",
                    "target_price": "37.97",
                    "quantity": "2",
                    "language": "japanese",
                    "condition": "Near Mint",
                    "visibility": "hidden",
                    "categories": "/singles/japanese, /singles/rarity/special-art-rare",
                    "tags": "Singles, Singles Intake",
                }
            ]
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["sku"], "602682")
        self.assertEqual(row["image_review_status"], "pending")
        self.assertEqual(row["tax_code_status"], "pending")
        self.assertEqual(row["categories_status"], "pending")
        self.assertEqual(row["fulfillment_status"], "pending")
        self.assertIn("/singles/japanese", row["categories_suggested"])
        self.assertIn("Singles", row["tags_suggested"])

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
        self.assertIn("English", drafts[0]["final_title"])

    def test_convert_collection_rows_maps_and_aggregates_duplicates(self):
        datasets = {
            "english": convert_collection_csv_to_single_listing_intake.DatasetPaths(
                language="english",
                signal_csv=Path("/tmp/english-signal.csv"),
                groups_csv=Path("/tmp/english-groups.csv"),
            )
        }
        source_rows = [
            {
                "Position": "1",
                "Name": "Darkrai VSTAR",
                "Set Name": "Crown Zenith Galarian Gallery",
                "Card Number": "GG50/GG70",
                "Variant": "Holofoil",
                "Language": "English",
                "Notes": "binder",
            },
            {
                "Position": "2",
                "Name": "Darkrai VSTAR",
                "Set Name": "Crown Zenith Galarian Gallery",
                "Card Number": "GG50/GG70",
                "Variant": "Holofoil",
                "Language": "English",
                "Notes": "binder",
            },
            {
                "Position": "3",
                "Name": "Ditto",
                "Set Name": "Delta Species",
                "Card Number": "063/113",
                "Variant": "Reverse Holofoil",
                "Language": "English",
                "Notes": "reverse",
            },
        ]

        with patch.object(
            convert_collection_csv_to_single_listing_intake,
            "load_signal_rows",
            side_effect=[
                [
                    {
                        "productId": "478077",
                        "productName": "Darkrai VSTAR",
                        "groupName": "SWSH: Crown Zenith: Galarian Gallery",
                        "number": "GG50/GG70",
                        "subTypeName": "Holofoil",
                    },
                    {
                        "productId": "84840",
                        "productName": "Ditto - 63/113 (Pikachu)",
                        "groupName": "EX Delta Species",
                        "number": "63/113",
                        "subTypeName": "Reverse Holofoil",
                    },
                ]
            ],
        ):
            intake_rows, errors = convert_collection_csv_to_single_listing_intake.convert_collection_rows(
                source_rows,
                datasets=datasets,
            )

        self.assertEqual(errors, [])
        self.assertEqual(len(intake_rows), 2)
        self.assertEqual(intake_rows[0]["sku"], "478077-holofoil")
        self.assertEqual(intake_rows[0]["quantity"], "2")
        self.assertEqual(intake_rows[0]["notes"], "binder")
        self.assertEqual(intake_rows[1]["sku"], "84840-reverse-holofoil")
        self.assertEqual(intake_rows[1]["product_id"], "84840")
        self.assertEqual(intake_rows[1]["subtype"], "Reverse Holofoil")

    def test_convert_collection_rows_returns_clear_error_for_unmatched_card(self):
        datasets = {
            "english": convert_collection_csv_to_single_listing_intake.DatasetPaths(
                language="english",
                signal_csv=Path("/tmp/english-signal.csv"),
                groups_csv=Path("/tmp/english-groups.csv"),
            )
        }
        with patch.object(
            convert_collection_csv_to_single_listing_intake,
            "load_signal_rows",
            return_value=[],
        ):
            intake_rows, errors = convert_collection_csv_to_single_listing_intake.convert_collection_rows(
                [
                    {
                        "Position": "1",
                        "Name": "Missing Card",
                        "Set Name": "Unknown Set",
                        "Card Number": "1/1",
                        "Variant": "Normal",
                        "Language": "English",
                    }
                ],
                datasets=datasets,
            )

        self.assertEqual(intake_rows, [])
        self.assertEqual(len(errors), 1)
        self.assertIn("no dataset match", errors[0])


if __name__ == "__main__":
    unittest.main()
