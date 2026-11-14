"""Lorcana Phase 0 (docs/lorcana_market_plan.md): category, rarity tiers, pipeline wiring,
history unpacking and the Lorcast image plan."""

import argparse
import io
import json
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

import duckdb

from scripts.common.category_config import get_category_config
from scripts.common.lorcana_rarity import lorcana_rarity_tier, lorcana_rarity_tier_sql
from scripts.common.product_classification import get_product_kind_sql
from scripts.download import collect_tcgcsv_categories, fetch_lorcast_images
from scripts.extract import extract_category_from_archives as extract
from scripts.indicators import build_lorcana_card_attributes as attributes
from scripts.pipeline import notify_discord, run_daily_update
from scripts.utilities import export_products_for_my_groups as product_export


class LorcanaCategoryTests(unittest.TestCase):
    def test_lorcana_uses_its_own_table_prefix(self):
        category = get_category_config(71)
        self.assertEqual(category.slug, "lorcana")
        self.assertEqual(category.products_table, "lorcana_products")
        self.assertEqual(category.product_signal_table, "lorcana_product_signal_snapshot")

    def test_collect_only_job_no_longer_fetches_lorcana(self):
        # The nightly pipeline fetches Lorcana now; fetching it here too would ask TCGCSV
        # for the same files twice a day.
        self.assertNotIn(71, collect_tcgcsv_categories.COLLECT_CATEGORIES)

    def test_discord_summary_reports_lorcana(self):
        self.assertEqual(notify_discord.CATEGORIES[71], ("Lorcana", "lorcana"))


class LorcanaRarityTierTests(unittest.TestCase):
    def test_tiers(self):
        self.assertEqual(lorcana_rarity_tier("Enchanted"), "chase")
        self.assertEqual(lorcana_rarity_tier("iconic"), "chase")
        self.assertEqual(lorcana_rarity_tier("Super Rare"), "high")
        self.assertEqual(lorcana_rarity_tier("Common"), "base")
        self.assertEqual(lorcana_rarity_tier("None"), "promo")
        self.assertEqual(lorcana_rarity_tier("Quest"), "quest")

    def test_unknown_or_empty_is_unclassified(self):
        self.assertIsNone(lorcana_rarity_tier("Mythic"))
        self.assertIsNone(lorcana_rarity_tier(""))
        self.assertIsNone(lorcana_rarity_tier(None))

    def test_sql_matches_python(self):
        con = duckdb.connect()
        for rarity in ["Enchanted", "Legendary", "Rare", "Promo", "Quest", "Mythic", None]:
            got = con.execute(f"SELECT {lorcana_rarity_tier_sql('p.rarity')} FROM (SELECT ? AS rarity) p", [rarity]).fetchone()[0]
            self.assertEqual(got, lorcana_rarity_tier(rarity), rarity)


class TinPatternTests(unittest.TestCase):
    """Tins are sealed; Lorcana cards named "Tinker Bell" or "... Tiny ..." are not."""

    def kind(self, name, number="12/204", rarity="Common"):
        con = duckdb.connect()
        sql = f"SELECT {get_product_kind_sql('p')} FROM (SELECT ? AS name, ? AS rarity, ? AS number) p"
        return con.execute(sql, [name, rarity, number]).fetchone()[0]

    def test_cards_with_tin_inside_a_word_stay_cards(self):
        self.assertEqual(self.kind("Tinker Bell - Tiny Tactician"), "card")
        self.assertEqual(self.kind("Merlin - Ink Drop Tinkerer"), "card")

    def test_pokemon_tins_stay_sealed(self):
        self.assertEqual(self.kind("Pokemon GO Mini Tin", "", ""), "sealed")
        self.assertEqual(self.kind("Collector Tins Bundle", "", ""), "sealed")


class DailyUpdateIndexStepTests(unittest.TestCase):
    def steps(self, category_id):
        args = argparse.Namespace(
            category_id=category_id, skip_download=True, skip_metadata=True, skip_analytics=False,
            skip_parquet=False, full_metadata_refresh=False, workers=1, refresh_csv=False,
            limit_days=0, latest_first=False,
        )
        return [name for name, _ in run_daily_update.build_steps(args)]

    def test_lorcana_skips_the_index_step_until_it_has_indexes(self):
        names = self.steps(71)
        self.assertFalse(any("index overview" in n for n in names))
        self.assertIn("Build Lorcana product signal snapshot", names)
        self.assertIn("Export parquet partitions", names)

    def test_pokemon_keeps_the_index_step(self):
        self.assertTrue(any("index overview" in n for n in self.steps(3)))


@unittest.skipUnless(shutil.which("7z"), "7z is only in the container")
class ExtractCategoryTests(unittest.TestCase):
    def test_fills_existing_days_from_both_sources_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            # A daily 7z archive for 2025-06-01 holding Pokemon (3) and Lorcana (71).
            src = tmp / "src"
            for category in ("3", "71"):
                group = src / "2025-06-01" / category / "123"
                group.mkdir(parents=True)
                (group / "prices").write_text('{"results": []}')
            raw = tmp / "raw"
            raw.mkdir()
            subprocess.run(["7z", "a", str(raw / "prices-2025-06-01.ppmd.7z"), "2025-06-01"],
                           cwd=src, check=True, stdout=subprocess.DEVNULL)
            (raw / "prices-2025-06-02.ppmd.7z").write_bytes(b"")  # a failed, empty download
            # Collect-only tarballs for 2026-11-13 (day folder exists) and 2026-11-14 (it doesn't).
            collect = tmp / "collect"
            for day in ("2026-11-13", "2026-11-14"):
                (collect / day).mkdir(parents=True)
                with tarfile.open(collect / day / f"tcgcsv-{day}-71.tar.gz", "w:gz") as tar:
                    for name in ("71/groups", "71/456/prices", "71/456/products"):
                        info = tarfile.TarInfo(name)
                        info.size = 2
                        tar.addfile(info, io.BytesIO(b"{}"))
            out = tmp / "extracted"
            for day in ("2025-06-01", "2026-11-13"):
                (out / f"prices-{day}.ppmd" / day / "3").mkdir(parents=True)

            argv = ["--category-id", "71", "--archives-dir", str(raw), "--collect-dir", str(collect),
                    "--extract-root", str(out)]
            self.assertEqual(extract.main(argv), 0)
            self.assertTrue((out / "prices-2025-06-01.ppmd/2025-06-01/71/123/prices").exists())
            self.assertTrue((out / "prices-2026-11-13.ppmd/2026-11-13/71/456/prices").exists())
            self.assertFalse((out / "prices-2026-11-13.ppmd/2026-11-13/71/456/products").exists())
            self.assertFalse((out / "prices-2026-11-14.ppmd").exists())  # no day folder: left alone
            self.assertEqual(list(out.glob(".tmp-*")), [])
            self.assertEqual(extract.main(argv), 0)  # second run: nothing to do


class LorcastImagePlanTests(unittest.TestCase):
    def card(self, tcgplayer_id, url):
        return {"id": "crd_1", "tcgplayer_id": tcgplayer_id, "collector_number": "7",
                "set": {"code": "1"}, "image_uris": {"digital": {"large": url}}}

    def test_downloads_missing_or_changed_images_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / "large").mkdir()
            (out / "large" / "100.avif").write_bytes(b"x")
            (out / "large" / "200.avif").write_bytes(b"x")
            manifest = {"100": {"source": "https://cards/a.avif?1"}, "200": {"source": "https://cards/b.avif?1"}}
            cards = [
                self.card(100, "https://cards/a.avif?1"),  # unchanged: skip
                self.card(200, "https://cards/b.avif?2"),  # image replaced: fetch again
                self.card(300, "https://cards/c.avif?1"),  # new: fetch
                self.card(None, "https://cards/d.avif?1"),  # no TCGplayer id: ignore
            ]
            new_manifest, todo = fetch_lorcast_images.plan_downloads(cards, manifest, out)
            self.assertEqual([key for key, _ in todo], ["200", "300"])
            self.assertEqual(sorted(new_manifest), ["100", "200", "300"])
            self.assertEqual(new_manifest["300"]["file"], "large/300.avif")


class LorcanaCardAttributeTests(unittest.TestCase):
    def product(self, product_id, **fields):
        return {"productId": product_id, "extendedData": [{"name": k, "value": v} for k, v in fields.items()]}

    def test_card_fields_are_read_and_sealed_products_left_out(self):
        card = self.product(1, Property="Frozen", InkType="Amber;Steel", CardType="Character",
                            Rarity="Enchanted", **{"Cost Ink": "5", "Lore Value": "2"})
        row = attributes.attribute_row(24740, card)
        self.assertEqual((row["property"], row["ink"], row["inks"]), ("Frozen", "Amber", "Amber;Steel"))
        self.assertEqual((row["cost"], row["lore"], row["strength"]), (5, 2, None))
        self.assertEqual(row["rarity_tier"], "chase")
        self.assertIsNone(attributes.attribute_row(24740, self.product(2)))  # a booster box

    def test_newer_copy_of_a_set_wins(self):
        import os
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / "catalogue" / "10").mkdir(parents=True)
            (tmp / "catalogue" / "11").mkdir(parents=True)
            (tmp / "raw").mkdir()
            old = tmp / "catalogue" / "10" / "products.json"
            new = tmp / "raw" / "10.json"
            only = tmp / "catalogue" / "11" / "products.json"
            for path in (old, new, only):
                path.write_text('{"results": []}')
            os.utime(old, (1, 1))
            files = attributes.newest_product_files(tmp / "raw", tmp / "catalogue")
            self.assertEqual(files, {10: new, 11: only})

    def test_build_writes_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            path = tmp / "1.json"
            path.write_text(json.dumps({"results": [self.product(5, Property="Moana", InkType="Ruby")]}))
            rows = attributes.build_rows({1: path})
            attributes.write_outputs(rows, tmp / "a.csv", tmp / "db.duckdb")
            got = duckdb.connect(str(tmp / "db.duckdb")).execute(
                "SELECT productId, groupId, property, ink FROM lorcana_card_attributes").fetchall()
            self.assertEqual(got, [(5, 1, "Moana", "Ruby")])

    def test_metadata_step_keeps_raw_lists_for_lorcana_only(self):
        self.assertEqual(product_export.RAW_PRODUCT_CATEGORIES, {71})
        with tempfile.TemporaryDirectory() as tmp:
            original = product_export.DATA_DIR
            product_export.DATA_DIR = tmp
            try:
                product_export.save_raw_products(get_category_config(71), 9, [{"productId": 1}])
            finally:
                product_export.DATA_DIR = original
            saved = json.loads((Path(tmp) / "lorcana_products_raw" / "9.json").read_text())
            self.assertEqual(saved, {"results": [{"productId": 1}]})

    def test_nightly_runs_it_for_lorcana_only(self):
        def names(category_id):
            args = argparse.Namespace(
                category_id=category_id, skip_download=True, skip_metadata=False, skip_analytics=True,
                skip_parquet=True, full_metadata_refresh=False, workers=1, refresh_csv=False,
                limit_days=0, latest_first=False,
            )
            return [name for name, _ in run_daily_update.build_steps(args)]
        self.assertIn("Build Lorcana card attributes", names(71))
        self.assertNotIn("Build Lorcana card attributes", names(3))
        self.assertTrue(run_daily_update.can_fall_back_to_cached_metadata("Build Lorcana card attributes"))


class LorcanaImageRouteTests(unittest.TestCase):
    """/lorcana/images/<productId>.avif: admin-only while Lorcana is internal."""

    @classmethod
    def setUpClass(cls):
        from fastapi.testclient import TestClient
        from scripts.dashboards import api
        cls.api = api
        cls.client = TestClient(api.app)

    def test_needs_a_sign_in(self):
        self.assertEqual(self.client.get("/lorcana/images/123.avif").status_code, 401)

    def test_admin_gets_the_image_or_404(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "123.avif").write_bytes(b"avif-bytes")
            with patch.object(self.api, "require_admin_user", new=lambda **_: None), \
                    patch.object(self.api, "LORCANA_IMAGE_DIR", Path(tmp)):
                found = self.client.get("/lorcana/images/123.avif")
                missing = self.client.get("/lorcana/images/456.avif")
                not_a_number = self.client.get("/lorcana/images/..%2Fsecret.avif")
        self.assertEqual(found.status_code, 200)
        self.assertEqual(found.content, b"avif-bytes")
        self.assertEqual(found.headers["content-type"], "image/avif")
        self.assertTrue(found.headers["cache-control"].startswith("private"))
        self.assertEqual(missing.status_code, 404)
        self.assertIn(not_a_number.status_code, (404, 422))


if __name__ == "__main__":
    unittest.main()
