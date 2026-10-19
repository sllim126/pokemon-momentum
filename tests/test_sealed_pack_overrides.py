"""The owner's checked pack counts (scripts/dashboards/sealed_pack_overrides.csv)."""

import tempfile
import unittest
from pathlib import Path

from scripts.dashboards import api


class SealedPackOverridesCsvTests(unittest.TestCase):
    def test_reads_counts_and_pack_mixes_and_skips_bad_lines(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "overrides.csv"
            path.write_text(
                "product_id,product,pack_count,packs,source\n"
                "1,Mix,6,2x Journey Together; 1x Surging Sparks; 3x Unknown,owner\n"
                "2,Count only,8,,owner\n"
                "x,Bad id,5,,owner\n",
                encoding="utf-8",
            )
            overrides = api._load_sealed_pack_overrides_csv(path)
        self.assertEqual(set(overrides), {1, 2})
        self.assertEqual(overrides[1]["pack_count"], 6)
        self.assertEqual(overrides[1]["packs"], [
            {"set": "Journey Together", "count": 2},
            {"set": "Surging Sparks", "count": 1},
            {"set": "Unknown", "count": 3},
        ])
        self.assertEqual(overrides[2], {"pack_count": 8})

    def test_checked_file_is_loaded_and_each_mix_adds_up(self):
        overrides = api._load_sealed_pack_overrides_csv()
        self.assertGreater(len(overrides), 100)
        for product_id, override in overrides.items():
            if "packs" in override:
                self.assertEqual(sum(p["count"] for p in override["packs"]), override["pack_count"], product_id)
        # The checked counts win over the older hand entries in api.py.
        self.assertEqual(api._find_pack_count_override(635609)["pack_count"], 360)


class JapaneseBoxSizeTests(unittest.TestCase):
    def test_specialty_sets_are_ten_packs_and_regular_sets_thirty(self):
        cases = {
            ("Terastal Fest ex Booster Box", "SV8a: Terastal Fest ex"): 10,
            ("VMAX Climax Booster Box", "S8b: VMAX Climax"): 10,
            ("Shiny Star V Booster Box", "S4a: Shiny Star V"): 10,
            ("TAG TEAM GX: Tag All Stars Booster Box", "SM12a: TAG TEAM GX: Tag All Stars"): 10,
            ("Mega Dream ex Booster Box", "M2a: High Class Pack: MEGA Dream ex"): 10,
            ("Pokemon Card 151 Booster Box", "SV2a: Pokemon Card 151"): 20,
            ("Black Bolt Deluxe Booster Box", "SV11B: Black Bolt"): 4,
            ("25th Anniversary Collection Booster Box", "S8a: 25th Anniversary Collection"): 16,
            ("MEGA Expansion 30th Celebration Booster Box", "M6a: MEGA Expansion 30th Celebration"): 20,
            ("Dark Phantasma Booster Box", "S10a: Dark Phantasma"): 20,
            ("Pokemon GO Booster Box", "S10b: Pokemon GO"): 20,
            ("Battle Region Booster Box", "S9a: Battle Region"): 10,
            ("Inferno X Booster Box", "M2: Inferno X"): 30,
        }
        for (name, group), packs in cases.items():
            self.assertEqual(api._jp_pricing_profile(name, group)[0], packs, name)


if __name__ == "__main__":
    unittest.main()
