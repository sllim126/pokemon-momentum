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


if __name__ == "__main__":
    unittest.main()
