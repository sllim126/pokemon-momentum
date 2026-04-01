from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
SEALED_DEALS_HTML = REPO_ROOT / "scripts" / "dashboards" / "sealed_deals.html"
SET_EXPLORER_HTML = REPO_ROOT / "scripts" / "dashboards" / "set_explorer.html"


class SecondaryPageContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sealed_deals_html = SEALED_DEALS_HTML.read_text(encoding="utf-8")
        cls.set_explorer_html = SET_EXPLORER_HTML.read_text(encoding="utf-8")

    def test_sealed_deals_page_has_core_filters_and_table(self):
        self.assertIn("Poke6s Sealed Deals", self.sealed_deals_html)
        self.assertIn("Loading sealed deals...", self.sealed_deals_html)
        self.assertIn("/sealed_deals?", self.sealed_deals_html)
        self.assertIn("pack_count", self.sealed_deals_html)
        self.assertIn("price_per_pack", self.sealed_deals_html)

    def test_set_explorer_page_has_core_filters_and_fetch(self):
        self.assertIn("Poke6s Set Explorer", self.set_explorer_html)
        self.assertIn("/set_baskets?", self.set_explorer_html)
        self.assertIn("details.filter-menu", self.set_explorer_html)
        self.assertIn("top_hit_price", self.set_explorer_html)
        self.assertIn("total_set_cost", self.set_explorer_html)
