from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
SEALED_DEALS_HTML = REPO_ROOT / "scripts" / "dashboards" / "sealed_deals.html"
SET_EXPLORER_HTML = REPO_ROOT / "scripts" / "dashboards" / "set_explorer.html"
INDEX_OVERVIEW_HUB_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_hub.html"
INDEX_OVERVIEW_SV100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview.html"
INDEX_OVERVIEW_MEGA100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_mega100.html"
INDEX_OVERVIEW_POKEMON100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_pokemon100.html"
INDEX_OVERVIEW_SWSH100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_swsh100.html"
INDEX_OVERVIEW_SM100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_sm100.html"
INDEX_OVERVIEW_XY100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_xy100.html"
INDEX_OVERVIEW_BW100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_bw100.html"
INDEX_OVERVIEW_DP100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_dp100.html"
INDEX_OVERVIEW_EX100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_ex100.html"
INDEX_OVERVIEW_NEO100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_neo100.html"
INDEX_OVERVIEW_ECARD100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_ecard100.html"
INDEX_OVERVIEW_WOTC100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_wotc100.html"
INDEX_OVERVIEW_JP_POKEMON100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_jp_pokemon100.html"
INDEX_OVERVIEW_JP_SV100_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_jp_sv100.html"


class SecondaryPageContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sealed_deals_html = SEALED_DEALS_HTML.read_text(encoding="utf-8")
        cls.set_explorer_html = SET_EXPLORER_HTML.read_text(encoding="utf-8")
        cls.index_overview_hub_html = INDEX_OVERVIEW_HUB_HTML.read_text(encoding="utf-8")
        cls.index_overview_sv100_html = INDEX_OVERVIEW_SV100_HTML.read_text(encoding="utf-8")
        cls.index_overview_mega100_html = INDEX_OVERVIEW_MEGA100_HTML.read_text(encoding="utf-8")
        cls.index_overview_pokemon100_html = INDEX_OVERVIEW_POKEMON100_HTML.read_text(encoding="utf-8")
        cls.index_overview_swsh100_html = INDEX_OVERVIEW_SWSH100_HTML.read_text(encoding="utf-8")
        cls.index_overview_sm100_html = INDEX_OVERVIEW_SM100_HTML.read_text(encoding="utf-8")
        cls.index_overview_xy100_html = INDEX_OVERVIEW_XY100_HTML.read_text(encoding="utf-8")
        cls.index_overview_bw100_html = INDEX_OVERVIEW_BW100_HTML.read_text(encoding="utf-8")
        cls.index_overview_dp100_html = INDEX_OVERVIEW_DP100_HTML.read_text(encoding="utf-8")
        cls.index_overview_ex100_html = INDEX_OVERVIEW_EX100_HTML.read_text(encoding="utf-8")
        cls.index_overview_neo100_html = INDEX_OVERVIEW_NEO100_HTML.read_text(encoding="utf-8")
        cls.index_overview_ecard100_html = INDEX_OVERVIEW_ECARD100_HTML.read_text(encoding="utf-8")
        cls.index_overview_wotc100_html = INDEX_OVERVIEW_WOTC100_HTML.read_text(encoding="utf-8")
        cls.index_overview_jp_pokemon100_html = INDEX_OVERVIEW_JP_POKEMON100_HTML.read_text(encoding="utf-8")
        cls.index_overview_jp_sv100_html = INDEX_OVERVIEW_JP_SV100_HTML.read_text(encoding="utf-8")

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

    def test_index_overview_hub_page_lists_indexes(self):
        self.assertIn("Index Overview", self.index_overview_hub_html)
        self.assertIn("Scarlet &amp; Violet 100", self.index_overview_hub_html)
        self.assertIn("Mega Evolution 100", self.index_overview_hub_html)
        self.assertIn("Original WOTC 100", self.index_overview_hub_html)
        self.assertIn("Sword &amp; Shield 100", self.index_overview_hub_html)
        self.assertIn("Pokemon Top 100", self.index_overview_hub_html)
        self.assertIn("/index-overview-pokemon100", self.index_overview_hub_html)
        self.assertIn("/index-overview-wotc100", self.index_overview_hub_html)
        self.assertIn("/index-overview-neo100", self.index_overview_hub_html)
        self.assertIn("/index-overview-ecard100", self.index_overview_hub_html)
        self.assertIn("/index-overview-dp100", self.index_overview_hub_html)
        self.assertIn("/index-overview-ex100", self.index_overview_hub_html)
        self.assertIn("/index-overview-bw100", self.index_overview_hub_html)
        self.assertIn("/index-overview-xy100", self.index_overview_hub_html)
        self.assertIn("/index-overview-sm100", self.index_overview_hub_html)
        self.assertIn("/index-overview-swsh100", self.index_overview_hub_html)
        self.assertIn("/index-overview-sv100", self.index_overview_hub_html)
        self.assertIn("/index-overview-mega100", self.index_overview_hub_html)
        self.assertIn("/index-overview-jp-pokemon100", self.index_overview_hub_html)
        self.assertIn("/index-overview-jp-sv100", self.index_overview_hub_html)

    def test_index_overview_sv100_page_has_core_sections(self):
        self.assertIn("Scarlet &amp; Violet 100", self.index_overview_sv100_html)
        self.assertIn("How This Index Works", self.index_overview_sv100_html)
        self.assertIn("Included Sets", self.index_overview_sv100_html)
        self.assertIn("Top 100 Holdings", self.index_overview_sv100_html)
        self.assertIn("/index-overview", self.index_overview_sv100_html)

    def test_mega100_index_overview_page_has_entry_point(self):
        self.assertIn("Mega Evolution 100", self.index_overview_mega100_html)
        self.assertIn("/index-overview", self.index_overview_mega100_html)
        self.assertIn("/index-overview-sv100", self.index_overview_mega100_html)
        self.assertIn("/index-overview-data?index=mega100", self.index_overview_mega100_html)

    def test_pokemon100_index_overview_page_has_entry_point(self):
        self.assertIn("Pokemon Top 100", self.index_overview_pokemon100_html)
        self.assertIn("/index-overview", self.index_overview_pokemon100_html)
        self.assertIn("/index-overview-data?index=pokemon100", self.index_overview_pokemon100_html)

    def test_swsh100_index_overview_page_has_entry_point(self):
        self.assertIn("Sword &amp; Shield 100", self.index_overview_swsh100_html)
        self.assertIn("/index-overview", self.index_overview_swsh100_html)
        self.assertIn("/index-overview-data?index=swsh100", self.index_overview_swsh100_html)

    def test_sm100_index_overview_page_has_entry_point(self):
        self.assertIn("Sun &amp; Moon 100", self.index_overview_sm100_html)
        self.assertIn("/index-overview", self.index_overview_sm100_html)
        self.assertIn("/index-overview-data?index=sm100", self.index_overview_sm100_html)

    def test_xy100_index_overview_page_has_entry_point(self):
        self.assertIn("XY 100", self.index_overview_xy100_html)
        self.assertIn("/index-overview", self.index_overview_xy100_html)
        self.assertIn("/index-overview-data?index=xy100", self.index_overview_xy100_html)

    def test_bw100_index_overview_page_has_entry_point(self):
        self.assertIn("Black &amp; White 100", self.index_overview_bw100_html)
        self.assertIn("/index-overview", self.index_overview_bw100_html)
        self.assertIn("/index-overview-data?index=bw100", self.index_overview_bw100_html)

    def test_dp100_index_overview_page_has_entry_point(self):
        self.assertIn("Diamond &amp; Pearl 100", self.index_overview_dp100_html)
        self.assertIn("/index-overview", self.index_overview_dp100_html)
        self.assertIn("/index-overview-data?index=dp100", self.index_overview_dp100_html)

    def test_ex100_index_overview_page_has_entry_point(self):
        self.assertIn("EX 100", self.index_overview_ex100_html)
        self.assertIn("/index-overview", self.index_overview_ex100_html)
        self.assertIn("/index-overview-data?index=ex100", self.index_overview_ex100_html)

    def test_neo100_index_overview_page_has_entry_point(self):
        self.assertIn("Neo 100", self.index_overview_neo100_html)
        self.assertIn("/index-overview", self.index_overview_neo100_html)
        self.assertIn("/index-overview-data?index=neo100", self.index_overview_neo100_html)

    def test_ecard100_index_overview_page_has_entry_point(self):
        self.assertIn("e-Card 100", self.index_overview_ecard100_html)
        self.assertIn("/index-overview", self.index_overview_ecard100_html)
        self.assertIn("/index-overview-data?index=ecard100", self.index_overview_ecard100_html)

    def test_wotc100_index_overview_page_has_entry_point(self):
        self.assertIn("Original WOTC 100", self.index_overview_wotc100_html)
        self.assertIn("/index-overview", self.index_overview_wotc100_html)
        self.assertIn("/index-overview-data?index=wotc100", self.index_overview_wotc100_html)

    def test_jp_pokemon100_index_overview_page_has_entry_point(self):
        self.assertIn("JP Pokemon Top 100", self.index_overview_jp_pokemon100_html)
        self.assertIn("/index-overview", self.index_overview_jp_pokemon100_html)
        self.assertIn("/index-overview-data?index=jp_pokemon100&category_id=85", self.index_overview_jp_pokemon100_html)

    def test_jp_sv100_index_overview_page_has_entry_point(self):
        self.assertIn("JP Scarlet &amp; Violet 100", self.index_overview_jp_sv100_html)
        self.assertIn("/index-overview", self.index_overview_jp_sv100_html)
        self.assertIn("/index-overview-data?index=jp_sv100&category_id=85", self.index_overview_jp_sv100_html)
