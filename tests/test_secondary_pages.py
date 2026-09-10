from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
DESKTOP_LAB_HTML = REPO_ROOT / "scripts" / "dashboards" / "dashboard_lab.html"
SEALED_DEALS_HTML = REPO_ROOT / "scripts" / "dashboards" / "sealed_deals.html"
SET_EXPLORER_HTML = REPO_ROOT / "scripts" / "dashboards" / "set_explorer.html"
BUDGET_BUILDER_HTML = REPO_ROOT / "scripts" / "dashboards" / "budget_builder.html"
COLLECTOR_HUB_HTML = REPO_ROOT / "scripts" / "dashboards" / "collector_hub.html"
PLACEHOLDER_LIBRARY_HTML = REPO_ROOT / "scripts" / "dashboards" / "placeholder_library.html"
INDEX_OVERVIEW_HUB_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_hub.html"
INDEX_OVERVIEW_DETAIL_HTML = REPO_ROOT / "scripts" / "dashboards" / "index_overview_detail.html"


class SecondaryPageContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.desktop_lab_html = DESKTOP_LAB_HTML.read_text(encoding="utf-8")
        cls.sealed_deals_html = SEALED_DEALS_HTML.read_text(encoding="utf-8")
        cls.set_explorer_html = SET_EXPLORER_HTML.read_text(encoding="utf-8")
        cls.budget_builder_html = BUDGET_BUILDER_HTML.read_text(encoding="utf-8")
        cls.collector_hub_html = COLLECTOR_HUB_HTML.read_text(encoding="utf-8")
        cls.placeholder_library_html = PLACEHOLDER_LIBRARY_HTML.read_text(encoding="utf-8")
        cls.index_overview_hub_html = INDEX_OVERVIEW_HUB_HTML.read_text(encoding="utf-8")
        cls.index_overview_detail_html = INDEX_OVERVIEW_DETAIL_HTML.read_text(encoding="utf-8")

    def test_desktop_lab_page_exposes_live_route_reference_links(self):
        self.assertIn("Desktop Lab", self.desktop_lab_html)
        self.assertIn("Market Command Center", self.desktop_lab_html)
        self.assertIn("/dashboard", self.desktop_lab_html)
        self.assertIn("/set-explorer", self.desktop_lab_html)
        self.assertIn("/sealed-deals", self.desktop_lab_html)
        self.assertIn("/dashboard?tab=group_products", self.desktop_lab_html)
        self.assertIn("/dashboard?tab=browse_species", self.desktop_lab_html)
        self.assertIn("/dashboard?tab=under_the_radar", self.desktop_lab_html)

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

    def test_budget_builder_page_has_budget_controls_and_api_fetch(self):
        self.assertIn("Poke6s Budget Builder", self.budget_builder_html)
        self.assertIn("budgetSlider", self.budget_builder_html)
        self.assertIn("budgetInput", self.budget_builder_html)
        self.assertIn("maxPriceInput", self.budget_builder_html)
        self.assertIn("Maximum Card Price", self.budget_builder_html)
        self.assertIn('<option value="0">No limit</option>', self.budget_builder_html)
        self.assertIn("rarityGrid", self.budget_builder_html)
        self.assertIn("/budget_builder", self.budget_builder_html)
        self.assertIn("Use IR+", self.budget_builder_html)
        self.assertIn("I already have this card, find a replacement", self.budget_builder_html)
        self.assertIn("ownedStrip", self.budget_builder_html)
        self.assertIn("refreshResultsBtn", self.budget_builder_html)
        self.assertIn("Refresh Results", self.budget_builder_html)
        self.assertIn("allowDuplicatesToggle", self.budget_builder_html)
        self.assertIn("Duplicates are acceptable", self.budget_builder_html)

    def test_collector_hub_page_has_manifest_and_core_routes(self):
        self.assertIn("Poke6s Collector Hub", self.collector_hub_html)
        self.assertIn("Master Set Hub", self.collector_hub_html)
        self.assertIn("/collector-manifest", self.collector_hub_html)
        self.assertIn("/placeholders", self.collector_hub_html)
        self.assertIn("/collector-assets/checklists-sv/index.html", self.collector_hub_html)
        self.assertIn("/collector-assets/print-combined/index.html", self.collector_hub_html)
        self.assertIn("/dashboard-lab", self.collector_hub_html)
        self.assertIn("/set-explorer", self.collector_hub_html)

    def test_placeholder_library_page_has_manifest_and_download_routes(self):
        self.assertIn("Poke6s Placeholder Library", self.placeholder_library_html)
        self.assertIn("Placeholder Library", self.placeholder_library_html)
        self.assertIn("/placeholder-downloads-manifest", self.placeholder_library_html)
        self.assertIn("/placeholder-downloads/sv-source-csv", self.placeholder_library_html)
        self.assertIn("/collector-assets/print-combined/index.html", self.placeholder_library_html)
        self.assertIn("/collector-assets/checklists-sv/index.html", self.placeholder_library_html)

    def test_index_overview_hub_page_lists_indexes(self):
        self.assertIn("Index Overview", self.index_overview_hub_html)
        self.assertIn("Scarlet &amp; Violet 100", self.index_overview_hub_html)
        self.assertIn("Mega Evolution 100", self.index_overview_hub_html)
        self.assertIn("Original WOTC 100", self.index_overview_hub_html)
        self.assertIn("Sword &amp; Shield 100", self.index_overview_hub_html)
        self.assertIn("Pokemon Top 151", self.index_overview_hub_html)
        self.assertIn("JP Pokemon Top 151", self.index_overview_hub_html)
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

    def test_shared_index_overview_page_has_all_route_contracts(self):
        html = self.index_overview_detail_html
        self.assertIn("How This Index Works", html)
        self.assertIn("Included Sets", html)
        self.assertIn("holdingsTitle", html)
        self.assertIn("activeIndex.categoryId", html)
        self.assertIn("activeIndex.index", html)
        for route in (
            "pokemon100", "sv100", "mega100", "wotc100", "neo100", "ecard100",
            "ex100", "dp100", "bw100", "xy100", "sm100", "swsh100",
            "jp_pokemon100", "jp_sv100",
        ):
            self.assertIn(f'index: "{route}"', html)
