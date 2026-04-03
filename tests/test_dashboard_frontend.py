from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_HTML = REPO_ROOT / "scripts" / "dashboards" / "dashboard.html"


class DashboardFrontendContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = DASHBOARD_HTML.read_text(encoding="utf-8")

    def test_research_tabs_use_current_labels(self):
        self.assertIn('data-tab="group_signals">Set Strength</button>', self.html)
        self.assertIn('data-tab="group_products">Browse Set</button>', self.html)
        self.assertIn('data-tab="browse_species">Browse Species</button>', self.html)
        self.assertIn('data-browse-set-view="time_to_buy">Time to Buy</button>', self.html)

    def test_dashboard_contains_quick_buy_targets_and_footer_link(self):
        self.assertIn("Quick Buy Targets", self.html)
        self.assertIn("https://market.poke6s.com", self.html)
        self.assertIn("not financial advice", self.html.lower())

    def test_good_buys_filter_sets_popout_exists(self):
        self.assertIn("Filter Sets", self.html)
        self.assertIn("goodBuysFilterTrigger", self.html)
        self.assertIn("goodBuysSetPopover", self.html)

    def test_browse_set_filter_menu_exists(self):
        self.assertIn('id="browseSetFilters"', self.html)
        self.assertIn('id="browseSetFilterSummary"', self.html)
        self.assertIn('id="browseSetFilterPanel"', self.html)
        self.assertIn("Hits Only", self.html)
        self.assertIn("IR+", self.html)
        self.assertIn("Poke Ball Holo", self.html)
        self.assertIn("Master Ball Holo", self.html)
        self.assertIn("Energy Symbol Pattern", self.html)

    def test_segment_toggle_visibility_logic_hides_irrelevant_tabs(self):
        self.assertIn('currentTab !== "browse_species"', self.html)
        self.assertIn('segmentFilteredTabs = new Set([', self.html)
        self.assertIn('"breakouts"', self.html)
        self.assertIn('"good_buys"', self.html)
        self.assertIn('"group_products"', self.html)

    def test_mobile_and_header_segment_controls_exist(self):
        self.assertGreaterEqual(self.html.count('data-segment="cards"'), 3)
        self.assertGreaterEqual(self.html.count('data-segment="sealed"'), 3)

    def test_browse_set_and_browse_species_help_text_is_present(self):
        self.assertIn("Browse Set shows tracked products in the selected set.", self.html)
        self.assertIn("Browse Species shows matching listings for a searched card name across sets.", self.html)

    def test_guide_overlay_and_account_entry_exist(self):
        self.assertIn('id="openGuideBtn"', self.html)
        self.assertIn('id="guideModal"', self.html)
        self.assertIn("How to Use the Dashboard", self.html)
        self.assertIn("maybeOpenGuideOnFirstVisit", self.html)

    def test_header_search_results_render_with_thumbnail_support(self):
        self.assertIn("brandsearch-result-thumb", self.html)
        self.assertIn("brandsearch-result-body", self.html)
