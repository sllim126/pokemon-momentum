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

    def test_dashboard_contains_quick_buy_targets_and_footer_link(self):
        self.assertIn("Quick Buy Targets", self.html)
        self.assertIn("https://market.poke6s.com", self.html)
        self.assertIn("not financial advice", self.html.lower())

    def test_good_buys_filter_sets_popout_exists(self):
        self.assertIn("Filter Sets", self.html)
        self.assertIn("goodBuysFilterTrigger", self.html)
        self.assertIn("goodBuysSetPopover", self.html)

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
        self.assertIn("Browse Set shows every tracked product in the selected set.", self.html)
        self.assertIn("Browse Species shows matching listings for a searched card name across sets.", self.html)
