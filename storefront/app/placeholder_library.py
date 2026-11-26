"""Placeholder Library (`/placeholders`, templates/placeholder_library.html): the printable binder
placeholders, checklists and their spreadsheet files, plus the file serving behind them and the
Collector Hub's tiles.

Moved from market.poke6s.com on 2026-11-20 (docs/market_inventory.md, option B, third piece);
the market's /placeholders, /collector-assets/... and /placeholder-downloads/... now redirect
here.

The files are generated outside the store: the "TCG Placeholders" folder of the main checkout,
rebuilt every night by scripts/utilities/sync_placeholder_library.py (docs/tcg_placeholders_guide.md).
The store gets that folder mounted read-only (docker-compose.yml), so a rebuild shows up here
straight away, with no deploy. Only the folders in BUCKETS and the files in DOWNLOADS are
reachable, and only these file types.

The generated pages carry their own inline <style> and <script> (checkbox saving, the review
page's filters). The store's CSP allows neither, so each page is served with a CSP that allows
exactly its own inline scripts, by SHA-256 hash (`page_csp`), plus inline styles. Anything else
injected into a page still can't run.
"""

from __future__ import annotations

import base64
import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

# URL name -> folder under "TCG Placeholders". Same names the market used, so old links map 1:1.
BUCKETS = {
    "checklists-sv": "checklists_sv",
    "checklists-mega": "checklists_mega",
    "print-sv": "output",
    "print-mega": "output_mega",
    "print-combined": "output_combined",
    "docs": "docs",
}

# Download id -> file under "TCG Placeholders". Same ids as the market's /placeholder-downloads.
DOWNLOADS = {
    "sv-source-csv": "placeholders.csv",
    "mega-source-csv": "mega_placeholders.csv",
    # Promos found in the catalog but not yet in mega_placeholders.csv (sync_placeholder_library.py).
    "mega-auto-promos-csv": "auto_mega_promos.csv",
    "prize-pack-series7-csv": "prize_pack_series7.csv",
    "prize-pack-series8-csv": "prize_pack_series8.csv",
    "prize-pack-series9-csv": "prize_pack_series9.csv",
    "combined-normalized-csv": "output_combined/placeholders.normalized.csv",
    "combined-printable-csv": "output_combined/placeholders.printable_unique.csv",
    "combined-json": "output_combined/placeholders.json",
    "combined-validation-report": "output_combined/validation_report.md",
    "sv-checklist-csv": "checklists_sv/scarlet_violet_checklist_all.csv",
    "sv-review-queue": "checklists_sv/review_queue.csv",
    "mega-checklist-csv": "checklists_mega/mega_evolution_checklist_all.csv",
    "mega-review-queue": "checklists_mega/review_queue.csv",
    # Made by hand (the owner's Publisher file, exported to PDF), not by the nightly build: a
    # title card for each Journey Together promo, saying which product it came from.
    "jtg-title-cards": "title_cards/journey_together_promo_title_cards.pdf",
}

# Per set, two PDFs from "TCG Placeholders/tools/build_title_cards.py" (owner, 2026-11-25/26),
# rebuilt nightly by scripts/utilities/sync_placeholder_library.py:
# - a printable checklist: every card with a box for each version, then the promos
#   (download id "<code>-checklist", file title_cards/checklists/<stem>_checklist.pdf; the set's
#   checklist page links to the same id);
# - grandmaster title cards: a card-sized label for every printing
#   (id "<stem>-title-cards" with "_" as "-", file title_cards/<stem>_grandmaster_title_cards.pdf).
# Set name, file stem and set code, in release order, per era.
MEGA_SETS = (
    ("Mega Evolution", "mega_evolution", "MEG"), ("Phantasmal Flames", "phantasmal_flames", "PFL"),
    ("Ascended Heroes", "ascended_heroes", "ASC"), ("Perfect Order", "perfect_order", "POR"),
    ("Chaos Rising", "chaos_rising", "CRI"), ("Pitch Black", "pitch_black", "PBL"),
    ("30th Celebration", "30th_celebration", "30C"), ("Delta Reign", "delta_reign", "DLR"),
    ("Mega Evolution Black Star Promos", "mega_evolution_promo", "MEP"),
)
SV_SETS = (
    ("Scarlet & Violet", "scarlet_violet_base_set", "SVI"), ("Paldea Evolved", "paldea_evolved", "PAL"),
    ("Obsidian Flames", "obsidian_flames", "OBF"), ("151", "scarlet_violet_151", "MEW"),
    ("Paradox Rift", "paradox_rift", "PAR"), ("Paldean Fates", "paldean_fates", "PAF"),
    ("Temporal Forces", "temporal_forces", "TEF"), ("Twilight Masquerade", "twilight_masquerade", "TWM"),
    ("Shrouded Fable", "shrouded_fable", "SFA"), ("Stellar Crown", "stellar_crown", "SCR"),
    ("Surging Sparks", "surging_sparks", "SSP"), ("Prismatic Evolutions", "prismatic_evolutions", "PRE"),
    ("Journey Together", "journey_together", "JTG"), ("Destined Rivals", "destined_rivals", "DRI"),
    ("Black Bolt", "black_bolt", "BLK"), ("White Flare", "white_flare", "WHT"),
)


def title_card_id(stem: str) -> str:
    return stem.replace("_", "-") + "-title-cards"


def checklist_id(code: str) -> str:
    return f"{code.lower()}-checklist"


for _name, _stem, _code in MEGA_SETS + SV_SETS:
    DOWNLOADS[title_card_id(_stem)] = f"title_cards/{_stem}_grandmaster_title_cards.pdf"
    DOWNLOADS[checklist_id(_code)] = f"title_cards/checklists/{_stem}_checklist.pdf"


def set_items(sets: tuple[tuple[str, str, str], ...]) -> tuple["Item", ...]:
    """One row per set: the checklist (main button) and its title cards."""
    return tuple(Item(name, "Every promo, staff and stamped version, with the product or event each came "
                      "from." if code == "MEP" else "Every card, with a box for each version it comes in, "
                      "then its promos and stamps.", "Collectors", download=checklist_id(code),
                      label="Checklist", also=(("Title cards", title_card_id(stem)),))
                 for name, stem, code in sets)

# What may be served, and as what. Markdown shows as plain text so it reads in the browser.
MEDIA_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".csv": "text/csv; charset=utf-8",
    ".json": "application/json",
    ".md": "text/plain; charset=utf-8",
    ".pdf": "application/pdf",
}

# Pages whose dates say when the library was last rebuilt (the nightly job rewrites them).
BUILD_MARKERS = ("output_combined/index.html", "checklists_mega/index.html")

INLINE_SCRIPT = re.compile(rb"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", re.S | re.I)


def resource_url(asset: str) -> str:
    """Address of a generated page or file, `asset` being "<bucket>/<path>"."""
    return f"/placeholders/files/{asset}"


def download_url(download_id: str) -> str:
    return f"/placeholders/download/{download_id}"


def page_csp(base_csp: str, body: bytes) -> str:
    """The store's CSP, loosened for one generated page: its own inline scripts (by hash) and
    inline styles. Nothing else changes."""
    hashes = " ".join(
        "'sha256-" + base64.b64encode(hashlib.sha256(script).digest()).decode() + "'"
        for script in INLINE_SCRIPT.findall(body)
    )
    csp = base_csp.replace("script-src 'self'", f"script-src 'self' {hashes}".rstrip())
    return csp.replace("style-src 'self'", "style-src 'self' 'unsafe-inline'")


def media_type(path: Path) -> str | None:
    return MEDIA_TYPES.get(path.suffix.lower())


class PlaceholderFiles:
    """The read-only "TCG Placeholders" folder: safe lookups, sizes and the last build date."""

    def __init__(self, root: Path):
        self.root = root

    def asset(self, bucket: str, rel: str) -> Path | None:
        """A file inside one of BUCKETS, or None (unknown bucket, escape attempt, hidden or
        unlisted file type, missing)."""
        folder = BUCKETS.get(bucket)
        if folder is None or any(part.startswith(".") for part in Path(rel).parts):
            return None
        base = (self.root / folder).resolve()
        target = (base / rel).resolve()
        if not target.is_relative_to(base) or not target.is_file() or media_type(target) is None:
            return None
        return target

    def download(self, download_id: str) -> Path | None:
        rel = DOWNLOADS.get(download_id)
        if rel is None:
            return None
        target = self.root / rel
        return target if target.is_file() else None

    def size_label(self, download_id: str) -> str:
        target = self.download(download_id)
        if target is None:
            return ""
        size = target.stat().st_size
        if size >= 1024 * 1024:
            return f"{size / (1024 * 1024):.1f} MB"
        return f"{max(1, round(size / 1024))} KB"

    def last_built(self) -> datetime | None:
        times = [(self.root / rel).stat().st_mtime for rel in BUILD_MARKERS if (self.root / rel).is_file()]
        return datetime.fromtimestamp(max(times), UTC) if times else None

    def available(self) -> bool:
        return (self.root / "output_combined" / "index.html").is_file()


@dataclass(frozen=True)
class Item:
    title: str
    text: str
    audience: str
    page: str = ""          # "<bucket>/<path>" of a page to open
    download: str = ""      # a DOWNLOADS id
    label: str = "Download"  # the download button's text
    also: tuple[tuple[str, str], ...] = ()   # more buttons: (text, DOWNLOADS id)


@dataclass(frozen=True)
class Section:
    title: str
    text: str
    items: tuple[Item, ...]


# The library page, in the order a collector needs it. Wording carried over from the market page.
SECTIONS = (
    Section("Print pages and checklists", "Open in the browser. Most collectors only need these.", (
        Item("All placeholders (start here)", "Print placeholder cards for Scarlet & Violet, Mega Evolution and "
             "Prize Pack cards, chosen by set release or card code.", "Collectors", page="print-combined/index.html"),
        Item("Mega Evolution placeholders", "Only the Mega Evolution era, including promos.", "Collectors",
             page="print-mega/index.html"),
        Item("Scarlet & Violet placeholders", "Only the Scarlet & Violet era.", "Collectors",
             page="print-sv/index.html"),
        Item("Mega Evolution checklists", "Tick-off checklists for the Mega Evolution era, including promos. "
             "Ticks are saved in your browser.", "Collectors", page="checklists-mega/index.html"),
        Item("Scarlet & Violet checklists", "Tick-off checklists for every Scarlet & Violet set.", "Collectors",
             page="checklists-sv/index.html"),
    )),
    Section("Set checklists and title cards: Mega Evolution", "Printable PDFs for each set. The checklist "
            "has every card with a box for each version it comes in (reverse holo, Poké Ball and so on), "
            "then the promos and stamps with where each came from. The title cards are a card-sized label "
            "for every one of those, in binder order: print on Letter paper, cut along the lines and "
            "sleeve one in each empty slot.", set_items(MEGA_SETS)),
    Section("Set checklists and title cards: Scarlet & Violet", "The same for every Scarlet & Violet set. "
            "Here each set's Black Star promos are in its own files.", set_items(SV_SETS) + (
        Item("Journey Together promo title cards (original)", "The first, hand-made sheet: only the "
             "Journey Together promos and the product, event or store each came from.", "Collectors",
             download="jtg-title-cards"),
    )),
    Section("Checklists as spreadsheets",
            "The promo and special-version checklists as CSV files, for Excel, Numbers or Google Sheets.", (
        Item("Mega Evolution checklist", "Every Mega Evolution checklist row in one spreadsheet.", "Collectors",
             download="mega-checklist-csv"),
        Item("Scarlet & Violet checklist", "Every Scarlet & Violet checklist row in one spreadsheet.", "Collectors",
             download="sv-checklist-csv"),
    )),
    Section("Generated files", "Built automatically from the source lists every night.", (
        Item("Printable card list", "One row per card to print, with duplicates removed. Handy for checking what's "
             "missing or printing with your own tool.", "Spreadsheets", download="combined-printable-csv"),
        Item("Every placeholder row", "Every placeholder row after clean-up, for checking in a spreadsheet.",
             "Spreadsheets", download="combined-normalized-csv"),
        Item("JSON export", "The same data as JSON, for scripts and other apps.", "Developers",
             download="combined-json"),
        Item("Validation report", "An automatic check of the lists: row counts, duplicates and missing fields.",
             "Maintainers", download="combined-validation-report"),
    )),
    Section("Source lists",
            "The lists everything above is built from. Spotted a wrong or missing card?", (
        Item("Mega Evolution source list", "The hand-checked list for Mega Evolution cards.", "Maintainers",
             download="mega-source-csv"),
        Item("New Mega promos (not confirmed yet)", "Promos found in the TCGplayer catalog that the hand-checked "
             "list doesn't have yet. They're already on the print pages; the product each came in still needs "
             "checking.", "Maintainers", download="mega-auto-promos-csv"),
        Item("Scarlet & Violet source list", "The hand-checked list every Scarlet & Violet placeholder is built "
             "from.", "Maintainers", download="sv-source-csv"),
        Item("Prize Pack Series 9", "Prize Pack Series 9 cards that also get placeholders, with standard or "
             "foil as on the official list.", "Maintainers", download="prize-pack-series9-csv"),
        Item("Prize Pack Series 8", "Prize Pack Series 8 cards that also get placeholders.", "Maintainers",
             download="prize-pack-series8-csv"),
        Item("Prize Pack Series 7", "Prize Pack Series 7 cards that also get placeholders.", "Maintainers",
             download="prize-pack-series7-csv"),
        Item("Mega Evolution review list", "Checklist rows that may need fixing: missing cards, or cards filed "
             "under the wrong set.", "Maintainers", download="mega-review-queue"),
        Item("Scarlet & Violet review list", "The same review list for Scarlet & Violet.", "Maintainers",
             download="sv-review-queue"),
    )),
)
