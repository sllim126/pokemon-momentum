"""Collector Hub (`/collector-hub`, templates/collector_hub.html): the free master-set
resources, explained, with links to open or download each one.

Moved from market.poke6s.com on 2026-11-20 (docs/market_inventory.md, option B); the market's
/collector-hub now redirects here. The page used to be built in the browser from the market's
/collector-manifest; here it is plain server-rendered HTML from the list below.

The resources themselves (checklist sites, placeholder print pages, CSVs) are generated files
under "TCG Placeholders/", served by the store since the Placeholder Library moved too
(app/placeholder_library.py: `resource_url` and `download_url` live there). Only the
"Sets by release date" link still goes to the market.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Resource:
    title: str
    text: str
    tags: tuple[str, ...]
    open_label: str
    asset: str                       # "<bucket>/<path>" (placeholder_library.BUCKETS)
    download: tuple[str, str] | None = None   # (placeholder_library.DOWNLOADS id, label)
    extra: tuple[str, str] | None = None      # (market path, label)


@dataclass(frozen=True)
class Section:
    title: str
    text: str
    resources: tuple[Resource, ...]


# In the order a collector uses them. Copy carried over from the market page (2026-11-20).
SECTIONS = (
    Section("Track your collection", "Checklists for every set, with the cards you own ticked off.", (
        Resource("Scarlet & Violet checklists",
                 "One page per Scarlet & Violet set listing every card and printing. Tick off what you own; "
                 "your ticks stay in this browser. Each set page can be printed or saved as a spreadsheet.",
                 ("SV", "Checklist", "Interactive"), "Open checklists", "checklists-sv/index.html",
                 download=("sv-checklist-csv", "All SV rows (CSV)")),
        Resource("Mega Evolution checklists",
                 "The same checklists for the Mega Evolution era, including promos and the Prize Pack cards "
                 "that carry over.",
                 ("Mega", "Checklist", "Interactive"), "Open checklists", "checklists-mega/index.html",
                 download=("mega-checklist-csv", "All Mega rows (CSV)")),
    )),
    Section("Print binder placeholders",
            "Stand-in cards for the cards you're still missing, ready to print and cut.", (
        Resource("All placeholders (start here)",
                 "Printable placeholder cards for Scarlet & Violet, Mega Evolution and Prize Pack cards in one "
                 "place. Choose cards by set release or by card code, then print.",
                 ("Combined", "Print", "Binder"), "Open print page", "print-combined/index.html",
                 download=("combined-printable-csv", "Card list (CSV)")),
        Resource("Mega Evolution placeholders",
                 "Only the Mega Evolution era, including promos, grouped by set release.",
                 ("Mega", "Print", "Binder"), "Open print page", "print-mega/index.html"),
        Resource("Scarlet & Violet placeholders",
                 "Only the Scarlet & Violet era, grouped by set release and card code.",
                 ("SV", "Print", "Binder"), "Open print page", "print-sv/index.html"),
    )),
    Section("Reference", "Background for planning a master set.", (
        Resource("Set release date notes",
                 "When each set, promo block and special release came out. For release dates alongside "
                 "prices, sort the market's Sets list by release date.",
                 ("Reference", "Dates", "Planning"), "Read the notes", "docs/set_release_dates.md",
                 extra=("/?list=sets", "Sets by release date")),
    )),
)

