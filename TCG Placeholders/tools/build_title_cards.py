#!/usr/bin/env python3
"""Build printable "grandmaster" title cards: one card-sized label for every card in a set.

The owner made the first sheet by hand in Publisher (title_cards/journey_together_promo_title_cards.pdf:
Journey Together's promos only). This script keeps that look and extends it to every printing a
grandmaster binder holds, for every Scarlet & Violet set (and, with --era mega, the Mega sets):

- the main set, from the TCGCSV catalogue (data/tcgcsv_catalogue/3/<group>/products.json);
- its reverse holos, from the price file's subtypes (a product with a "Reverse Holofoil" price
  has a reverse printing), and the Poké Ball / Master Ball pattern cards, which TCGplayer lists
  as their own products;
- the promos and special versions from the checklist CSVs (checklists_sv/csv/<set>.csv), which
  build_checklists.py already makes from placeholders.csv: stamps, Black Star promos, Prize
  Pack, staff and product exclusives;
- hand-written extras from title_cards/title_card_extras.csv (cards in the owner's own sheet
  that no data source has, such as tournament stamps or Japanese file-set promos).

Layout (copied from the owner's PDF): US Letter, a 3 x 3 grid of 178.6 x 249.5 pt boxes (about
2.48 x 3.47 in, so one fits in a card sleeve) with a thin grey outline, text centred in Agency FB
Bold 24 pt, shrinking when a card's text doesn't fit. Each card reads:

    Card name
    (blank)
    #034/159
    Reverse Holo                <- the printing, when it isn't the plain one
    (Where it comes from)       <- promos only

Cards run in binder order: by number, each number's printings together (plain, reverse holo,
Poké Ball, Master Ball, then stamped and promo versions), then the set's Black Star promos, then
extras with no number.

The font: Agency FB is a Microsoft font, so it is not in this public repository. Put the
owner's copy (AGENCYB.TTF from C:\\Windows\\Fonts) at "TCG Placeholders/fonts/AGENCYB.TTF"
(git-ignored), or pass --font. Any other TrueType font works for a preview.

Run it in the market container, which has matplotlib and pyarrow:

    docker-compose exec -T pokemon-momentum python "TCG Placeholders/tools/build_title_cards.py" --set JTG
    docker-compose exec -T pokemon-momentum python "TCG Placeholders/tools/build_title_cards.py" --era sv

Output: title_cards/<set>_grandmaster_title_cards.pdf, plus title_cards/csv/<set>.csv listing
every card's text for checking.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

PLACEHOLDERS = Path(__file__).resolve().parents[1]
REPO = PLACEHOLDERS.parent
CATALOGUE = REPO / "data" / "tcgcsv_catalogue" / "3"
PARQUET = REPO / "data" / "parquet"
OUT_DIR = PLACEHOLDERS / "title_cards"
EXTRAS = OUT_DIR / "title_card_extras.csv"
DEFAULT_FONT = PLACEHOLDERS / "fonts" / "AGENCYB.TTF"

# The main sets of each era, in release order: checklist code -> TCGCSV group abbreviation.
# (The checklist folder and its CSVs use the lower-case code.)
ERAS = {
    "sv": {
        "folder": "checklists_sv",
        "sets": ["SVI", "PAL", "OBF", "MEW", "PAR", "PAF", "TEF", "TWM", "SFA", "SCR", "SSP",
                 "PRE", "JTG", "DRI", "BLK", "WHT"],
    },
    "mega": {
        "folder": "checklists_mega",
        "sets": ["MEG", "PFL", "ASC", "POR", "CRI", "PBL", "DLR"],
    },
}

# Page geometry in points, measured from the owner's PDF (612 x 792 = US Letter).
PAGE_W, PAGE_H = 612.0, 792.0
GRID_X, GRID_Y = 36.0, 18.66
CELL_W, CELL_H = 178.58, 249.62
COLS, ROWS = 3, 3
LINE_GREY = (0.62, 0.62, 0.62)
LINE_WIDTH = 0.25
FONT_SIZES = (24, 22, 20, 18, 16, 14)   # the owner's 24 pt, smaller only when needed
LINE_GAP = 1.19                         # line pitch / font size in the owner's file
TEXT_W = CELL_W - 16                    # leave a little margin inside the outline
TEXT_H = CELL_H - 24

# Printing order inside one card number.
PRINTING_ORDER = {"": 0, "Reverse Holo": 1, "Poké Ball Pattern": 2, "Master Ball Pattern": 3}

# Regions that need no mention (the English cards everyone gets).
HOME_REGIONS = {"", "US/UK", "US", "US/Canada", "UK"}


@dataclass
class TitleCard:
    """One label: its lines (an empty string is a blank line) and where it sorts."""

    lines: list[str]
    sort: tuple = field(default=())
    source: str = ""   # main / checklist / extra, for the review CSV


# --- data sources -------------------------------------------------------------------------------

def load_json(path: Path):
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("results", data) if isinstance(data, dict) else data


def group_for(abbreviation: str) -> dict:
    """The TCGCSV group (set) with this abbreviation. Promo and energy groups share some
    abbreviations with other products, so only groups with a name prefix like "SV09:" or "SV:"
    or "ME01:" are considered first."""
    groups = [g for g in load_json(CATALOGUE / "groups.json") if g.get("abbreviation") == abbreviation]
    if not groups:
        raise SystemExit(f"No TCGCSV group with abbreviation {abbreviation!r}")
    groups.sort(key=lambda g: (not re.match(r"^(SV|ME)\w*:", g["name"]), g["publishedOn"]))
    return groups[0]


def latest_subtypes(group_id: int) -> dict[int, set[str]]:
    """productId -> the price subtypes it has (Normal, Holofoil, Reverse Holofoil), from the
    newest daily price file."""
    import pyarrow.parquet as pq

    days = sorted(p for p in PARQUET.glob("date=*") if (p / "category-3.parquet").exists())
    if not days:
        raise SystemExit(f"No price files under {PARQUET}")
    # ParquetFile, not read_table: the folder name (date=...) would be read as a partition column
    # that clashes with the file's own date column.
    table = pq.ParquetFile(days[-1] / "category-3.parquet").read(
        columns=["groupId", "productId", "subTypeName"])
    out: dict[int, set[str]] = {}
    for gid, pid, sub in zip(table.column("groupId").to_pylist(), table.column("productId").to_pylist(),
                             table.column("subTypeName").to_pylist()):
        if gid == group_id:
            out.setdefault(int(pid), set()).add(sub or "")
    return out


def ext(product: dict, name: str) -> str:
    return next((e["value"] for e in product.get("extendedData", []) if e["name"] == name), "")


def split_name(name: str) -> tuple[str, str]:
    """"Klink (Master Ball Pattern)" -> ("Klink", "Master Ball Pattern"); "Mew ex - 205/165 (151
    Metal Card)" -> ("Mew ex", "151 Metal Card"). TCGplayer adds the number to some names."""
    tag = ""
    m = re.search(r"\s*\(([^()]*)\)\s*$", name)
    if m:
        tag, name = m.group(1).strip(), name[: m.start()]
    name = re.sub(r"\s+-\s+\S*\d+\S*$", "", name).strip()
    return name, tag


def printing_label(tag: str) -> str:
    """TCGplayer's tag in our wording."""
    tag = tag.replace("Poke Ball", "Poké Ball")
    if tag.lower() in {"reverse holofoil", "reverse holo"}:
        return "Reverse Holo"
    return tag


def number_key(number: str) -> tuple:
    """Sort key for "034/159", "TG05/TG30", "GG12": numbers sort numerically after any letters."""
    head = number.split("/")[0]
    m = re.match(r"^([A-Za-z-]*)(\d+)", head)
    if not m:
        return (1, head, 0)
    return (0 if not m.group(1) else 1, m.group(1).upper(), int(m.group(2)))


def sort_key(section: int, nk: tuple, order: int, text: str) -> tuple:
    """One shape for every card's sort key: section (0 = numbered in the set, 1 = promos with
    another prefix such as SVP, 2 = unnumbered), the number key, the printing order, then text."""
    return (section, *nk, order, text)


def main_set_cards(code: str) -> tuple[list[TitleCard], str]:
    """Every main-set printing. Returns the cards and the set's printed total (e.g. "159")."""
    group = group_for(code)
    products = load_json(CATALOGUE / str(group["groupId"]) / "products.json")
    subtypes = latest_subtypes(group["groupId"])
    cards: list[TitleCard] = []
    seen: set[tuple[str, str]] = set()
    totals: dict[str, int] = {}
    for p in products:
        number = ext(p, "Number")
        if not number:
            continue   # sealed product
        name, tag = split_name(p["name"])
        label = printing_label(tag)
        if "/" in number:
            totals[number.split("/")[1]] = totals.get(number.split("/")[1], 0) + 1
        printings = [label]
        # A plain product with a reverse-holo price has a reverse printing too.
        if not tag and "Reverse Holofoil" in subtypes.get(int(p["productId"]), set()):
            printings.append("Reverse Holo")
        for printing in printings:
            if (number, printing) in seen:
                continue
            seen.add((number, printing))
            lines = [name, "", f"#{number}"]
            if printing:
                lines.append(printing)
            order = PRINTING_ORDER.get(printing, 4)
            cards.append(TitleCard(lines, sort_key(0, number_key(number), order, printing), "main"))
    total = max(totals, key=totals.get) if totals else ""
    return cards, total


def source_text(purchase: str) -> str:
    """The checklist's "Purchase Options" in a few words: the product after "<set> - ", and the
    Prize Pack series folded into one line ("Prize Pack Series 7, 8")."""
    words = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5, "Six": 6, "Seven": 7,
             "Eight": 8, "Nine": 9, "Ten": 10, "Eleven": 11, "Twelve": 12}
    parts, series = [], []
    for option in [o.strip() for o in purchase.split("|") if o.strip()]:
        product = option.split(" - ", 1)[1] if " - " in option else option
        m = re.search(r"Prize Pack Series (\w+)", product)
        if m:
            series.append(str(words.get(m.group(1), m.group(1))))
            continue
        product = product.replace("Pokemon", "Pokémon")
        if product not in parts:
            parts.append(product)
    if series:
        parts.append("Prize Pack Series " + ", ".join(sorted(set(series), key=lambda s: (len(s), s))))
    return "; ".join(parts)


def checklist_cards(code: str, era: str, total: str) -> list[TitleCard]:
    """Promos and special versions from the generated checklist CSV for this set."""
    path = PLACEHOLDERS / ERAS[era]["folder"] / "csv" / f"{code.lower()}.csv"
    if not path.exists():
        return []
    cards = []
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            raw = row["Card Number"].strip()
            m = re.match(r"^([A-Z]+)(\d+)$", raw)
            if m and m.group(1) == code and total:
                number, nk = f"{int(m.group(2)):03d}/{total}", number_key(m.group(2))
                section = 0
            elif m:
                number, nk, section = f"{m.group(1)} {m.group(2)}", (0, m.group(1), int(m.group(2))), 1
            else:
                number, nk, section = raw, (0, raw, 0), 1
            lines = [row["Card Name"].strip(), "", f"#{number}" if m else number]
            variant = row["Variant"].strip()
            if variant:
                lines.append(variant.replace("Pokemon", "Pokémon"))
            where = source_text(row.get("Purchase Options", ""))
            region = row.get("Region", "").strip()
            if region not in HOME_REGIONS:
                where = f"{where}, {region}" if where else region
            if where:
                lines.append(f"({where})")
            cards.append(TitleCard(lines, sort_key(section, nk, 5, variant), "checklist"))
    return cards


def extra_cards(code: str) -> list[TitleCard]:
    """Hand-written cards for this set from title_card_extras.csv. "Lines" separates lines with
    "|"; an empty part is a blank line. "Sort Number" places the card: a set number like
    "117" sits with that card, "SVP 200" among the promos, blank goes last."""
    if not EXTRAS.exists():
        return []
    cards = []
    with EXTRAS.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["Set Code"].strip().upper() != code:
                continue
            lines = row["Lines"].split("|")
            key = row["Sort Number"].strip()
            m = re.match(r"^([A-Z]+)\s*(\d+)$", key)
            if key.isdigit():
                section, nk = 0, number_key(key)
            elif m:
                section, nk = 1, (0, m.group(1), int(m.group(2)))
            else:
                section, nk = 2, (0, "", 0)
            cards.append(TitleCard(lines, sort_key(section, nk, 6, row["Lines"]), "extra"))
    return cards


def set_cards(code: str, era: str) -> list[TitleCard]:
    main, total = main_set_cards(code)
    promos = checklist_cards(code, era, total)
    # A special product TCGplayer lists in the main set (151's "Mew ex (151 Metal Card)") can also
    # be a checklist row ("Metal Card"): keep the checklist's card, which says where it comes from.
    promo_versions = {(c.lines[2], c.lines[3].lower()) for c in promos if len(c.lines) > 3}
    main = [c for c in main
            if len(c.lines) < 4 or c.lines[3] in PRINTING_ORDER
            or not any(num == c.lines[2] and v in c.lines[3].lower() for num, v in promo_versions)]
    cards = main + promos + extra_cards(code)
    cards.sort(key=lambda c: c.sort)
    return cards


# --- layout -------------------------------------------------------------------------------------

class Measure:
    """Text widths from the font's own advance widths (no renderer needed)."""

    def __init__(self, font_path: Path):
        from fontTools.ttLib import TTFont

        font = TTFont(str(font_path))
        self.cmap = font.getBestCmap()
        self.hmtx = font["hmtx"]
        self.upm = font["head"].unitsPerEm
        self.missing: set[str] = set()

    def width(self, text: str, size: float) -> float:
        total = 0
        for ch in text:
            glyph = self.cmap.get(ord(ch))
            if glyph is None:
                self.missing.add(ch)
                glyph = self.cmap.get(ord("?"))
            total += self.hmtx[glyph][0] if glyph else 0
        return total * size / self.upm


def wrap(line: str, size: float, measure: Measure) -> list[str]:
    """Break one line into lines that fit the card's width, at spaces."""
    if measure.width(line, size) <= TEXT_W:
        return [line]
    out, current = [], ""
    for word in line.split(" "):
        trial = f"{current} {word}".strip()
        if current and measure.width(trial, size) > TEXT_W:
            out.append(current)
            current = word
        else:
            current = trial
    out.append(current)
    return out


def fit(lines: list[str], measure: Measure) -> tuple[float, list[str]]:
    """The largest size (24 pt first) at which the wrapped text fits the card."""
    for size in FONT_SIZES:
        wrapped = [part for line in lines for part in (wrap(line, size, measure) if line else [""])]
        widest = max(measure.width(w, size) for w in wrapped)
        if widest <= TEXT_W and len(wrapped) * size * LINE_GAP <= TEXT_H:
            return size, wrapped
    return FONT_SIZES[-1], wrapped


def write_pdf(cards: list[TitleCard], font_path: Path, out: Path) -> Measure:
    import matplotlib

    matplotlib.use("Agg")
    matplotlib.rcParams["pdf.fonttype"] = 42     # embed the TrueType font (as the owner's file does)
    from matplotlib.backends.backend_pdf import PdfPages
    from matplotlib.font_manager import FontProperties
    from matplotlib.patches import Rectangle
    import matplotlib.pyplot as plt

    measure = Measure(font_path)
    per_page = COLS * ROWS
    out.parent.mkdir(parents=True, exist_ok=True)
    with PdfPages(out, metadata={"Title": out.stem.replace("_", " ").title(), "Author": "Poke6s",
                                 "Creator": "build_title_cards.py"}) as pdf:
        for start in range(0, len(cards), per_page):
            fig = plt.figure(figsize=(PAGE_W / 72, PAGE_H / 72))
            ax = fig.add_axes((0, 0, 1, 1))
            ax.set_xlim(0, PAGE_W)
            ax.set_ylim(PAGE_H, 0)          # y grows downward, like the PDF measurements
            ax.axis("off")
            for i, card in enumerate(cards[start:start + per_page]):
                x = GRID_X + (i % COLS) * CELL_W
                y = GRID_Y + (i // COLS) * CELL_H
                ax.add_patch(Rectangle((x, y), CELL_W, CELL_H, fill=False,
                                       edgecolor=LINE_GREY, linewidth=LINE_WIDTH))
                size, lines = fit(card.lines, measure)
                prop = FontProperties(fname=str(font_path), size=size)
                pitch = size * LINE_GAP
                top = y + CELL_H / 2 - pitch * (len(lines) - 1) / 2
                for n, text in enumerate(lines):
                    if text:
                        ax.text(x + CELL_W / 2, top + n * pitch, text, fontproperties=prop,
                                ha="center", va="center", color="black")
            pdf.savefig(fig)
            plt.close(fig)
    return measure


def write_listing(cards: list[TitleCard], out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["Page", "Position", "Source", "Text"])
        for i, card in enumerate(cards):
            w.writerow([i // 9 + 1, i % 9 + 1, card.source, " | ".join(card.lines)])


def slug(code: str) -> str:
    name = group_for(code)["name"]
    name = re.sub(r"^[A-Z0-9]+:\s*", "", name)
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--set", action="append", default=[], help="set code, e.g. JTG (repeatable)")
    ap.add_argument("--era", choices=sorted(ERAS), help="every main set of an era")
    ap.add_argument("--font", type=Path, default=DEFAULT_FONT, help="TrueType font file")
    ap.add_argument("--out", type=Path, default=OUT_DIR, help="output folder")
    args = ap.parse_args(argv)

    if not args.font.exists():
        print(f"Font not found: {args.font}\nCopy AGENCYB.TTF (Agency FB Bold) there, or pass --font.",
              file=sys.stderr)
        return 1
    jobs = [(c.upper(), era) for c in args.set
            for era in [next((e for e, v in ERAS.items() if c.upper() in v["sets"]), "sv")]]
    if args.era:
        jobs += [(c, args.era) for c in ERAS[args.era]["sets"]]
    if not jobs:
        ap.error("give --set or --era")

    for code, era in jobs:
        cards = set_cards(code, era)
        name = slug(code)
        pdf = args.out / f"{name}_grandmaster_title_cards.pdf"
        measure = write_pdf(cards, args.font, pdf)
        write_listing(cards, args.out / "csv" / f"{code.lower()}.csv")
        counts = {s: sum(c.source == s for c in cards) for s in ("main", "checklist", "extra")}
        note = f"  missing glyphs: {''.join(sorted(measure.missing))}" if measure.missing else ""
        print(f"{code}: {len(cards)} cards, {-(-len(cards) // 9)} pages ({counts['main']} main set, "
              f"{counts['checklist']} promo/special, {counts['extra']} extra) -> {pdf.name}{note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
