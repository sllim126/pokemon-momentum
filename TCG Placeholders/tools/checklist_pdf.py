"""Printable set checklists: a PDF with a tick box for every printing in a set (owner, 2026-11-26:
"someone downloading a checklist isn't wanting a .csv").

Made by build_title_cards.py from the same card list as the title cards, so the two always agree.
One page layout, US Letter:

- a navy header band with the set's name in Agency FB (the title cards' font), the card count,
  and lines to write your name and how many you own;
- "Main set": one row per card number with a box under each printing it comes in (the card,
  Reverse Holo, Poké Ball, Master Ball...; a dash where that printing doesn't exist). Sets with
  few printings fit two or three of these tables side by side;
- "Promos, stamps and special versions": one row per card with its version and where it came from;
- a footer with poke6s.com and the page number.

Body text is DejaVu Sans, which ships with matplotlib (free to embed and share).
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Callable, Sequence

PAGE_W, PAGE_H = 612.0, 792.0
MARGIN = 36.0
BOTTOM = PAGE_H - 40.0          # content stops here; the footer sits below
NAVY = "#08111d"                # the store's background colour
ACCENT = "#4da3ff"              # the store's accent blue
INK = "#1b2433"
MUTED = "#6b7a93"
ZEBRA = "#eef3fa"
RULE = "#c9d3e3"

ROW_H = 12.5
BODY = 7.6
HEAD = 6.4
BOX = 7.0
PANEL_GAP = 14.0

# Column headings for the printings, short enough for a narrow column.
SHORT = {"": "Card", "Reverse Holo": "Reverse", "Poké Ball Pattern": "Poké Ball",
         "Master Ball Pattern": "Master Ball", "Energy Symbol Pattern": "Energy Symbol"}


def write_checklist(set_name: str, cards: Sequence, out: Path, heading_font: Path,
                    printing_order: dict[str, int]) -> int:
    """Draw the checklist for one set; returns the page count. `cards` are build_title_cards'
    TitleCards in binder order."""
    import matplotlib

    matplotlib.use("Agg")
    matplotlib.rcParams["pdf.fonttype"] = 42
    from matplotlib.backends.backend_pdf import PdfPages
    from matplotlib.font_manager import FontProperties
    from matplotlib.patches import Rectangle
    import matplotlib.pyplot as plt

    data = Path(matplotlib.get_data_path()) / "fonts" / "ttf"
    body_path, bold_path = data / "DejaVuSans.ttf", data / "DejaVuSans-Bold.ttf"
    from build_title_cards import Measure   # same width measuring as the title cards

    m_body, m_bold = Measure(body_path), Measure(bold_path)

    def font(path: Path, size: float) -> FontProperties:
        return FontProperties(fname=str(path), size=size)

    # --- the two lists ------------------------------------------------------------------------
    rows: dict[str, dict] = {}
    for c in cards:
        if c.in_grid:
            row = rows.setdefault(c.number, {"number": c.number, "name": c.name, "has": set()})
            row["has"].add(c.printing)
    grid = list(rows.values())
    printings = sorted({p for r in grid for p in r["has"]},
                       key=lambda p: (printing_order.get(p, 4), p))
    specials = [c for c in cards if not c.in_grid]
    total = len(cards)

    # --- grid geometry ------------------------------------------------------------------------
    num_w = max([m_body.width(r["number"], BODY) for r in grid] + [20]) + 8
    name_w = min(max([m_body.width(r["name"], BODY) for r in grid] + [60]) + 8, 150)
    col_w = 34.0 if len(printings) <= 4 else 30.0
    panel_w = num_w + name_w + col_w * len(printings)
    avail = PAGE_W - 2 * MARGIN
    per_row = max(1, int((avail + PANEL_GAP) // (panel_w + PANEL_GAP))) if grid else 1
    if per_row > 1:   # spread the spare width over the name column
        name_w += (avail - per_row * panel_w - (per_row - 1) * PANEL_GAP) / per_row
        panel_w = num_w + name_w + col_w * len(printings)

    # Special rows: box, number, name, version, source (wrapped to fit).
    s_num_w = max([m_body.width(c.number, BODY) for c in specials] + [24]) + 8
    s_name_w = min(max([m_body.width(c.name, BODY) for c in specials] + [60]) + 8, 140)
    s_ver_w = min(max([m_body.width(c.version, BODY) for c in specials] + [50]) + 8, 140)
    s_where_w = avail - 16 - s_num_w - s_name_w - s_ver_w

    def wrap(text: str, width: float, measure, size: float) -> list[str]:
        words, lines, cur = text.split(), [], ""
        for w in words:
            trial = f"{cur} {w}".strip()
            if cur and measure.width(trial, size) > width:
                lines.append(cur)
                cur = w
            else:
                cur = trial
        return lines + ([cur] if cur else [])

    def clip(text: str, width: float, measure=m_body, size: float = BODY) -> str:
        if measure.width(text, size) <= width:
            return text
        while text and measure.width(text + "…", size) > width:
            text = text[:-1]
        return text.rstrip() + "…"

    special_rows = []
    for c in specials:
        ver = wrap(c.version, s_ver_w - 6, m_body, BODY) or [""]
        where = wrap(c.where, s_where_w - 4, m_body, BODY) or [""]
        special_rows.append((c, ver, where, max(len(ver), len(where), 1)))

    # --- drawing helpers (y grows downward) ---------------------------------------------------
    pages: list[list[Callable]] = []

    def new_page() -> float:
        pages.append([])
        first = len(pages) == 1
        pages[-1].append(lambda ax, n, first=first: header(ax, first, n))
        return 112.0 if first else 58.0

    def draw(fn: Callable) -> None:
        pages[-1].append(lambda ax, n, fn=fn: fn(ax))

    def text(ax, x, y, s, size=BODY, path=None, color=INK, ha="left", va="center"):
        ax.text(x, y, s, fontproperties=font(path or body_path, size), color=color, ha=ha, va=va)

    def tick_box(ax, cx, cy):
        ax.add_patch(Rectangle((cx - BOX / 2, cy - BOX / 2), BOX, BOX, fill=True, facecolor="white",
                               edgecolor="#3a4a66", linewidth=0.6))

    def header(ax, first: bool, page_no: int):
        if first:
            ax.add_patch(Rectangle((0, 0), PAGE_W, 78, color=NAVY))
            ax.add_patch(Rectangle((0, 78), PAGE_W, 3, color=ACCENT))
            text(ax, MARGIN, 34, set_name, 30, heading_font, "white")
            text(ax, MARGIN, 60, f"Grandmaster checklist  ·  {total} cards", 9, body_path, "#b9c8e2")
            text(ax, PAGE_W - MARGIN, 34, "POKE6S", 20, heading_font, ACCENT, ha="right")
            text(ax, PAGE_W - MARGIN, 58, "poke6s.com", 8, body_path, "#b9c8e2", ha="right")
            text(ax, MARGIN, 97, "Name", 8, body_path, MUTED)
            ax.plot([MARGIN + 28, MARGIN + 220], [101, 101], color=RULE, lw=0.8)
            of_total = f"/ {total}"
            right = PAGE_W - MARGIN
            text(ax, right, 97, of_total, 8, body_path, MUTED, ha="right")
            line_end = right - m_body.width(of_total, 8) - 4
            ax.plot([line_end - 60, line_end], [101, 101], color=RULE, lw=0.8)
            text(ax, line_end - 64, 97, "Owned", 8, body_path, MUTED, ha="right")
        else:
            ax.add_patch(Rectangle((0, 0), PAGE_W, 34, color=NAVY))
            ax.add_patch(Rectangle((0, 34), PAGE_W, 2, color=ACCENT))
            text(ax, MARGIN, 18, set_name, 16, heading_font, "white")
            text(ax, PAGE_W - MARGIN, 18, "Grandmaster checklist", 8, body_path, "#b9c8e2", ha="right")
        text(ax, MARGIN, PAGE_H - 22, "poke6s.com  ·  Free to print for your own collection", 6.5,
             body_path, MUTED)
        text(ax, PAGE_W - MARGIN, PAGE_H - 22, f"Page {page_no} of {len(pages)}", 6.5, body_path, MUTED,
             ha="right")

    def section_title(y: float, title: str, note: str) -> float:
        def fn(ax):
            text(ax, MARGIN, y + 9, title, 15, heading_font, NAVY)
            text(ax, MARGIN + m_heading(title) + 8, y + 10, note, 7.5, body_path, MUTED)
        draw(fn)
        return y + 24

    m_head_font = Measure(heading_font)

    def m_heading(title: str) -> float:
        return m_head_font.width(title, 15)

    # --- main-set grid ------------------------------------------------------------------------
    y = new_page()
    if grid:
        legend = "One row per card number, a box for each version it comes in. A dash: that version doesn't exist."
        y = section_title(y, "Main set", f"{len(grid)} cards  ·  {sum(len(r['has']) for r in grid)} versions")
        draw(lambda ax, yy=y: text(ax, MARGIN, yy - 2, legend, 6.8, body_path, MUTED))
        y += 8
        head_h = 20.0
        i = 0
        while i < len(grid):
            fit_rows = int((BOTTOM - y - head_h) // ROW_H)
            if fit_rows < 4:
                y = new_page()
                continue
            left = len(grid) - i
            per_panel = min(fit_rows, math.ceil(left / per_row))
            for p in range(per_row):
                chunk = grid[i + p * per_panel: i + (p + 1) * per_panel]
                if chunk:
                    draw(lambda ax, chunk=chunk, x=MARGIN + p * (panel_w + PANEL_GAP), yy=y:
                         grid_panel(ax, x, yy, chunk))
            used = min(per_panel, left)
            i += per_panel * per_row
            y += head_h + used * ROW_H + 18
            if i < len(grid):
                y = new_page()

    # --- specials -----------------------------------------------------------------------------
    if specials:
        if BOTTOM - y < 80:
            y = new_page()
        y = section_title(y, "Promos, stamps and special versions", f"{len(specials)} cards")
        idx = 0
        while idx < len(special_rows):
            draw(lambda ax, yy=y: special_header(ax, yy))
            y += 16
            while idx < len(special_rows):
                c, ver, where, n = special_rows[idx]
                h = max(ROW_H, n * 9.5 + 4)
                if y + h > BOTTOM:
                    y = new_page()
                    break
                draw(lambda ax, yy=y, row=special_rows[idx], h=h, zebra=idx % 2:
                     special_row(ax, yy, row, h, zebra))
                y += h
                idx += 1

    # --- the drawing functions queued above (closures: they run at render time, below) --------
    def grid_panel(ax, x, y, chunk):
        ax.add_patch(Rectangle((x, y), panel_w, 20, color="#dde7f5"))
        text(ax, x + 4, y + 10, "No.", HEAD, bold_path, NAVY)
        text(ax, x + num_w, y + 10, "Card", HEAD, bold_path, NAVY)
        for k, p in enumerate(printings):
            cx = x + num_w + name_w + k * col_w + col_w / 2
            label = SHORT.get(p, p)
            parts = wrap(label, col_w - 2, m_bold, HEAD)
            for j, part in enumerate(parts[:2]):
                text(ax, cx, y + 10 + (j - (len(parts[:2]) - 1) / 2) * 7, part, HEAD, bold_path, NAVY, ha="center")
        yy = y + 20
        for r_i, r in enumerate(chunk):
            if r_i % 2:
                ax.add_patch(Rectangle((x, yy), panel_w, ROW_H, color=ZEBRA, lw=0))
            cy = yy + ROW_H / 2
            text(ax, x + 4, cy, r["number"], BODY, body_path, MUTED)
            text(ax, x + num_w, cy, clip(r["name"], name_w - 6), BODY)
            for k, p in enumerate(printings):
                cx = x + num_w + name_w + k * col_w + col_w / 2
                if p in r["has"]:
                    tick_box(ax, cx, cy)
                else:
                    text(ax, cx, cy, "–", BODY, body_path, "#b5c0d2", ha="center")
            yy += ROW_H
        ax.plot([x, x + panel_w], [yy, yy], color=RULE, lw=0.6)

    def special_header(ax, y):
        ax.add_patch(Rectangle((MARGIN, y), avail, 16, color="#dde7f5"))
        x = MARGIN + 16
        for label, w in (("No.", s_num_w), ("Card", s_name_w), ("Version", s_ver_w), ("Where it's from", s_where_w)):
            text(ax, x + 2, y + 8, label, HEAD, bold_path, NAVY)
            x += w

    def special_row(ax, y, row, h, zebra):
        c, ver, where, n = row
        if zebra:
            ax.add_patch(Rectangle((MARGIN, y), avail, h, color=ZEBRA, lw=0))
        top = y + 2 + 4.75
        tick_box(ax, MARGIN + 8, top)
        x = MARGIN + 16
        text(ax, x + 2, top, clip(c.number, s_num_w - 4), BODY, body_path, MUTED)
        x += s_num_w
        text(ax, x + 2, top, clip(c.name, s_name_w - 6), BODY)
        x += s_name_w
        for j, line in enumerate(ver):
            text(ax, x + 2, top + j * 9.5, line, BODY)
        x += s_ver_w
        for j, line in enumerate(where):
            text(ax, x + 2, top + j * 9.5, line, BODY, body_path, "#44516a")

    # --- render -------------------------------------------------------------------------------
    out.parent.mkdir(parents=True, exist_ok=True)
    with PdfPages(out, metadata={"Title": f"{set_name} grandmaster checklist", "Author": "Poke6s",
                                 "Creator": "build_title_cards.py", "CreationDate": None}) as pdf:
        for n, ops in enumerate(pages, 1):
            fig = plt.figure(figsize=(PAGE_W / 72, PAGE_H / 72))
            ax = fig.add_axes((0, 0, 1, 1))
            ax.set_xlim(0, PAGE_W)
            ax.set_ylim(PAGE_H, 0)
            ax.axis("off")
            for op in ops:
                op(ax, n)
            pdf.savefig(fig)
            plt.close(fig)
    return len(pages)
