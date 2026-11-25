"""Charts and numbers for the blog post "Why a Grade 1 Pokémon Card Can Sell for More Than a
Grade 5" (2026-12-01).

Data: one day of PriceCharting's Pokémon CSV (data/pricecharting/raw/pokemon-cards-<date>.csv.gz).
Its grades cover any grading company, not only PSA. Column meanings (PriceCharting's docs, and
scripts/common/pricecharting.py for 7 and up): condition-9 = Grade 1, condition-10 = Grade 2,
condition-13..16 = Grades 3-6, cib = 7, new = 8, graded = 9, manual-only = PSA 10. The post's
sample is every card with a positive Grade 1 and Grade 5 price.

Graded prices must link back to PriceCharting wherever they are shown (licence, 2026-10-09):
each chart's note credits PriceCharting, and the post links every example card's page.

Chart 1: price by grade (1-9) for three example cards, one panel each with its own dollar
scale (their prices differ too much to share an axis). Chart 2: share of cards whose Grade 1
is above the Grade 5, by the card's Grade 5 price. Colours: the storefront's dark theme, with
the validated #4096f0 / #d95926 pair from master_sets_part2_charts.py.

    mkdir -p .chart_tmp
    docker-compose exec -T pokemon-momentum python scripts/blog/grade1_charts.py

PNGs land in .chart_tmp/; store them with storefront/app/images.store_original
(docs/blog_guide.md "Pictures and charts"), then delete .chart_tmp.
"""
import collections
import csv
import gzip
import statistics as st

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DATE = "2026-11-25"
SOURCE = f"/app/data/pricecharting/raw/pokemon-cards-{DATE}.csv.gz"
OUT = "/app/.chart_tmp/"
BG, TEXT, MUTED, FAINT, GRID = "#0e1828", "#eaf1ff", "#93a6c8", "#6f82a6", "#1d2a40"
LINE, HIGHLIGHT = "#4096f0", "#d95926"
GRADES = {1: "condition-9-price", 2: "condition-10-price", 3: "condition-13-price",
          4: "condition-14-price", 5: "condition-15-price", 6: "condition-16-price",
          7: "cib-price", 8: "new-price", 9: "graded-price"}
EXAMPLES = (  # PriceCharting id, label in the chart
    ("11069001", "Mega Charizard X ex #125\nPhantasmal Flames"),
    ("5834844", "Pikachu with Grey Felt Hat #85\nPromo"),
    ("630417", "Charizard #4\nBase Set"),
)
plt.rcParams.update({"font.family": "DejaVu Sans", "text.color": TEXT, "axes.labelcolor": MUTED,
                     "xtick.color": MUTED, "ytick.color": MUTED, "font.size": 13})


def money(value: str) -> float | None:
    text = (value or "").replace("$", "").replace(",", "").strip()
    try:
        number = float(text)
    except ValueError:
        return None
    return number if number > 0 else None


cards = {}
with gzip.open(SOURCE, "rt", encoding="utf-8") as handle:
    for row in csv.DictReader(handle):
        prices = {g: money(row[c]) for g, c in GRADES.items()}
        prices["raw"] = money(row["loose-price"])
        cards[row["id"]] = prices
both = [p for p in cards.values() if p[1] and p[5]]
print(DATE, len(both), "cards with Grade 1 and Grade 5")
print("Grade 1 > Grade 5:", f"{100 * sum(p[1] > p[5] for p in both) / len(both):.0f}%",
      "median ratio", round(st.median(p[1] / p[5] for p in both), 2))
full = [p for p in both if all(p[g] for g in range(1, 10))]
cheapest = collections.Counter(min(range(1, 10), key=lambda g: p[g]) for p in full)
print("cheapest grade, cards with all of 1-9:", len(full), cheapest.most_common(3))

# ---- Chart 1: price by grade, three cards ----
fig, axes = plt.subplots(1, 3, figsize=(13, 5.6), dpi=200)
fig.patch.set_facecolor(BG)
fig.subplots_adjust(left=0.07, right=0.98, top=0.83, bottom=0.2, wspace=0.32)
for ax, (pc_id, label) in zip(axes, EXAMPLES):
    p = cards[pc_id]
    ys = [p[g] for g in range(1, 10)]
    ax.set_facecolor(BG)
    for side in ax.spines.values():
        side.set_visible(False)
    ax.tick_params(length=0)
    ax.grid(axis="y", color=GRID, linewidth=1)
    ax.set_axisbelow(True)
    ax.plot(range(1, 10), ys, color=LINE, linewidth=2.2, marker="o", markersize=8,
            markeredgecolor=BG, markeredgewidth=2, zorder=3)
    ax.plot([1], [ys[0]], marker="o", markersize=12, color=HIGHLIGHT, markeredgecolor=BG,
            markeredgewidth=2, zorder=4)
    ax.annotate(f"Grade 1\n${ys[0]:,.0f}", (1, ys[0]), xytext=(10, 4), textcoords="offset points",
                fontsize=12, color=TEXT, fontweight="bold", va="bottom")
    ax.annotate(f"Grade 9  ${ys[-1]:,.0f}", (9, ys[-1]), xytext=(-6, 10), textcoords="offset points",
                fontsize=11, color=MUTED, ha="right", va="bottom")
    ax.set_ylim(0, max(ys) * 1.28)
    ax.set_xticks(range(1, 10))
    ax.set_xlabel("Grade")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"${v:,.0f}"))
    ax.set_title(label, fontsize=13, color=TEXT, loc="left", pad=12)  # names the panel, not the chart
    print(label.replace("\n", ", "), {g: p[g] for g in range(1, 10)}, "raw", p["raw"])
fig.text(0.02, 0.03, f"Price by grade, any grading company. PriceCharting, {DATE}.  poke6s.com",
         fontsize=10.5, color=FAINT)
fig.savefig(OUT + "grade1_chart1.png", facecolor=BG)
plt.close(fig)

# ---- Chart 2: how often Grade 1 beats Grade 5, by the Grade 5 price ----
bands = (("Under $20", 0, 20), ("$20 to $100", 20, 100), ("$100 to $500", 100, 500), ("Over $500", 500, 1e12))
rows = []
for label, lo, hi in bands:
    group = [p for p in both if lo <= p[5] < hi]
    share = 100 * sum(p[1] > p[5] for p in group) / len(group)
    rows.append((label, len(group), share, st.median(p[1] / p[5] for p in group)))
    print(rows[-1])
fig, ax = plt.subplots(figsize=(12, 5.6), dpi=200)
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)
fig.subplots_adjust(left=0.2, right=0.97, top=0.95, bottom=0.2)
for side in ax.spines.values():
    side.set_visible(False)
ax.tick_params(length=0)
ax.grid(axis="x", color=GRID, linewidth=1)
ax.set_axisbelow(True)
for i, (label, n, share, ratio) in enumerate(rows):
    ax.barh(i, share, height=0.56, color=HIGHLIGHT if share >= 50 else LINE, edgecolor=BG, linewidth=2)
    # "97%  ·  5.4×": the share, then the typical (median) Grade 1 price as a multiple of the Grade 5.
    ax.text(share + 1.2, i, f"{share:.0f}%  ·  {ratio:.1f}×", va="center", fontsize=13, color=TEXT)
ax.axvline(50, color=FAINT, linewidth=1.2, linestyle=(0, (2, 3)))
ax.text(50.6, 3.42, "half", color=FAINT, fontsize=11, va="center")
ax.set_yticks(range(len(rows)))
# "\$": a bare "$" pair makes matplotlib typeset the text between them as maths.
ax.set_yticklabels([f"{label}\n({n:,} cards)".replace("$", r"\$") for label, n, *_ in rows],
                   color=TEXT, linespacing=1.5)
ax.set_ylabel("Grade 5 price", color=MUTED, labelpad=12)
ax.invert_yaxis()
ax.set_xlim(0, 118)
ax.set_xticks([0, 25, 50, 75, 100])
ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
fig.text(0.02, 0.03, f"Bar: share of cards whose Grade 1 is above their Grade 5. ×: typical Grade 1 price as a multiple "
         f"of the Grade 5.\n{len(both):,} cards, any grading company. PriceCharting, {DATE}.  poke6s.com",
         fontsize=10.5, color=FAINT)
fig.savefig(OUT + "grade1_chart2.png", facecolor=BG)
plt.close(fig)
print("ok")
