"""Charts for the blog post "Is Pokémon Sealed Cheaper on Black Friday?" (2026-11-26).

Data: TCGplayer market prices from prices_db.duckdb, the same method as the post's numbers.
English sealed products (no card number, cases left out), priced $20 or more two weeks before
Black Friday, and priced (within two days) at every checkpoint the post uses: 4, 2 and 1 weeks
before, Black Friday, 3 days, 2, 4 and 6 weeks after. Each product's change is measured from its
price two weeks before. Chart 1 shows the share of products priced higher and lower each
day; chart 2 the median product by type.
Colours: the storefront's dark theme; 2024 #4096f0 and 2025 #d95926 are the validated pair from
master_sets_part2_charts.py.

Run inside the app container, which has matplotlib and the database:

    mkdir -p .chart_tmp
    docker-compose exec -T pokemon-momentum python scripts/blog/black_friday_charts.py

PNGs land in .chart_tmp/; add them to the store with storefront/app/images.store_original
(docs/blog_guide.md "Pictures and charts"), then delete .chart_tmp.
"""
import statistics as st
from datetime import date, timedelta

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

BG, TEXT, MUTED, FAINT, GRID = "#0e1828", "#eaf1ff", "#93a6c8", "#6f82a6", "#1d2a40"
Y2024, Y2025 = "#4096f0", "#d95926"
# Light to dark blue for "Black Friday -> two weeks later -> six weeks later" (one hue, in order).
STEPS = ["#24476f", "#3274bd", "#7ab8ff"]
OUT = "/app/.chart_tmp/"
BLACK_FRIDAYS = {2024: date(2024, 11, 29), 2025: date(2025, 11, 28)}
CHECKPOINTS = (-28, -14, -7, 0, 3, 14, 28, 42)
plt.rcParams.update({"font.family": "DejaVu Sans", "text.color": TEXT, "axes.labelcolor": MUTED,
                     "xtick.color": MUTED, "ytick.color": TEXT, "font.size": 13})


def kind(name: str) -> str | None:
    """Product type from its name, as in the post. Cases are left out (None)."""
    n = name.lower()
    if "case" in n:
        return None
    for key, label in (("booster box", "Booster boxes"), ("elite trainer box", "Elite Trainer Boxes"),
                       ("booster bundle", "Booster bundles")):
        if key in n:
            return label
    return "Other"


def load(con, bf: date) -> list[tuple[str, dict[int, float]]]:
    """[(type, {day offset from Black Friday: price})] for every product in the post's sample."""
    rows = con.execute(
        """select p.name, list({'d': datediff('day', ?::DATE, n.date), 'm': n.marketPrice})
           from pokemon_products p join pokemon_prices_named n using (productId)
           where (p.number is null or p.number = '') and n.marketPrice is not null
             and n.date between ?::DATE - INTERVAL 30 DAY and ?::DATE + INTERVAL 45 DAY
           group by p.productId, p.name""", [bf, bf, bf]).fetchall()
    sample = []
    for name, points in rows:
        k = kind(name)
        if not k:
            continue
        prices = {x["d"]: x["m"] for x in points}

        def near(t):
            for o in (0, 1, -1, 2, -2):
                if t + o in prices:
                    return prices[t + o]
        base = near(-14)
        if not base or base < 20 or any(near(t) is None for t in CHECKPOINTS):
            continue
        sample.append((k, {d: 100 * (m - base) / base for d, m in prices.items()}))
    return sample


def frame(fig, ax, note):
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.tick_params(length=0)
    ax.set_axisbelow(True)
    # No title or subtitle: the article text around each chart says what it shows.
    fig.text(0.03, 0.025, note, fontsize=10.5, color=FAINT)


def legend(fig, items, y):
    x = 0.03
    for label, color in items:
        fig.patches.append(FancyBboxPatch((x, y - 0.012), 0.016, 0.03, boxstyle="round,pad=0,rounding_size=0.004",
                                          transform=fig.transFigure, color=color))
        fig.text(x + 0.022, y + 0.003, label, fontsize=13, color=TEXT, va="center")
        x += 0.05 + 0.0088 * len(label)


con = duckdb.connect("/app/data/processed/prices_db.duckdb", read_only=True)
samples = {year: load(con, bf) for year, bf in BLACK_FRIDAYS.items()}
for year, sample in samples.items():
    print(year, len(sample), "products")

# ---- Chart 1: share of products priced higher / lower than two weeks before, day by day ----
# Not the median: about half the products sit at their old price for days at a time, so the
# median reads exactly 0% until more than half have moved, then jumps, which looks like a
# Black Friday trigger that isn't there. The shares show the drift as it happens.
fig, ax = plt.subplots(figsize=(12, 6.6), dpi=200)
fig.subplots_adjust(left=0.08, right=0.86, top=0.86, bottom=0.17)
frame(fig, ax, "Share of products priced above / below their price two weeks before Black Friday. English sealed, $20+. "
               "TCGplayer market prices.  poke6s.com")
ax.grid(axis="y", color=GRID, linewidth=1)
legend(fig, [(f"{y}  ({len(samples[y]):,} products)", c) for y, c in ((2024, Y2024), (2025, Y2025))], y=0.94)
days = range(-13, 43)
for year, color in ((2024, Y2024), (2025, Y2025)):
    for sign, style in ((1, "-"), (-1, (0, (5, 3)))):
        line = []
        for d in days:
            vals = [ch[d] for _, ch in samples[year] if d in ch]
            line.append(100 * sum(1 for v in vals if v * sign > 0) / len(vals))
        ax.plot(list(days), line, color=color, linewidth=2.6, linestyle=style, solid_capstyle="round")
        print(year, "higher" if sign > 0 else "lower", "BF", round(line[13]), "+14", round(line[27]), "+42", round(line[-1]))
for label, y in (("priced higher", 72), ("priced lower", 22)):
    ax.text(43, y, label, color=MUTED, fontsize=12.5, va="center")
ax.axvline(0, color=MUTED, linewidth=1.2, linestyle=(0, (2, 4)))
ax.text(0.6, 97, "Black Friday", color=MUTED, fontsize=12, va="top")
ax.set_ylim(0, 100); ax.set_xlim(-14, 42.5)
ax.set_yticks([0, 25, 50, 75, 100])
ax.set_xticks([-13, 0, 14, 28, 42])
ax.set_xticklabels(["2 weeks\nbefore", "Black\nFriday", "2 weeks\nafter", "4 weeks\nafter", "6 weeks\nafter"])
ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
fig.savefig(OUT + "bf_chart1.png", facecolor=BG); plt.close(fig)

# ---- Chart 2: 2024 by product type, Black Friday / two weeks / six weeks after ----
order = ["Booster bundles", "Elite Trainer Boxes", "Booster boxes", "All sealed $20+"]
rows = []
for label in order:
    items = [ch for k, ch in samples[2024] if label == "All sealed $20+" or k == label]

    def med(t):
        return st.median(next(ch[t + o] for o in (0, 1, -1, 2, -2) if t + o in ch) for ch in items)
    rows.append((label, len(items), med(0), med(14), med(42)))
    print(rows[-1])
fig, ax = plt.subplots(figsize=(12, 6.6), dpi=200)
fig.subplots_adjust(left=0.24, right=0.95, top=0.86, bottom=0.12)
frame(fig, ax, "2024. Median change from each product's price two weeks before Black Friday. "
               "TCGplayer market prices.  poke6s.com")
ax.grid(axis="x", color=GRID, linewidth=1)
legend(fig, [("Black Friday", STEPS[0]), ("2 weeks later", STEPS[1]), ("6 weeks later (mid-January)", STEPS[2])], y=0.94)
h = 0.26
for i, (label, n, a, b, c) in enumerate(rows):
    for j, (v, color) in enumerate(((a, STEPS[0]), (b, STEPS[1]), (c, STEPS[2]))):
        y = i + (j - 1) * h
        ax.barh(y, v, height=h, color=color, edgecolor=BG, linewidth=2)
        ax.text(max(v, 0) + 0.4, y, f"{v:+.0f}%", va="center", fontsize=12, color=TEXT)
ax.set_yticks(range(len(rows)))
ax.set_yticklabels([f"{label}\n({n} products)" for label, n, *_ in rows], linespacing=1.5)
ax.invert_yaxis()
ax.axvline(0, color=FAINT, linewidth=1.2)
ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:+.0f}%" if v else "0%"))
fig.savefig(OUT + "bf_chart2.png", facecolor=BG); plt.close(fig)
print("ok")
