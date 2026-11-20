"""Charts for the blog post "Master Sets, Part 2: Japanese vs English" (2026-11-20).

Data: TCGplayer market prices for 2026-11-18, summed over every printing in each set, worked
out from prices_db.duckdb and written in below. Dark theme from the storefront's store.css;
English #4096f0 and Japanese #d95926 pass the dataviz palette check on the #0e1828 surface.

Run inside the app container, which has matplotlib (the folder must exist first):

    mkdir -p .chart_tmp
    docker-compose exec -T pokemon-momentum python - < scripts/blog/master_sets_part2_charts.py

PNGs land in .chart_tmp/; add them to the store with storefront/app/images.store_original
(storefront/README.md "Articles"), then delete .chart_tmp. A starting point for later posts.
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

BG, TEXT, MUTED, FAINT, GRID = "#0e1828", "#eaf1ff", "#93a6c8", "#6f82a6", "#1d2a40"
EN, JP, REST = "#4096f0", "#d95926", "#3a4a66"
OUT = "/app/.chart_tmp/"
plt.rcParams.update({"font.family": "DejaVu Sans", "text.color": TEXT, "axes.labelcolor": MUTED,
                     "xtick.color": MUTED, "ytick.color": TEXT, "font.size": 13})


def base(fig, ax, title, subtitle):
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)
    for s in ax.spines.values(): s.set_visible(False)
    ax.tick_params(length=0)
    ax.grid(axis="x", color=GRID, linewidth=1); ax.set_axisbelow(True)
    fig.text(0.03, 0.955, title, fontsize=21, fontweight="bold", color=TEXT, va="top")
    fig.text(0.03, 0.905, subtitle, fontsize=13, color=MUTED, va="top")
    fig.text(0.03, 0.02, "Sum of TCGplayer market prices for every printing, Nov 18, 2026. Excludes shipping.  poke6s.com",
             fontsize=10.5, color=FAINT)


def legend(fig, items, y=0.855):
    x = 0.03
    for label, color in items:
        fig.patches.append(FancyBboxPatch((x, y - 0.008), 0.016, 0.022, boxstyle="round,pad=0,rounding_size=0.004",
                                          transform=fig.transFigure, color=color))
        fig.text(x + 0.022, y + 0.003, label, fontsize=13, color=TEXT, va="center")
        x += 0.045 + 0.0085 * len(label)


def hbar(ax, y, w, h, color, left=0):
    ax.barh(y, w, height=h, left=left, color=color, edgecolor=BG, linewidth=2)


# ---- Chart 1: English vs Japanese ----
groups = [
    ("Big special sets: Japanese is far cheaper", [
        ("Prismatic Evolutions", "Terastal Festival ex", 4428, 1745),
        ("Black Bolt", "Black Bolt (JP)", 3715, 1641),
        ("White Flare", "White Flare (JP)", 3326, 1506),
        ("Ascended Heroes", "MEGA Dream ex", 3228, 1437),
        ("Mega Evolution", "Mega Brave + Mega Symphonia", 1257, 936)]),
    ("Main sets: about even", [
        ("Destined Rivals", "Glory of Team Rocket + Heat Wave Arena", 1229, 1412),
        ("Delta Reign", "Storm Emeralda", 938, 914),
        ("Perfect Order", "Nihil Zero", 431, 422),
        ("Journey Together", "Battle Partners", 392, 382)]),
    ("Newest Mega sets: English is cheaper", [
        ("Phantasmal Flames", "Inferno X", 896, 1440),
        ("Pitch Black", "Abyss Eye", 502, 680),
        ("Chaos Rising", "Ninja Spinner", 410, 649)]),
]
fig, ax = plt.subplots(figsize=(12, 13.5), dpi=200)
fig.subplots_adjust(left=0.30, right=0.95, top=0.82, bottom=0.05)
base(fig, ax, "English vs Japanese master sets", "What it costs to complete each set, English next to its Japanese version")
legend(fig, [("English", EN), ("Japanese", JP)])
y, ticks, labels, h = 0, [], [], 0.36
for gtitle, rows in groups:
    ax.text(-0.43, y - 0.15, gtitle.upper(), transform=ax.get_yaxis_transform(), fontsize=11.5,
            color=MUTED, fontweight="bold", va="center")
    y += 0.75
    for en, jp, ev, jv in rows:
        hbar(ax, y - h / 2, ev, h, EN); hbar(ax, y + h / 2, jv, h, JP)
        for val, yy in ((ev, y - h / 2), (jv, y + h / 2)):
            ax.text(val + 50, yy, f"\\${val:,}", va="center", fontsize=11.5, color=TEXT)
        ticks.append(y); labels.append(f"{en}\n{jp}")
        y += 1.05
    y += 0.35
ax.set_yticks(ticks); ax.set_yticklabels(labels, fontsize=12, linespacing=1.55)
for t in ax.get_yticklabels(): t.set_color(TEXT)
ax.invert_yaxis(); ax.set_xlim(0, 4900)
ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"${v:,.0f}"))
fig.savefig(OUT + "chart1.png", facecolor=BG); plt.close(fig)

# ---- Chart 2: secret rares vs everything else ----
rows = [("Mega Evolution", 1200, 1257), ("Phantasmal Flames", 857, 896), ("Destined Rivals", 1145, 1229),
        ("Perfect Order", 396, 431), ("Prismatic Evolutions", 3783, 4428), ("Journey Together", 314, 392)]
fig, ax = plt.subplots(figsize=(12, 7.2), dpi=200)
fig.subplots_adjust(left=0.22, right=0.95, top=0.74, bottom=0.10)
base(fig, ax, "The secret rares are the set", "Share of each English master set's cost, by type of card")
legend(fig, [("Secret rares (numbered past the set total)", EN), ("Everything else", REST)], y=0.82)
for i, (name, sec, full) in enumerate(rows):
    pct = 100 * sec / full
    hbar(ax, i, pct, 0.6, EN); hbar(ax, i, 100 - pct, 0.6, REST, left=pct)
    ax.text(pct - 1.5, i, f"{pct:.0f}%", va="center", ha="right", fontsize=13, fontweight="bold", color="#08111d")
    ax.text(101.5, i, f"\\${full - sec:,} of \\${full:,}", va="center", fontsize=11, color=MUTED)
ax.set_yticks(range(len(rows))); ax.set_yticklabels([r[0] for r in rows])
ax.invert_yaxis(); ax.set_xlim(0, 122); ax.set_xticks([0, 25, 50, 75, 100])
ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
ax.text(101.5, -0.75, "Everything else costs", fontsize=11, color=MUTED, va="center")
fig.savefig(OUT + "chart2.png", facecolor=BG); plt.close(fig)

# ---- Chart 3: top card share ----
rows = [("Chaos Rising / Ninja Spinner", "Mega Greninja ex", 26, 58),
        ("Pitch Black / Abyss Eye", "Mega Darkrai ex", 25, 54),
        ("Delta Reign / Storm Emeralda", "Mega Rayquaza ex", 30, 53),
        ("Perfect Order / Nihil Zero", "Mega Zygarde ex", 19, 39),
        ("Journey Together / Battle Partners", "Lillie's Clefairy ex", 19, 33),
        ("Phantasmal Flames / Inferno X", "Mega Charizard X ex", 59, 45)]
fig, ax = plt.subplots(figsize=(12, 9), dpi=200)
fig.subplots_adjust(left=0.33, right=0.95, top=0.78, bottom=0.08)
base(fig, ax, "Why? One card.", "The top chase card's share of the whole master set's cost")
legend(fig, [("English", EN), ("Japanese", JP)], y=0.845)
h = 0.36
for i, (pair, card, e, j) in enumerate(rows):
    hbar(ax, i - h / 2, e, h, EN); hbar(ax, i + h / 2, j, h, JP)
    ax.text(e + 0.8, i - h / 2, f"{e}%", va="center", fontsize=12)
    ax.text(j + 0.8, i + h / 2, f"{j}%", va="center", fontsize=12)
ax.set_yticks(range(len(rows))); ax.set_yticklabels([f"{p}\n{c}" for p, c, *_ in rows], linespacing=1.5)
for t, (_, c, *_) in zip(ax.get_yticklabels(), rows): t.set_fontsize(12)
ax.invert_yaxis(); ax.set_xlim(0, 68); ax.set_xticks([0, 20, 40, 60])
ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:.0f}%"))
ax.axhline(4.5, color=GRID, linewidth=1.5, linestyle=(0, (4, 4)))
ax.text(67, 4.38, "Charizard: the English exception", ha="right", va="bottom", fontsize=11, color=MUTED)
fig.savefig(OUT + "chart3.png", facecolor=BG); plt.close(fig)
print("ok")
