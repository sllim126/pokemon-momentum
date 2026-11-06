"""Track record: what the Discover lists picked, and how those picks did afterwards.

Why: a list called "Breakouts" is only worth following if breakouts tend to keep rising.
This module keeps score in public, including the misses, and always next to what the whole
market did over the same days, so a good number in a rising market isn't mistaken for skill.

How it works:
  1. Recording (record_daily_picks, called from api.py when the site starts, which happens
     after every nightly update): the top PICKS_PER_LIST rows of each list in TRACKED_LISTS
     (api.py) are stored with the price they were shown at, in tracking_store's
     list_pick_history. Budget Builder picks come from budget_pick_history instead (the
     builder records its own default view each day), with the entry price read from history.
  2. Scoring (build_track_record): a card counts on the first day it joins a list; staying
     on the list on later days is the same pick, not a new one (NEW_PICK_GAP_DAYS). Each pick
     is scored at 7, 14 and 30 days with the TCGplayer market price from the series snapshot.
     Horizons that haven't come round yet count as open.
  3. Baseline: the market mood snapshot carries, for every day, what all cards priced $5 or
     more did over the next 7/14/30 days (scripts/common/market_mood.py). Each list is shown
     against that baseline, weighted by how many picks it made on each day.

Returns are price changes only: no fees, shipping or the gap between market price and what a
card sells for. The page says so.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from statistics import median

PICKS_PER_LIST = 25
HORIZONS = (7, 14, 30)
NEW_PICK_GAP_DAYS = 30  # a card back on a list after this many days off it is a new pick
HIT_PCT = 10.0  # "hit" = up at least this much


def _d(value) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def first_appearances(picks: list[dict]) -> list[dict]:
    """Keep each card's first day on a list, dropping repeat days within NEW_PICK_GAP_DAYS.

    picks: dicts with list_key, price_date, product_id, sub_type_name (any order). A card
    that leaves a list and comes back more than NEW_PICK_GAP_DAYS after it was last seen
    counts again.
    """
    last_seen: dict[tuple, date] = {}
    fresh = []
    for pick in sorted(picks, key=lambda p: (str(p["price_date"]), p["list_key"], p.get("rank", 0))):
        key = (pick["list_key"], int(pick["product_id"]), str(pick["sub_type_name"]))
        day = _d(pick["price_date"])
        seen = last_seen.get(key)
        if seen is None or (day - seen).days > NEW_PICK_GAP_DAYS:
            fresh.append(pick)
        last_seen[key] = day
    return fresh


def price_on(series: tuple[list, list] | None, day: date) -> float | None:
    """The price on `day` from a (dates, prices) series, or the last one before it.

    None when the series starts after `day` or `day` is past its end (not happened yet).
    """
    if not series:
        return None
    dates, prices = series
    if not dates or day > _d(dates[-1]) or day < _d(dates[0]):
        return None
    target = day.isoformat()
    # Dates are ISO strings in order, so a binary search on the strings works.
    lo, hi = 0, len(dates) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if str(dates[mid])[:10] <= target:
            lo = mid
        else:
            hi = mid - 1
    price = prices[lo]
    return float(price) if price is not None else None


def score_picks(picks: list[dict], series: dict[tuple, tuple[list, list]], latest: date) -> list[dict]:
    """Add the entry price (if missing), the price now and the 7/14/30-day results to each pick.

    series: {(productId, subTypeName): (dates, prices)}. A pick with no usable entry price is
    dropped, since it can't be scored either way.
    """
    scored = []
    for pick in picks:
        key = (int(pick["product_id"]), str(pick["sub_type_name"]))
        day = _d(pick["price_date"])
        entry = pick.get("entry_price") or price_on(series.get(key), day)
        if not entry or entry <= 0:
            continue
        now = price_on(series.get(key), latest)
        row = {
            **pick,
            "price_date": day.isoformat(),
            "entry_price": round(float(entry), 2),
            "now_price": now,
            "now_pct": round((now / entry - 1) * 100, 1) if now else None,
            "age_days": (latest - day).days,
        }
        for h in HORIZONS:
            later = price_on(series.get(key), day + timedelta(days=h)) if (latest - day).days >= h else None
            row[f"d{h}_pct"] = round((later / entry - 1) * 100, 1) if later else None
        scored.append(row)
    return scored


def summarize(scored: list[dict], baseline: dict[str, dict] | None = None) -> dict:
    """Hit rates and median results per horizon, with the market baseline for the same days.

    baseline: {date: {"fwd7_up": share 0-1, "fwd7_hit": share, "fwd7_median": pct, ...}} from
    the market mood snapshot. The market figure for a horizon is the average over the pick
    days, weighted by picks per day, so it matches the mix of days the list picked on.
    """
    # Open = younger than the longest horizon, so its final result is still to come.
    young = [r for r in scored if r["age_days"] < HORIZONS[-1]]
    out = {"picks": len(scored), "open": len(young)}
    open_moves = [r["now_pct"] for r in young if r["now_pct"] is not None]
    out["open_median_pct"] = round(median(open_moves), 1) if open_moves else None
    for h in HORIZONS:
        done = [r for r in scored if r[f"d{h}_pct"] is not None]
        results = [r[f"d{h}_pct"] for r in done]
        entry = {
            "scored": len(done),
            "up_share": round(100 * sum(1 for x in results if x > 0) / len(results), 1) if results else None,
            "hit_share": round(100 * sum(1 for x in results if x >= HIT_PCT) / len(results), 1) if results else None,
            "median_pct": round(median(results), 1) if results else None,
            "market_up_share": None,
            "market_hit_share": None,
            "market_median_pct": None,
        }
        if baseline and done:
            weights = defaultdict(int)
            for r in done:
                weights[r["price_date"]] += 1
            for field, src in (("market_up_share", "up"), ("market_hit_share", "hit"), ("market_median_pct", "median")):
                pairs = [(baseline[d].get(f"fwd{h}_{src}"), w) for d, w in weights.items() if d in baseline]
                pairs = [(v, w) for v, w in pairs if v is not None]
                if pairs:
                    total = sum(w for _, w in pairs)
                    value = sum(v * w for v, w in pairs) / total
                    entry[field] = round(value * 100 if src in ("up", "hit") else value, 1)
        out[f"d{h}"] = entry
    return out


def build_track_record(
    picks: list[dict],
    series: dict[tuple, tuple[list, list]],
    latest: date,
    baseline: dict[str, dict] | None,
    list_labels: dict[str, str],
    recent_limit: int = 60,
) -> dict:
    """The whole Track record payload: per-list summaries, an all-lists line and recent picks."""
    fresh = first_appearances(picks)
    scored = score_picks(fresh, series, latest)
    by_list = defaultdict(list)
    for row in scored:
        by_list[row["list_key"]].append(row)
    lists = []
    for key, label in list_labels.items():
        rows = by_list.get(key, [])
        lists.append({"key": key, "label": label, **summarize(rows, baseline)})
    recent = sorted(scored, key=lambda r: (r["price_date"], -r.get("rank", 0)), reverse=True)[:recent_limit]
    first_day = min((r["price_date"] for r in scored), default=None)
    return {
        "available": bool(scored),
        "latest_date": latest.isoformat(),
        "first_pick_date": first_day,
        "horizons": list(HORIZONS),
        "hit_pct": HIT_PCT,
        "picks_per_list": PICKS_PER_LIST,
        "all": summarize(scored, baseline),
        "lists": lists,
        "recent": recent,
    }
