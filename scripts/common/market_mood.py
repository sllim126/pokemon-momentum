"""Market mood: one 0-100 number for how broadly Pokemon card (singles) prices are rising.

It is a breadth reading, not a price index: every printing priced at MOOD_MIN_PRICE or more
gets one vote, whatever it costs, so a $3 card counts as much as a $3,000 one. Three
questions are asked for every day of the series snapshot (the last ~730 days):

  above_avg  - of the cards not exactly at their 30-day average, the share above it
  rising     - of the cards whose price moved over 7 days, the share that went up
  new_highs  - of the cards at a new 90-day high or a new 90-day low, the share at a high

Each is a share from 0 to 1. The mood is their average times 100. Cards with an unchanged
price are left out of each share rather than counted as falling: TCGplayer market prices
often sit still for days, and counting those would drag every reading toward "cold".

MOOD_SQL is shared by the nightly snapshot (scripts/indicators/build_market_mood_snapshot.py)
and the website's fallback (api.py, market_mood route), so both give the same numbers. It
reads the series snapshot, whose arrays hold one price per day, so LAG(p, 7) is a week back.
"""

MOOD_MIN_PRICE = 2.0  # bulk below this changes price in steps of cents and would add noise
HIGH_LOW_WINDOW = 90  # days for "new high" / "new low"
BASELINE_MIN_PRICE = 5.0  # same floor as the Discover lists, so the baseline is like for like
BASELINE_HORIZONS = (7, 14, 30)

# Plain-language bands for the number. Kept deliberately mild: this is breadth, not advice.
MOOD_BANDS = (
    (20, "Cold", "Most cards are slipping."),
    (40, "Cool", "More cards are falling than rising."),
    (60, "Neutral", "Rising and falling cards are roughly balanced."),
    (80, "Warm", "Most cards are rising."),
    (101, "Hot", "Nearly everything is rising. Prices may be running ahead of themselves."),
)


def mood_sql(series_table: str, signal_table: str, category_id: int) -> str:
    """Per-day counts behind the mood, one row per date, oldest first.

    Singles only: the product signal snapshot says which printings are cards (productKind),
    so sealed products, whose prices move for other reasons, stay out.

    The fwd{h}_* columns are the Track record's market baseline (scripts/dashboards/
    pick_tracker.py): of the cards priced BASELINE_MIN_PRICE or more on that day, how many
    had a price h days later, how many were higher (_up) or at least 10% higher (_hit), and
    the median change in percent. Recent days have no h-days-later price yet, so their
    counts are 0 and their median NULL.
    """
    forward = ",\n            ".join(
        f"COUNT(*) FILTER (WHERE p >= {BASELINE_MIN_PRICE} AND f{h} IS NOT NULL) AS fwd{h}_n,\n"
        f"            COUNT(*) FILTER (WHERE p >= {BASELINE_MIN_PRICE} AND f{h} > p) AS fwd{h}_up,\n"
        f"            COUNT(*) FILTER (WHERE p >= {BASELINE_MIN_PRICE} AND f{h} >= p * 1.10) AS fwd{h}_hit,\n"
        f"            MEDIAN((f{h} / p - 1) * 100) FILTER (WHERE p >= {BASELINE_MIN_PRICE} AND f{h} IS NOT NULL) AS fwd{h}_median"
        for h in BASELINE_HORIZONS
    )
    leads = "\n                ".join(f"LEAD(p, {h}) OVER k AS f{h}," for h in BASELINE_HORIZONS)
    return f"""
        WITH cards AS (
            SELECT DISTINCT productId, subTypeName
            FROM {signal_table}
            WHERE categoryId = {int(category_id)} AND productKind = 'card'
        ), s AS (
            SELECT ser.productId, ser.subTypeName,
                UNNEST(CAST(ser.dates_json AS DATE[])) AS d,
                UNNEST(CAST(ser.prices_json AS DOUBLE[])) AS p,
                UNNEST(CAST(ser.sma30_json AS DOUBLE[])) AS sma30
            FROM {series_table} ser
            JOIN cards USING (productId, subTypeName)
            WHERE ser.categoryId = {int(category_id)}
        ), w AS (
            SELECT d, p, sma30,
                {leads}
                LAG(p, 7) OVER k AS p7,
                MAX(p) OVER (k ROWS BETWEEN {HIGH_LOW_WINDOW} PRECEDING AND 1 PRECEDING) AS hi,
                MIN(p) OVER (k ROWS BETWEEN {HIGH_LOW_WINDOW} PRECEDING AND 1 PRECEDING) AS lo,
                ROW_NUMBER() OVER k AS rn
            FROM s
            WINDOW k AS (PARTITION BY productId, subTypeName ORDER BY d)
        )
        SELECT d AS date,
            COUNT(*) AS cards,
            SUM((p > sma30)::INT) AS above_avg,
            SUM((p < sma30)::INT) AS below_avg,
            SUM((p > p7)::INT) AS up_7d,
            SUM((p < p7)::INT) AS down_7d,
            -- A card needs a full window of history before it can make a new high or low.
            SUM((rn > {HIGH_LOW_WINDOW} AND p > hi)::INT) AS new_highs,
            SUM((rn > {HIGH_LOW_WINDOW} AND p < lo)::INT) AS new_lows,
            {forward}
        FROM w
        WHERE p >= {MOOD_MIN_PRICE}
        GROUP BY d
        ORDER BY d
    """


def _share(yes, no):
    """yes / (yes + no), or None when neither happened (nothing to judge)."""
    yes, no = int(yes or 0), int(no or 0)
    return yes / (yes + no) if yes + no else None


def mood_band(score: float | None) -> dict:
    """The label and one-line meaning for a 0-100 mood."""
    if score is None:
        return {"label": "No reading", "meaning": "Not enough price history yet."}
    for upper, label, meaning in MOOD_BANDS:
        if score < upper:
            return {"label": label, "meaning": meaning}
    return {"label": MOOD_BANDS[-1][1], "meaning": MOOD_BANDS[-1][2]}


def score_rows(rows: list[dict]) -> list[dict]:
    """Turn MOOD_SQL's daily counts into readings: the three shares (as %) and the mood.

    A day where none of the three can be judged (the first week of history) is dropped. A
    day missing one part averages the parts it has.
    """
    readings = []
    for row in rows:
        parts = {
            "above_avg": _share(row["above_avg"], row["below_avg"]),
            "rising": _share(row["up_7d"], row["down_7d"]),
            "new_highs": _share(row["new_highs"], row["new_lows"]),
        }
        known = [v for v in parts.values() if v is not None]
        if not known or parts["rising"] is None:
            continue
        readings.append({
            "date": str(row["date"])[:10],
            "mood": round(100 * sum(known) / len(known), 1),
            "cards": int(row["cards"]),
            **{k: (round(100 * v, 1) if v is not None else None) for k, v in parts.items()},
            "up_7d": int(row["up_7d"] or 0),
            "down_7d": int(row["down_7d"] or 0),
            "new_highs_count": int(row["new_highs"] or 0),
            "new_lows_count": int(row["new_lows"] or 0),
        })
    return readings


def mood_summary(readings: list[dict]) -> dict:
    """Latest reading with its band, and how it compares with a week and a month before."""
    if not readings:
        return {"available": False}
    latest = readings[-1]

    def back(days: int):
        return readings[-1 - days]["mood"] if len(readings) > days else None

    return {
        "available": True,
        "latest": {**latest, **mood_band(latest["mood"])},
        "week_ago": back(7),
        "month_ago": back(30),
        "year_high": max(r["mood"] for r in readings[-365:]),
        "year_low": min(r["mood"] for r in readings[-365:]),
        "history": [{"date": r["date"], "mood": r["mood"]} for r in readings],
        "min_price": MOOD_MIN_PRICE,
    }


def baseline_by_date(rows: list[dict]) -> dict[str, dict]:
    """{date: {"fwd7_up": share, "fwd7_hit": share, "fwd7_median": pct, ...}} for the Track record.

    Shares are 0-1. A horizon with no cards to judge (recent days) is left out.
    """
    out = {}
    for row in rows:
        day = {}
        for h in BASELINE_HORIZONS:
            n = int(row.get(f"fwd{h}_n") or 0)
            if n:
                day[f"fwd{h}_up"] = int(row[f"fwd{h}_up"] or 0) / n
                day[f"fwd{h}_hit"] = int(row[f"fwd{h}_hit"] or 0) / n
                median = row.get(f"fwd{h}_median")
                day[f"fwd{h}_median"] = float(median) if median is not None else None
        if day:
            out[str(row["date"])[:10]] = day
    return out
