"""Hold check: if I buy at this price today, can I sell at a profit after N days?

Three pieces of evidence, all from the card's own price history:
- break-even: the sale price that returns the buy price after fees and shipping
- trend: where the recent trend (log-linear fit over the last `horizon` days) points
- track record: how often past `horizon`-day stretches rose enough to clear break-even

This is a description of past behaviour, not a forecast or financial advice.
"""

from __future__ import annotations

import bisect
import math
from datetime import date, timedelta

from scripts.common.selling_costs import Channel, break_even_price, net_proceeds

MIN_TREND_POINTS = 8
MIN_HISTORY_WINDOWS = 60
FUTURE_MATCH_TOLERANCE_DAYS = 7
# Verdict judgement calls; tune here.
LIKELY_TRACK_RECORD_PCT = 50
POSSIBLE_TRACK_RECORD_PCT = 40
FLAT_TREND_PCT = -1.0  # % per 30 days still counted as flat
SPIKE_ABOVE_TREND_PCT = 20  # flag prices this far above the trend line


def _as_date(value) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def _trend(points: list[tuple[date, float]], horizon: int) -> dict | None:
    """Least-squares fit of log(price) against days; projects `horizon` days past the last point."""
    if len(points) < MIN_TREND_POINTS:
        return None
    origin = points[0][0]
    xs = [(d - origin).days for d, _ in points]
    ys = [math.log(p) for _, p in points]
    n = len(xs)
    mean_x, mean_y = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mean_x) ** 2 for x in xs)
    if sxx == 0:
        return None
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / sxx
    intercept = mean_y - slope * mean_x
    ss_tot = sum((y - mean_y) ** 2 for y in ys)
    ss_res = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys))
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    last_x = xs[-1]
    latest_price = points[-1][1]
    return {
        "points": n,
        "fit_now": math.exp(intercept + slope * last_x),
        # Projected from today's price at the trend's rate, so "trending up" always
        # projects above today even when the latest price sits above the fit line.
        "projected_price": latest_price * math.exp(slope * horizon),
        "pct_per_30d": (math.exp(slope * 30) - 1) * 100,
        "r2": r2,
    }


def _track_record(points: list[tuple[date, float]], horizon: int, channel: Channel, price_ratio: float = 1.0) -> dict:
    """Every past day bought at `price_ratio` x market and held `horizon` days: did it clear break-even?"""
    dates = [d for d, _ in points]
    wins = 0
    returns = []
    for d, price in points:
        target = d + timedelta(days=horizon)
        i = bisect.bisect_left(dates, target)
        best = None
        for j in (i - 1, i):
            if 0 <= j < len(dates) and abs((dates[j] - target).days) <= FUTURE_MATCH_TOLERANCE_DAYS:
                if best is None or abs((dates[j] - target).days) < abs((dates[best] - target).days):
                    best = j
        if best is None:
            continue
        future = points[best][1]
        returns.append(future / price - 1)
        if future >= break_even_price(price * price_ratio, channel):
            wins += 1
    returns.sort()
    return {
        "windows": len(returns),
        "cleared_break_even_pct": (wins / len(returns) * 100) if returns else None,
        "median_return_pct": (returns[len(returns) // 2] * 100) if returns else None,
    }


def hold_check(history, buy_price: float | None, channel: Channel, horizon: int = 90) -> dict:
    """`history` is an iterable of (date, market_price) rows, any order."""
    points = sorted(
        (_as_date(d), float(p)) for d, p in history if p is not None and float(p) > 0
    )
    if not points:
        return {"verdict": "no_data", "headline": "No price history for this item."}
    latest_date, latest_price = points[-1]
    buy = float(buy_price) if buy_price and buy_price > 0 else latest_price
    be = break_even_price(buy, channel)
    cutoff = latest_date - timedelta(days=horizon - 1)
    trend = _trend([pt for pt in points if pt[0] >= cutoff], horizon)
    # A typed buy price is applied as the same discount (or premium) to every past window.
    record = _track_record(points, horizon, channel, buy / latest_price)

    projected = trend["projected_price"] if trend else None
    projected_profit = net_proceeds(projected, channel) - buy if projected else None
    cleared = record["cleared_break_even_pct"]

    if trend is None or record["windows"] < MIN_HISTORY_WINDOWS:
        verdict, headline = "not_enough_history", "Not enough recent price history to judge a hold."
    elif projected >= be and cleared >= LIKELY_TRACK_RECORD_PCT:
        verdict, headline = "likely", f"Likely profitable in {horizon} days if the trend holds."
    elif projected >= be or (cleared >= POSSIBLE_TRACK_RECORD_PCT and trend["pct_per_30d"] >= FLAT_TREND_PCT):
        # A good track record only counts while the card isn't currently falling.
        verdict, headline = "possible", f"Possible in {horizon} days, but risky."
    else:
        verdict, headline = "unlikely", f"Not a {horizon}-day flip. Only worth it as a longer hold."

    reasons = [f"Needs ${be:,.2f} (+{(be / buy - 1) * 100:.0f}%) to break even after {channel.label} fees and shipping."]
    if trend:
        above_trend_pct = (latest_price / trend["fit_now"] - 1) * 100
        if above_trend_pct >= SPIKE_ABOVE_TREND_PCT:
            reasons.append(
                f"Today's price is {above_trend_pct:.0f}% above its {horizon}-day trend line; "
                "sharp spikes often fall back."
            )
        direction = "up" if trend["pct_per_30d"] >= 0 else "down"
        reasons.append(
            f"Last {horizon} days trend {direction} {abs(trend['pct_per_30d']):.1f}% a month; "
            f"if that continues it reaches about ${projected:,.2f}."
        )
    if cleared is not None:
        reasons.append(
            f"In {record['windows']} past {horizon}-day stretches, buying at "
            f"{'market' if abs(buy - latest_price) < 0.005 else f'{buy / latest_price * 100:.0f}% of market'} cleared break-even "
            f"{cleared:.0f}% of the time (median change {record['median_return_pct']:+.0f}%)."
        )

    return {
        "verdict": verdict,
        "headline": headline,
        "reasons": reasons,
        "channel": channel.label,
        "horizon_days": horizon,
        "latest_date": latest_date.isoformat(),
        "market_price": round(latest_price, 2),
        "buy_price": round(buy, 2),
        "break_even_price": round(be, 2),
        "break_even_pct": round((be / buy - 1) * 100, 1),
        "trend_points": trend["points"] if trend else 0,
        "trend_pct_per_30d": round(trend["pct_per_30d"], 1) if trend else None,
        "trend_fit_r2": round(trend["r2"], 2) if trend else None,
        "above_trend_pct": round((latest_price / trend["fit_now"] - 1) * 100, 1) if trend else None,
        "projected_price": round(projected, 2) if projected else None,
        "projected_profit": round(projected_profit, 2) if projected_profit is not None else None,
        "history_windows": record["windows"],
        "cleared_break_even_pct": round(cleared, 1) if cleared is not None else None,
        "median_return_pct": round(record["median_return_pct"], 1) if record["median_return_pct"] is not None else None,
    }
