"""Weekly Discord posts for the Poke6s server, built from the site's live data.

Every Friday morning (host crontab, user adam) the "Poke6s Control" bot posts:

- #what-should-i-buy: sealed products cheapest against buying their packs loose, the top
  Pullbacks, and a $100 Budget Builder pull.
- #market-talk: the overall market (Top 151) against the Scarlet & Violet and Mega indexes,
  which sets are cooling (Whole-set Dips), and a question to start a conversation.
- #set-breakdowns: a "set of the week", rotating through the newest English sets.
- #card-spotlight: a "card of the week", alternating a card on the rise (Top Movers) and a card
  on sale (Pullbacks), with graded prices from PriceCharting (always linked back, as the
  PriceCharting licence requires) and the 6-Month Hold check.

It reads the site's own API on this machine (http://localhost:8001), so it only ever shows what
the site shows. Cards with few price changes are skipped, so a single odd sale doesn't become
the card of the week. A state file (data/discord_weekly_state.json) records the week last
posted, so a second run in the same week posts nothing.

    python3 scripts/utilities/post_weekly_discord.py --dry-run   # print the posts only
    python3 scripts/utilities/post_weekly_discord.py             # post (once per ISO week)
    python3 scripts/utilities/post_weekly_discord.py --force     # post again this week

Settings: POKE6S_DISCORD_BOT_TOKEN from the environment or .env (never printed).
Exit codes: 0 ok (or already posted this week), 1 error.
"""

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE_FILE = ROOT / "data" / "discord_weekly_state.json"
API = "http://localhost:8001"
SITE = "https://market.poke6s.com"
DISCORD = "https://discord.com/api/v10"
USER_AGENT = "DiscordBot (https://market.poke6s.com, 1.0)"
CHANNELS = {  # Poke6s server channel ids
    "buy": "1489020872080625664",  # #what-should-i-buy
    "market": "1489020914824909001",  # #market-talk
    "sets": "1489020956545515611",  # #set-breakdowns
    "spotlight": "1489020991781867550",  # #card-spotlight
}
NFA = "*Research tool, not financial advice.*"
MIN_PRICE_CHANGES = 6  # a featured card must have changed price this often in 30 days
QUESTIONS = (
    "What are you watching this week? 👇",
    "Buying, holding or selling right now? 👇",
    "Which set do you think bottoms out first? 👇",
    "Vintage or modern for the next year? 👇",
    "What's the best pull you've seen this month? 👇",
)


def setting(name: str) -> str:
    """A setting from the environment, falling back to the repo's .env file."""
    value = os.getenv(name, "").strip()
    env_file = ROOT / ".env"
    if not value and env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, _, raw = line.partition("=")
            if key.strip() == name:
                value = raw.strip().strip("'\"")
    return value


def get(path: str, **params) -> dict:
    url = f"{API}{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=60) as response:
        return json.load(response)


def rows(payload: dict) -> list[dict]:
    """API answers come as {columns, rows} or {items}; return a list of dicts either way."""
    if "items" in payload:
        return payload["items"]
    return [dict(zip(payload["columns"], row)) for row in payload.get("rows", [])]


def money(value: float) -> str:
    return f"${value:,.2f}"


def pct(value: float | None) -> str:
    return "—" if value is None else f"{value:+.1f}%"


def card_link(product_id, sub_type: str) -> str:
    return f"<{SITE}/?product={product_id}%7C%7C{urllib.parse.quote(sub_type)}&card=1>"


def short_set(name: str) -> str:
    """'SV: Scarlet & Violet 151' -> 'Scarlet & Violet 151'."""
    return re.sub(r"^[^:]+:\s*", "", name or "")


# ---- #what-should-i-buy -----------------------------------------------------------------

LOOSE_TYPES = {"Booster Box", "Booster Bundle", "Elite Trainer Box", "Pokemon Center ETB", "Build & Battle Box"}


def sealed_vs_loose(limit: int = 3) -> list[dict]:
    """Sealed products cheapest against buying the same packs loose (same rules as the app's
    loosePackPrices/packBreakdown in desktop_home.html)."""
    items = get("/sealed_deals", category_id=3, min_price=0, max_price=100000, limit=2000)["items"]
    loose: dict[str, float] = {}
    for item in items:
        name, group = item["productName"], item.get("groupName") or ""
        if item["productType"] != "Booster Pack" or item["pack_count"] != 1 or not (item["latest_price"] or 0) > 0:
            continue
        if re.search(r"\[|mini|special|promo|packs|general mills|miscellaneous", f"{name} {group}", re.I) or not name.endswith("Booster Pack"):
            continue
        for alias in {group, re.sub(r"^(SM|XY|SWSH|BW|DP|HGSS|EX)\s+-\s+", "", short_set(group))}:
            loose[alias] = min(loose.get(alias, 1e9), item["latest_price"])
    deals = []
    for item in items:
        if item["productType"] not in LOOSE_TYPES or not item.get("pack_mix") or not 20 <= item["latest_price"] <= 500:
            continue
        parts = [re.match(r"^(\d+)x (.+)$", p.strip()) for p in item["pack_mix"].split(",")]
        if any(not m or m[2].strip() not in loose for m in parts):
            continue
        loose_total = sum(int(m[1]) * loose[m[2].strip()] for m in parts)
        vs = (item["latest_price"] / loose_total - 1) * 100
        if vs <= -5:
            deals.append({**item, "loose_total": loose_total, "loose_each": loose_total / item["pack_count"], "vs": vs})
    return sorted(deals, key=lambda d: d["vs"])[:limit]


def post_buy(today: dt.date) -> str:
    lines = [f"**What should I buy? Picks for the week of {today:%b} {today.day}**", ""]
    deals = sealed_vs_loose()
    if deals:
        lines.append("**📦 Sealed cheaper than buying the packs loose**")
        for d in deals:
            lines.append(f"• **{d['productName']}**: {money(d['latest_price'])}, {money(d['price_per_pack'])} a pack, "
                         f"about {abs(d['vs']):.0f}% under {d['pack_count']} loose packs ({money(d['loose_each'])} each)")
        lines.append("")
    pullbacks = sorted(rows(get("/good_buys", category_id=3, limit=500)), key=lambda r: -(r["latest_price"] or 0))[:3]
    if pullbacks:
        lines.append("**📉 Pullbacks: falling faster than their set**")
        for r in pullbacks:
            lines.append(f"• **{r['productName']}**, {short_set(r['groupName'])}: {money(r['latest_price'])}, "
                         f"{pct(r['roc_30d_pct'])} in 30 days, {r['off_high_pct']:.0f}% below its 90-day high")
        lines.append("")
    budget = get("/budget_builder", budget=100, limit=7, category_id=3)
    picks = budget.get("items") or []
    if picks:
        lines.append(f"**💵 A $100 budget pull:** {len(picks)} cards for {money(budget['spent'])}")
        lines.append(" · ".join(f"{p['productName']} {money(p['latest_price'])}" for p in picks))
        lines.append("")
    lines.append(f"Full lists: <{SITE}/?list=sealed_deals> · <{SITE}/?list=good_buys> · <{SITE}/?list=budget>")
    lines.append(NFA)
    return "\n".join(lines)


# ---- #market-talk -----------------------------------------------------------------------

def post_market(today: dt.date, week: int) -> str:
    lines = [f"**Market snapshot: {today:%b} {today.day}** 📊", "The Poke6s indexes each started at 1,000:"]
    for key in ("pokemon100", "mega100", "sv100"):
        data = get("/index-overview-data", index=key, category_id=3)
        summary = data["summary"]
        tag = " (the overall market)" if key == "pokemon100" else ""
        lines.append(f"• **{data['index_name']}**{tag}: {summary['current_level']:,.0f}, "
                     f"**{pct(summary['day30_change']['pct'])}** in 30 days")
    dips = rows(get("/set_dips", category_id=3, limit=500))
    by_set: dict[str, list[dict]] = {}
    for r in dips:
        by_set.setdefault(short_set(r["groupName"]), []).append(r)
    cooling = sorted(by_set.items(), key=lambda kv: -len(kv[1]))[:3]
    if cooling:
        lines += ["", "**Sets cooling off** (whole set falling together):"]
        for name, cards in cooling:
            median = sorted(c["set_median_30d_pct"] for c in cards if c.get("set_median_30d_pct") is not None)
            typical = f", typical card {pct(median[len(median) // 2])}" if median else ""
            lines.append(f"• **{name}**: {len(cards)} cards on sale{typical}")
    lines += ["", QUESTIONS[week % len(QUESTIONS)], f"<{SITE}/?list=indexes> · <{SITE}/?list=set_dips>", NFA]
    return "\n".join(lines)


# ---- #set-breakdowns --------------------------------------------------------------------

def post_set(week: int) -> str:
    sets = rows(get("/set_baskets", category_id=3, limit=2000, min_cards=60))
    skip = re.compile(r"promo|prize pack|trainer gallery|galarian gallery|shiny vault|miscellaneous|mcdonald|trick or trade|energies", re.I)
    recent = sorted((s for s in sets if s.get("release_date") and not skip.search(s["groupName"])),
                    key=lambda s: s["release_date"], reverse=True)[:8]
    s = recent[week % len(recent)]
    released = dt.date.fromisoformat(s["release_date"])
    lines = [f"**Set of the week: {s['groupName']}** (released {released:%b} {released.day}, {released.year})",
             f"• **{s['card_count']} cards.** A full set costs about **{money(s['total_set_cost'])[:-3]}**",
             f"• Average card {money(s['avg_card_price'])}, median {money(s['median_card_price'])}",
             f"• Top hit: **{s['top_hit_name']}** at {money(s['top_hit_price'])} ({s['top_hit_share_pct']:.0f}% of the set's value)",
             f"• The top 3 hits are {s['top3_share_pct']:.0f}% of the set's value "
             f"(depth score {s['depth_score']:.0f}/100; lower means more top-heavy)"]
    signals = {r["groupId"]: r for r in rows(get("/group_signals", category_id=3, limit=1000))}
    sig = signals.get(s["groupId"])
    if sig and sig.get("avg_30d_pct") is not None:
        lines.append(f"• Average card over 30 days: {pct(sig['avg_30d_pct'])}, over 90 days: {pct(sig.get('avg_90d_pct'))}")
    lines += ["", f"Every card in the set: <{SITE}/?list=set&set={s['groupId']}>", NFA]
    return "\n".join(lines)


# ---- #card-spotlight --------------------------------------------------------------------

def spotlight_card(week: int) -> tuple[dict, str]:
    """Odd weeks: a $50+ card on the rise; even weeks: a $50+ card on sale (Pullbacks)."""
    if week % 2:
        candidates = [r for r in rows(get("/top_movers", category_id=3, limit=300))
                      if (r.get("p_now") or 0) >= 50 and abs(r.get("roc_pct") or 0) < 100
                      and r.get("productKind") == "card"  # a single card, not sealed product
                      and "energy" not in (r.get("productName") or "").lower()]  # a Pokemon, not an Energy card
        candidates = [{**r, "latest_price": r["p_now"], "roc_30d_pct": r["roc_pct"],
                       "recent_distinct_prices_30d": r.get("recent_distinct_prices")} for r in candidates]
        label = "on the rise"
    else:
        candidates = [r for r in rows(get("/good_buys", category_id=3, limit=500)) if (r["latest_price"] or 0) >= 50]
        label = "on sale"
    good = [r for r in candidates if (r.get("recent_distinct_prices_30d") or 0) >= MIN_PRICE_CHANGES]
    return (good or candidates)[0], label


def post_spotlight(week: int) -> str:
    card, label = spotlight_card(week)
    pid, sub = card["productId"], card["subTypeName"]
    title = f"{card['productName']}, {short_set(card['groupName'])}" + (f" {card['number']}" if card.get("number") else "")
    lines = [f"**Card of the week ({label}): {title}** 🔍",
             f"• Market price **{money(card['latest_price'])}** (TCGplayer), **{pct(card.get('roc_30d_pct'))}** in 30 days"]
    graded = get("/graded_prices", productId=pid, subTypeName=sub)
    if graded.get("available"):
        prices = graded["prices"]
        parts = [f"{name} {money(prices[key])[:-3]}" for key, name in
                 (("grade9", "Grade 9"), ("psa10", "PSA 10"), ("bgs10", "BGS 10"), ("cgc10", "CGC 10")) if prices.get(key)]
        if parts:
            lines += ["", f"**Graded** (prices from PriceCharting: <{graded['url']}>)", " · ".join(parts)]
    hold = get("/hold_check", productId=pid, subTypeName=sub, category_id=3)
    if hold.get("headline"):
        reason = (hold.get("reasons") or [""])[0]
        lines += ["", f"**6-Month Hold:** {hold['headline']} {reason}".strip()]
    lines += ["", f"Full card page: {card_link(pid, sub)}", NFA]
    return "\n".join(lines)


# ---- posting ----------------------------------------------------------------------------

def discord_post(token: str, channel_id: str, content: str) -> str:
    body = json.dumps({"content": content, "allowed_mentions": {"parse": []}, "flags": 4}).encode()
    for attempt in range(3):
        request = urllib.request.Request(f"{DISCORD}/channels/{channel_id}/messages", data=body, method="POST",
                                         headers={"Authorization": f"Bot {token}", "Content-Type": "application/json", "User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)["id"]
        except urllib.error.HTTPError as error:
            payload = json.loads(error.read() or b"{}")
            if error.code == 429 and attempt < 2:
                time.sleep(min(float(payload.get("retry_after", 1.0)), 30.0) + 0.25)
                continue
            raise RuntimeError(f"Discord post failed: HTTP {error.code} {payload.get('message', '')}") from None
    raise RuntimeError("Discord kept rate-limiting us")


def main() -> int:
    parser = argparse.ArgumentParser(description="Post the weekly Poke6s Discord updates.")
    parser.add_argument("--dry-run", action="store_true", help="Print the posts; post nothing.")
    parser.add_argument("--force", action="store_true", help="Post even if this week was already posted.")
    args = parser.parse_args()

    today = dt.date.today()
    week_key = f"{today.isocalendar().year}-W{today.isocalendar().week:02d}"
    week = today.isocalendar().week
    state = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}
    if not args.dry_run and not args.force and state.get("last_week") == week_key:
        print(f"Already posted for {week_key}; nothing to do.")
        return 0

    builders = {"buy": lambda: post_buy(today), "market": lambda: post_market(today, week),
                "sets": lambda: post_set(week), "spotlight": lambda: post_spotlight(week)}
    posts = {}
    for key, build in builders.items():
        try:
            posts[key] = build()
        except Exception as error:  # one broken post shouldn't stop the others
            print(f"WARNING: could not build the {key} post: {error}")
    for key, text in posts.items():
        if len(text) > 2000:
            posts[key] = text[:1990].rsplit("\n", 1)[0]
    if args.dry_run:
        for key, text in posts.items():
            print(f"===== #{key} ({len(text)} chars) =====\n{text}\n")
        return 0

    token = setting("POKE6S_DISCORD_BOT_TOKEN")
    if not token:
        print("ERROR: POKE6S_DISCORD_BOT_TOKEN is not set.")
        return 1
    posted = {}
    for key, text in posts.items():
        try:
            posted[key] = discord_post(token, CHANNELS[key], text)
            print(f"POSTED #{key}: {posted[key]}")
        except RuntimeError as error:
            print(f"ERROR posting #{key}: {error}")
        time.sleep(1.2)
    if posted:
        state.update({"last_week": week_key, "posted_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                      "messages": posted})
        STATE_FILE.write_text(json.dumps(state, indent=1))
    return 0 if len(posted) == len(posts) else 1


if __name__ == "__main__":
    raise SystemExit(main())
