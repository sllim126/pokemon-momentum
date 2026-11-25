"""Announce new blog posts in the Poke6s Discord's #general-site-updates channel.

A post goes live either when it is merged with today's date or, when scheduled, at midnight
Mountain time on its date, with nothing to trigger a message. So this runs hourly from the
host crontab (daytime only, see deploy/crontab.txt) and asks the store itself what is live:
the article addresses in the local storefront's sitemap (http://localhost:8010/sitemap.xml)
are exactly the published posts, since drafts and scheduled posts are left out of it.

Each address not yet announced gets one message, "New on the blog: <title>" plus the link;
Discord's link preview adds the description and cover picture from the page's og: tags. The
announced addresses are kept in data/discord_article_posts.json. The first run, with no state
file, only records what is already live, so the back catalogue is never posted.

    python3 scripts/utilities/announce_articles_discord.py --dry-run   # print, post nothing
    python3 scripts/utilities/announce_articles_discord.py             # post new posts

Settings: POKE6S_DISCORD_BOT_TOKEN from the environment or .env (never printed), the same bot
as post_weekly_discord.py. If the store doesn't answer, nothing is posted or recorded and the
next hourly run tries again. Exit codes: 0 ok, 1 error.
"""

import argparse
import html
import json
import re
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from post_weekly_discord import DISCORD, ROOT, USER_AGENT, setting  # noqa: E402

STORE = "http://localhost:8010"
SITE = "https://poke6s.com"
CHANNEL = "1497495197234237522"  # #general-site-updates
STATE_FILE = ROOT / "data" / "discord_article_posts.json"
ARTICLE_URL = re.compile(r"<loc>(https://poke6s\.com/articles/[^<]+)</loc>")


def fetch(url: str) -> str:
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read().decode("utf-8")


def live_articles() -> list[str]:
    """Addresses of every published article, from the store's sitemap."""
    return ARTICLE_URL.findall(fetch(f"{STORE}/sitemap.xml"))


def title_of(url: str) -> str:
    """The post's title, from its og:title tag (read from the local store, not Cloudflare)."""
    page = fetch(STORE + url.removeprefix(SITE))
    found = re.search(r'property="og:title" content="([^"]*)"', page)
    return html.unescape(found.group(1)) if found else url.rsplit("/", 1)[-1]


def post(text: str) -> None:
    """Send one message as the Poke6s Control bot. Link previews stay on (that's the point)."""
    request = urllib.request.Request(
        f"{DISCORD}/channels/{CHANNEL}/messages",
        data=json.dumps({"content": text, "allowed_mentions": {"parse": []}}).encode(),
        headers={"Authorization": f"Bot {setting('POKE6S_DISCORD_BOT_TOKEN')}",
                 "Content-Type": "application/json", "User-Agent": USER_AGENT},
    )
    urllib.request.urlopen(request, timeout=30).close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="print what would be posted")
    args = parser.parse_args()

    live = live_articles()
    if not STATE_FILE.exists():
        # First run: everything already live counts as announced.
        if not args.dry_run:
            STATE_FILE.write_text(json.dumps({"announced": sorted(live)}, indent=1) + "\n")
        print(f"first run: recorded {len(live)} live articles, posted nothing")
        return 0

    announced = set(json.loads(STATE_FILE.read_text())["announced"])
    new = [url for url in live if url not in announced]
    for url in new:
        text = f"📰 New on the blog: **{title_of(url)}**\n{url}"
        print(("would post: " if args.dry_run else "posting: ") + text.replace("\n", "  "))
        if not args.dry_run:
            post(text)
            # Saved after each message, so a failure part-way never posts the same one twice.
            announced.add(url)
            STATE_FILE.write_text(json.dumps({"announced": sorted(announced)}, indent=1) + "\n")
    if not new:
        print("nothing new")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:  # noqa: BLE001  (logged; the next hourly run retries)
        print(f"error: {type(error).__name__}: {error}")
        sys.exit(1)
