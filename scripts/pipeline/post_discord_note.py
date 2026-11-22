"""Post a short note to the owner's Discord job channel (the same webhook as notify_discord.py).

For one-off messages that aren't a pipeline job summary, such as "the Delta Reign draft has
fresh prices and is ready for your final read". Stdlib only, runs on the host:

    python3 scripts/pipeline/post_discord_note.py --title "Delta Reign: ready for review" \
        --body "Master set $934 -> $901 ..." --url https://poke6s.com/articles/...

The body can also come from stdin (`--body -`). Uses POKE6S_DISCORD_WEBHOOK_URL from the
environment or .env; without it the message is only printed. Mentions nobody unless
--mention is given. Never prints the webhook URL.
"""

import argparse
import json
import sys
import urllib.error

from notify_discord import post, setting

BLUE = 0x4096F0  # an informational note, not a job result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--title", required=True)
    parser.add_argument("--body", required=True, help="Message text, or - to read it from stdin.")
    parser.add_argument("--url", help="Link the title points to.")
    parser.add_argument("--mention", default="", help='For example "@everyone"; default: nobody.')
    parser.add_argument("--dry-run", action="store_true", help="Print the payload instead of posting it.")
    args = parser.parse_args()

    body = sys.stdin.read() if args.body == "-" else args.body
    embed = {"title": args.title[:256], "description": body.strip()[:4000], "color": BLUE}
    if args.url:
        embed["url"] = args.url
    payload = {"content": args.mention, "embeds": [embed],
               "allowed_mentions": {"parse": ["everyone"] if args.mention else []}}

    webhook = setting("POKE6S_DISCORD_WEBHOOK_URL")
    if args.dry_run or not webhook:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0
    try:
        post(webhook, payload)
    except (urllib.error.URLError, OSError) as exc:
        print(f"Discord post failed: {exc}", file=sys.stderr)
        return 1
    print("Posted to Discord.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
