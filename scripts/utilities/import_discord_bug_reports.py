"""Copy new messages from the Discord #bug-reports channel into the site's bug report queue.

Visitors report bugs in Discord (there is no on-site bug form). This job reads the channel
through the "Poke6s Control" bot, saves each new message as a bug report (the same
`bug_reports` table the admin page /bug-reports shows), then adds a ✅ reaction to the
message so everyone can see it was captured.

- Reads only the bug-reports channel (POKE6S_DISCORD_BUG_CHANNEL_ID, default below).
- Remembers the last message it saw (table discord_import_state), and also skips any
  message already saved, so running it twice never duplicates a report.
- Skips messages from bots, Discord system messages, and empty messages.
- Never changes or deletes anything in Discord except adding its own ✅ reaction.

Settings: POKE6S_DISCORD_BOT_TOKEN (required) and POKE6S_DISCORD_BUG_CHANNEL_ID, from the
environment or the repo's .env. The token is never printed.

Runs inside the website container (it writes data/processed/tracking.sqlite3, which belongs
to root), every 15 minutes from user adam's crontab:

    docker-compose exec -T pokemon-momentum python scripts/utilities/import_discord_bug_reports.py

Add --dry-run to list what would be imported without saving or reacting.
Exit codes: 0 ok, 1 Discord or settings error.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.dashboards.tracking_store import create_bug_report, ensure_tracking_schema, get_con, utc_now_iso

API = "https://discord.com/api/v10"
DEFAULT_CHANNEL_ID = "1489120381993357464"  # #bug-reports in the Poke6s server
USER_AGENT = "DiscordBot (https://market.poke6s.com, 1.0)"
CAPTURED_EMOJI = "✅"  # ✅
NORMAL_MESSAGE_TYPES = {0, 19}  # 0 = message, 19 = reply; everything else is a system message
TITLE_LIMIT = 120


def setting(name: str, default: str = "") -> str:
    """A setting from the environment, falling back to the repo's .env file."""
    value = os.getenv(name, "").strip()
    env_file = ROOT / ".env"
    if not value and env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, _, raw = line.partition("=")
            if key.strip() == name:
                value = raw.strip().strip("'\"")
    return value or default


def discord(method: str, path: str, token: str) -> object:
    """One Discord API call; when Discord asks us to slow down (429) it waits and retries."""
    for attempt in range(3):
        request = urllib.request.Request(
            API + path,
            method=method,
            headers={"Authorization": f"Bot {token}", "User-Agent": USER_AGENT, "Content-Length": "0"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read()
                return json.loads(body) if body else None
        except urllib.error.HTTPError as error:
            payload = json.loads(error.read() or b"{}")
            if error.code == 429 and attempt < 2:
                time.sleep(min(float(payload.get("retry_after", 1.0)), 30.0) + 0.25)
                continue
            raise RuntimeError(f"Discord {method} {path.split('?')[0]} failed: HTTP {error.code} {payload.get('message', '')}") from None
    raise RuntimeError(f"Discord {method} {path.split('?')[0]} kept rate-limiting us")


def ensure_state_table() -> None:
    """The last message id read, per channel."""
    ensure_tracking_schema()
    con = get_con()
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS discord_import_state ("
            "channel_id TEXT PRIMARY KEY, last_message_id TEXT NOT NULL, updated_at TEXT NOT NULL)"
        )
        con.commit()
    finally:
        con.close()


def last_seen(channel_id: str) -> str:
    con = get_con()
    try:
        row = con.execute("SELECT last_message_id FROM discord_import_state WHERE channel_id = ?", [channel_id]).fetchone()
        return row[0] if row else "0"
    finally:
        con.close()


def save_last_seen(channel_id: str, message_id: str) -> None:
    con = get_con()
    try:
        con.execute(
            "INSERT INTO discord_import_state (channel_id, last_message_id, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(channel_id) DO UPDATE SET last_message_id = excluded.last_message_id, updated_at = excluded.updated_at",
            [channel_id, message_id, utc_now_iso()],
        )
        con.commit()
    finally:
        con.close()


def already_saved(message_id: str) -> bool:
    """True if a report for this Discord message exists (guards against re-imports)."""
    con = get_con()
    try:
        row = con.execute(
            "SELECT 1 FROM bug_reports WHERE context_json LIKE ? LIMIT 1",
            [f'%"discord_message_id": "{message_id}"%'],
        ).fetchone()
        return row is not None
    finally:
        con.close()


def new_messages(channel_id: str, after: str, token: str) -> list[dict]:
    """Every message newer than `after`, oldest first (Discord returns 100 per page)."""
    messages: list[dict] = []
    while True:
        page = discord("GET", f"/channels/{channel_id}/messages?limit=100&after={after}", token)
        if not page:
            break
        page = sorted(page, key=lambda m: int(m["id"]))
        messages.extend(page)
        after = page[-1]["id"]
        if len(page) < 100:
            break
    return messages


def to_report(message: dict, guild_id: str | None, channel_id: str) -> dict:
    """The bug_reports row for one Discord message."""
    author = message.get("author", {})
    name = author.get("global_name") or author.get("username") or "unknown"
    content = (message.get("content") or "").strip()
    attachments = [{"filename": a.get("filename", "file"), "url": a.get("url", "")} for a in message.get("attachments", [])]
    first_line = content.splitlines()[0] if content else ""
    if first_line:
        title = first_line[:TITLE_LIMIT] + ("…" if len(first_line) > TITLE_LIMIT else "")
    else:
        title = "Discord bug report (attachment only)"
    jump_url = f"https://discord.com/channels/{guild_id or '@me'}/{channel_id}/{message['id']}"
    context = {
        "source": "discord",
        "discord_message_id": message["id"],
        "discord_url": jump_url,
        "discord_author": author.get("username"),
        "discord_author_id": author.get("id"),
        "posted_at": message.get("timestamp"),
        "attachments": attachments,
    }
    return {
        "reporter_username": f"discord:{name}",
        "page_path": "discord #bug-reports",
        "page_url": jump_url,
        "title": title,
        "details": content or "(No text; see the attachments.)",
        "context_json": json.dumps(context, ensure_ascii=False),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import new #bug-reports messages from Discord.")
    parser.add_argument("--dry-run", action="store_true", help="List what would be imported; save and react to nothing.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    token = setting("POKE6S_DISCORD_BOT_TOKEN")
    channel_id = setting("POKE6S_DISCORD_BUG_CHANNEL_ID", DEFAULT_CHANNEL_ID)
    if not token:
        print("ERROR: POKE6S_DISCORD_BOT_TOKEN is not set.")
        return 1
    ensure_state_table()
    try:
        channel = discord("GET", f"/channels/{channel_id}", token)
        guild_id = channel.get("guild_id") if isinstance(channel, dict) else None
        messages = new_messages(channel_id, last_seen(channel_id), token)
    except RuntimeError as error:
        print(f"ERROR: {error}")
        return 1

    imported = skipped = 0
    for message in messages:
        author = message.get("author", {})
        has_content = bool((message.get("content") or "").strip() or message.get("attachments"))
        if author.get("bot") or message.get("type") not in NORMAL_MESSAGE_TYPES or not has_content or already_saved(message["id"]):
            skipped += 1
        elif args.dry_run:
            imported += 1
            print(f"WOULD IMPORT {message['id']} from {author.get('username')}: {to_report(message, guild_id, channel_id)['title']}")
        else:
            report_id = create_bug_report(to_report(message, guild_id, channel_id))
            imported += 1
            print(f"IMPORTED {message['id']} as bug report #{report_id}")
            try:
                emoji = urllib.parse.quote(CAPTURED_EMOJI)
                discord("PUT", f"/channels/{channel_id}/messages/{message['id']}/reactions/{emoji}/@me", token)
            except RuntimeError as error:
                print(f"WARNING: saved, but couldn't add the reaction: {error}")
            time.sleep(0.3)
        if not args.dry_run:
            save_last_seen(channel_id, message["id"])

    print(f"Discord #bug-reports: {len(messages)} new message(s), {imported} imported, {skipped} skipped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
