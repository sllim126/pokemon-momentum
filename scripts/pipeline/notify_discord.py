"""Post a job summary (nightly update or Squarespace price sync) to a Discord webhook.

Runs on the host (stdlib only) at the end of run_daily_update_host.sh and
run_squarespace_price_sync.sh, so it still reports when Docker or the job itself is
what broke. Settings come from the environment or the repo's .env file:

- POKE6S_DISCORD_WEBHOOK_URL: channel webhook (without it the message is only printed)
- POKE6S_DISCORD_MENTION: who to ping on failures/warnings, default "@everyone";
  set to "none" to disable

It never fails the calling job.
"""

import argparse
import csv
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EXTRACTED_DIR = ROOT / "data" / "extracted"
CATEGORIES = {3: ("English", "pokemon"), 85: ("Japanese", "pokemon_jp")}
STALE_AFTER_DAYS = 2
GREEN, ORANGE, RED = 0x2ECC71, 0xE67E22, 0xE74C3C
ERROR_LINE = re.compile(r"(ERROR|Traceback|Stopping after failed step|^- .+: exit \d+|exit=[1-9])")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", choices=sorted(JOBS), default="daily-update")
    parser.add_argument("--exit-code", type=int, required=True, help="Exit status of the job.")
    parser.add_argument("--log-file", type=Path, help="Defaults to the job's log under logs/.")
    parser.add_argument("--log-offset", type=int, default=0, help="Byte offset where this run's log output starts.")
    parser.add_argument("--started-at", help="UTC ISO timestamp when the run started.")
    parser.add_argument("--title", help="Defaults to the job's title.")
    parser.add_argument("--dry-run", action="store_true", help="Print the payload instead of posting it.")
    return parser.parse_args()


def setting(name: str, default: str = "") -> str:
    value = os.getenv(name, "").strip()
    env_file = ROOT / ".env"
    if not value and env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, _, raw = line.partition("=")
            if key.strip() == name:
                value = raw.strip().strip("'\"")
    return value or default


def read_health(slug: str) -> dict | None:
    path = EXTRACTED_DIR / f"{slug}_health_snapshot.csv"
    try:
        with path.open(newline="", encoding="utf-8") as f:
            return next(csv.DictReader(f))
    except (OSError, StopIteration):
        return None


def run_log(path: Path, offset: int) -> list[str]:
    try:
        with path.open("rb") as f:
            f.seek(offset)
            return f.read().decode("utf-8", errors="replace").splitlines()
    except OSError:
        return []


def error_field(lines: list[str]) -> dict:
    errors = [line.strip() for line in lines if ERROR_LINE.search(line)]
    detail = "\n".join(errors[-12:]) or "No error lines captured; check the log."
    return {"name": "Errors", "value": f"```\n{detail[-950:]}\n```", "inline": False}


def daily_update_summary(args: argparse.Namespace, lines: list[str]) -> tuple[str, int, list[dict]]:
    today = datetime.now(timezone.utc).date()
    stale = False
    fields = []
    for category_id, (label, slug) in CATEGORIES.items():
        health = read_health(slug)
        if health is None:
            stale = True
            fields.append({"name": label, "value": "no health snapshot found", "inline": True})
            continue
        latest = date.fromisoformat(health["latest"])
        age = (today - latest).days
        stale = stale or age > STALE_AFTER_DAYS
        flag = " ⚠️ stale" if age > STALE_AFTER_DAYS else ""
        rows = f"{int(health['rows']):,}" if health.get("rows", "").isdigit() else health.get("rows", "?")
        fields.append({
            "name": label,
            "value": f"latest **{latest}** ({age}d old){flag}\n{rows} price rows",
            "inline": True,
        })

    loaded = [
        re.sub(r"^\[\d+/\d+\] DONE ", "", line.strip())
        for line in lines
        if "] DONE " in line and "rows=" in line and not line.rstrip().endswith("rows=0")
    ]
    fields.append({
        "name": "Loaded this run",
        "value": "\n".join(loaded[-4:])[:1000] if loaded else "no new price rows",
        "inline": False,
    })

    if any("store pricing coverage audit reported" in line for line in lines):
        fields.append({"name": "Store audit", "value": "⚠️ missing_unexpected SKUs (see log)", "inline": False})

    if args.exit_code != 0:
        fields.append(error_field(lines))
        return f"❌ FAILED (exit {args.exit_code})", RED, fields
    if stale:
        return "⚠️ finished, but data is stale", ORANGE, fields
    return "✅ succeeded", GREEN, fields


def price_sync_summary(args: argparse.Namespace, lines: list[str]) -> tuple[str, int, list[dict]]:
    def first_value(prefix: str) -> str:
        return next((line.split(":", 1)[1].strip() for line in lines if line.startswith(prefix)), "?")

    updated = [line for line in lines if line.startswith("UPDATED ")]
    failed = [line.strip() for line in lines if line.startswith(("FAILED ", "ERROR "))]
    missing = sum(1 for line in lines if "missing from Squarespace export" in line)
    fields = [
        {"name": "Matched SKUs", "value": first_value("Matched SKUs"), "inline": True},
        {"name": "Proposed", "value": first_value("Proposed updates"), "inline": True},
        {"name": "Updated", "value": str(len(updated)), "inline": True},
    ]
    if missing:
        fields.append({"name": "Rules not in Squarespace export", "value": str(missing), "inline": True})
    if failed:
        fields.append({
            "name": f"Failed updates ({len(failed)})",
            "value": f"```\n{chr(10).join(failed[:10])[:950]}\n```",
            "inline": False,
        })
    if args.exit_code != 0:
        fields.append(error_field(lines))
        return f"❌ FAILED (exit {args.exit_code})", RED, fields
    if failed:
        return f"⚠️ finished with {len(failed)} failed update(s)", ORANGE, fields
    return "✅ succeeded", GREEN, fields


JOBS = {
    "daily-update": ("Nightly market update", ROOT / "logs" / "daily_update.log", daily_update_summary),
    "price-sync": ("Squarespace price sync", ROOT / "logs" / "squarespace_price_sync_cron.log", price_sync_summary),
}


def build_payload(args: argparse.Namespace) -> dict:
    default_title, default_log, summarize = JOBS[args.job]
    log_file = args.log_file or default_log
    status, color, fields = summarize(args, run_log(log_file, args.log_offset))

    duration = ""
    if args.started_at:
        started = datetime.fromisoformat(args.started_at.replace("Z", "+00:00"))
        minutes = (datetime.now(timezone.utc) - started).total_seconds() / 60
        duration = f" in {minutes:.1f} min"

    payload = {
        "username": "Poke6s Pipeline",
        "embeds": [{
            "title": f"{args.title or default_title}: {status}",
            "description": f"Run finished{duration}. Log: `{log_file.relative_to(ROOT) if log_file.is_relative_to(ROOT) else log_file}`",
            "color": color,
            "fields": fields,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }],
        "allowed_mentions": {"parse": []},
    }
    mention = setting("POKE6S_DISCORD_MENTION", "@everyone")
    if color != GREEN and mention.lower() != "none":
        # In a private channel, @everyone only reaches members who can see it.
        payload["content"] = f"{mention} {args.title or default_title} needs attention"
        payload["allowed_mentions"] = {"parse": ["everyone", "users", "roles"]}
    return payload


def post(url: str, payload: dict) -> None:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        # Discord rejects urllib's default User-Agent.
        headers={"Content-Type": "application/json", "User-Agent": "Poke6sPipeline/1.0.0"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as resp:
        resp.read()


def main() -> int:
    args = parse_args()
    payload = build_payload(args)
    url = setting("POKE6S_DISCORD_WEBHOOK_URL")
    if args.dry_run or not url:
        if not url and not args.dry_run:
            print("POKE6S_DISCORD_WEBHOOK_URL is not set; Discord notification skipped.")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0
    try:
        post(url, payload)
        print("Discord notification sent.")
    except (urllib.error.URLError, OSError) as exc:
        print(f"Warning: Discord notification failed: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
