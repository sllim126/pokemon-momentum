"""Export this project's Codex chat threads to Markdown for review.

Codex (the VS Code extension) keeps every thread in ~/.codex:
  - state_5.sqlite          `threads` table: id, title/name, cwd, dates
  - thread_history_1.sqlite `thread_items` table: one JSON row per message,
                            command, file edit, etc., ordered by rollout_ordinal

Only threads whose working directory is this repo are exported, and the
automated "Guardian review" sub-threads are skipped. Each thread becomes one
file in docs/chat_history/codex/ holding the owner's requests, Codex's replies and
the files it changed. Command output and reasoning are left out to keep the
files readable.

The output folder is git-ignored: the repo is public and the chats contain
business plans and pricing decisions. Any value from .env that appears in a
chat is replaced with [REDACTED:<NAME>] before writing.

Run on the host (not in the container, which can't see ~/.codex):
    python3 scripts/utilities/export_codex_history.py
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
CODEX = Path.home() / ".codex"
OUT_DIR = REPO / "docs" / "chat_history" / "codex"

# Patterns that look like credentials even when they aren't in .env.
TOKEN_PATTERNS = [
    re.compile(r"https://discord(?:app)?\.com/api/webhooks/\S+"),
    re.compile(r"(?i)\b(t|token|api_key|apikey|key)=[A-Za-z0-9_\-]{16,}"),
    re.compile(r"\bsk-[A-Za-z0-9_\-]{20,}"),
    re.compile(r"\btcg_[0-9a-f]{20,}"),  # JustTCG API keys
    re.compile(r"(?i)bearer\s+[A-Za-z0-9_\-\.]{20,}"),
    # Values pasted as `API_KEY = "..."` or `api_key: ...`.
    re.compile(r"""(?i)\b\w*(?:api[_-]?key|token|secret)\b["']?\s*[:=]\s*["']?[A-Za-z0-9_\-]{16,}"""),
    # A UUID-style key pasted in prose ("Here's my api key: 1234abcd-..."). Bare
    # UUIDs elsewhere are product/listing IDs and are kept.
    re.compile(r"(?i)\b(?:key|token)\b[^\n]{0,30}?[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
    # Inline passwords, e.g. a CIFS mount line `password=...,vers=3.0`.
    re.compile(r"(?i)\b(?:password|passwd|pwd)\s*[=:]\s*[^\s,;\"'`)]{4,}"),
]

# Placeholders that look like credentials but aren't; left alone so the
# redaction count stays meaningful.
PLACEHOLDER = re.compile(r"(?i)your_|xxxx|<[^>]+>")


def load_env_secrets() -> dict[str, str]:
    """Return {value: NAME} for every non-trivial .env value."""
    secrets: dict[str, str] = {}
    env = REPO / ".env"
    if not env.exists():
        return secrets
    for line in env.read_text().splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            continue
        name, value = line.split("=", 1)
        value = value.strip().strip("'\"")
        if len(value) >= 8:
            secrets[value] = name.strip()
    return secrets


def redact(text: str, secrets: dict[str, str]) -> str:
    # Longest values first so a value containing another is replaced whole.
    for value in sorted(secrets, key=len, reverse=True):
        if value in text:
            text = text.replace(value, f"[REDACTED:{secrets[value]}]")
    for pattern in TOKEN_PATTERNS:
        text = pattern.sub(lambda m: m.group(0) if PLACEHOLDER.search(m.group(0)) else "[REDACTED]", text)
    return text


def user_text(item: dict) -> str:
    """The owner's words, without the IDE context Codex prepends."""
    parts = []
    for c in item.get("content", []):
        if c.get("type") == "text":
            parts.append(c.get("text", ""))
        else:
            parts.append(f"[{c.get('type', 'attachment')} attached]")
    text = "\n".join(parts)
    marker = "## My request for Codex:"
    if marker in text:
        text = text.split(marker, 1)[1]
    return text.strip()


def slugify(title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return slug[:60] or "untitled"


def export_thread(hist: sqlite3.Connection, thread: dict, secrets: dict[str, str]) -> tuple[str, int]:
    rows = hist.execute(
        "SELECT turn_id, item_type, item_json FROM thread_items "
        "WHERE thread_id = ? ORDER BY rollout_ordinal",
        (thread["id"],),
    ).fetchall()

    lines = [
        f"# {thread['title']}",
        "",
        f"- Codex thread: `{thread['id']}`",
        f"- Started: {thread['created']}  ·  Last activity: {thread['updated']}",
        f"- Branch at start: `{thread['branch'] or 'unknown'}`",
        "",
    ]
    turn = None
    files: list[str] = []
    commands = 0
    requests = 0

    def flush_turn_footer():
        nonlocal files, commands
        if files:
            lines.append("**Files changed:** " + ", ".join(f"`{f}`" for f in dict.fromkeys(files)))
            lines.append("")
        if commands:
            lines.append(f"_({commands} shell command{'s' if commands != 1 else ''} run)_")
            lines.append("")
        files, commands = [], 0

    for turn_id, item_type, raw in rows:
        item = json.loads(raw)
        if turn_id != turn:
            flush_turn_footer()
            turn = turn_id
        if item_type == "userMessage":
            requests += 1
            lines += ["---", "", f"## Owner ({requests})", "", user_text(item), ""]
        elif item_type == "agentMessage":
            text = item.get("text", "").strip()
            if not text:
                continue
            if item.get("phase") == "commentary":
                lines += [f"> _Codex:_ {text}".replace("\n", "\n> "), ""]
            else:
                lines += ["**Codex:**", "", text, ""]
        elif item_type == "fileChange":
            for change in item.get("changes", []):
                files.append(change.get("path", "?").replace(str(REPO) + "/", ""))
        elif item_type == "commandExecution":
            commands += 1
        elif item_type == "contextCompaction":
            lines += ["_[Codex compacted the conversation context here]_", ""]
    flush_turn_footer()
    return redact("\n".join(lines), secrets), requests


def main() -> None:
    state = sqlite3.connect(f"file:{CODEX / 'state_5.sqlite'}?mode=ro", uri=True)
    hist = sqlite3.connect(f"file:{CODEX / 'thread_history_1.sqlite'}?mode=ro", uri=True)
    secrets = load_env_secrets()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    threads = state.execute(
        "SELECT id, COALESCE(name, title), created_at, updated_at, git_branch FROM threads "
        "WHERE cwd = ? AND COALESCE(thread_source, '') NOT LIKE '%guardian%' "
        "AND source NOT LIKE '%subagent%' ORDER BY created_at",
        (str(REPO),),
    ).fetchall()

    index = ["# Codex chat history", "", "| Date | Thread | Requests | File |", "|---|---|---|---|"]
    for tid, title, created, updated, branch in threads:
        # Untitled threads are named after their first message; keep line one.
        title = title.strip().splitlines()[0][:90]
        fmt = lambda ts: datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        thread = {"id": tid, "title": title, "created": fmt(created), "updated": fmt(updated), "branch": branch}
        body, requests = export_thread(hist, thread, secrets)
        if requests == 0:
            continue
        name = f"{fmt(created)[:10]}_{slugify(title)}_{tid[:13]}.md"
        (OUT_DIR / name).write_text(body + "\n")
        index.append(f"| {fmt(created)[:10]} | {title} | {requests} | [{name}]({name}) |")
        print(f"{name}  ({requests} requests)")

    (OUT_DIR / "README.md").write_text("\n".join(index) + "\n")


if __name__ == "__main__":
    main()
