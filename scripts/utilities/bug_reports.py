"""Read and resolve the site's bug reports (the `bug_reports` table in data/processed/tracking.sqlite3).

Reports arrive three ways: the Discord #bug-reports channel (copied every 15 minutes by
import_discord_bug_reports.py), the phone app's feedback form, and (older) the market site. The
store admin's Overview lists the open ones. When the owner asks an agent to "look at bug report
#N", the agent reads it here, fixes the problem, and marks it resolved:

    docker-compose exec -T pokemon-momentum python scripts/utilities/bug_reports.py list
    docker-compose exec -T pokemon-momentum python scripts/utilities/bug_reports.py show 12
    docker-compose exec -T pokemon-momentum python scripts/utilities/bug_reports.py resolve 12

`list` shows open reports (`--all` for every one). `resolve` sets the status to "resolved"
(`--status duplicate` / `wontfix` / `new` to set another or reopen). Run inside the website
container: the database belongs to root on the host. Reports can hold a visitor's Discord name
or email: print them in the terminal, never paste them into commits or public places.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.dashboards import tracking_store  # noqa: E402

STATUSES = ("new", "resolved", "duplicate", "wontfix")
OPEN = ("new",)


def where_from(page_path: str | None) -> str:
    """Where a report came from, from its page_path (same rule as the admin Overview's _bug_source)."""
    path = (page_path or "").lower()
    if path.startswith("discord"):
        return "Discord"
    if path.startswith("/m"):
        return "Phone app"
    return "Market site"


def list_reports(show_all: bool = False) -> list[dict]:
    """Reports oldest first: the open ones (status in OPEN), or every report with show_all."""
    con = tracking_store.get_con()
    try:
        sql = "SELECT id, created_at, status, page_path, title FROM bug_reports"
        if not show_all:
            sql += f" WHERE coalesce(status, 'new') IN ({','.join('?' * len(OPEN))})"
        sql += " ORDER BY id"
        return [dict(row) for row in con.execute(sql, () if show_all else OPEN)]
    finally:
        con.close()


def get_report(report_id: int) -> dict | None:
    """One report with every column, or None when there's no such id."""
    con = tracking_store.get_con()
    try:
        row = con.execute("SELECT * FROM bug_reports WHERE id = ?", (report_id,)).fetchone()
        return dict(row) if row else None
    finally:
        con.close()


def set_status(report_id: int, status: str) -> bool:
    """Change one report's status. False when there's no such report."""
    if status not in STATUSES:
        raise ValueError(f"status must be one of {', '.join(STATUSES)}")
    con = tracking_store.get_con()
    try:
        with con:
            return con.execute("UPDATE bug_reports SET status = ? WHERE id = ?", (status, report_id)).rowcount == 1
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    listing = sub.add_parser("list", help="open reports (or --all)")
    listing.add_argument("--all", action="store_true")
    show = sub.add_parser("show", help="one report in full")
    show.add_argument("id", type=int)
    resolve = sub.add_parser("resolve", help="mark a report resolved (or --status ...)")
    resolve.add_argument("id", type=int)
    resolve.add_argument("--status", default="resolved", choices=STATUSES)
    args = parser.parse_args(argv)

    if args.command == "list":
        rows = list_reports(args.all)
        for row in rows:
            print(f"#{row['id']:<4} {row['created_at'][:10]}  {row['status']:<9} {where_from(row['page_path']):<11} "
                  f"{row['title']}")
        print(f"{len(rows)} report(s)" + ("" if args.all else " open"))
        return 0
    if args.command == "show":
        report = get_report(args.id)
        if report is None:
            print(f"No bug report #{args.id}", file=sys.stderr)
            return 1
        for key in ("id", "created_at", "status", "reporter_username", "page_url", "tab", "search_query",
                    "title", "expected", "details"):
            if report.get(key) not in (None, ""):
                print(f"{key}: {report[key]}")
        context = json.loads(report.get("context_json") or "{}")
        if context:
            print("context:", json.dumps(context, indent=2, ensure_ascii=False))
        return 0
    if not set_status(args.id, args.status):
        print(f"No bug report #{args.id}", file=sys.stderr)
        return 1
    print(f"Bug report #{args.id}: {args.status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
