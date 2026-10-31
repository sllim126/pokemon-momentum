"""SQLite access and schema migrations.

Migrations are plain SQL files in `storefront/app/migrations/`, named `NNN_description.sql`
and applied in order at start-up (and by the importer). Each file runs once; the applied
versions are recorded in `schema_migrations`. Never edit a migration that has been applied
somewhere: add a new file instead.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


def connect(db_path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    """Open the store database.

    The public site opens it read-only, so a bug in a page handler can't change products or
    orders (plan: "The public app can't change products or orders").
    """
    if readonly:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False)
    else:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def migrate(db_path: Path) -> list[str]:
    """Apply any migrations not yet applied. Returns the versions applied this call."""
    conn = connect(db_path)
    try:
        # WAL lets the public site read while the importer or admin writes.
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        done = {row["version"] for row in conn.execute("SELECT version FROM schema_migrations")}
        applied: list[str] = []
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            version = path.stem
            if version in done:
                continue
            # executescript commits on its own, so wrap the file and its record in one
            # explicit transaction: a failing migration leaves no half-built schema.
            sql = path.read_text(encoding="utf-8")
            conn.executescript(
                "BEGIN;\n" + sql + "\n"
                f"INSERT INTO schema_migrations (version, applied_at) VALUES ('{version}', "
                "strftime('%Y-%m-%dT%H:%M:%SZ','now'));\nCOMMIT;"
            )
            applied.append(version)
        return applied
    finally:
        conn.close()


def latest_migration(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT max(version) AS v FROM schema_migrations").fetchone()
    return row["v"] if row else None
