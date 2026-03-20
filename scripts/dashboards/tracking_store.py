from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import secrets
import sqlite3
from pathlib import Path


# Tracking data intentionally lives outside the market-history database so lightweight
# auth/tag writes do not interfere with the analytical DuckDB pipeline.
TRACKING_DB_PATH = Path("/app/data/processed/tracking.sqlite3")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_con() -> sqlite3.Connection:
    TRACKING_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(TRACKING_DB_PATH))
    con.row_factory = sqlite3.Row
    return con


def ensure_tracking_schema() -> None:
    """Create the lightweight tracking account/session/tag tables when missing."""
    con = get_con()
    try:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS tracking_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                pin_salt TEXT NOT NULL,
                pin_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS tracking_sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES tracking_users(id)
            );

            CREATE TABLE IF NOT EXISTS tracking_tags (
                user_id INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                sub_type_name TEXT NOT NULL,
                tag TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (user_id, category_id, product_id, sub_type_name, tag),
                FOREIGN KEY(user_id) REFERENCES tracking_users(id)
            );
            """
        )
        con.commit()
    finally:
        con.close()


def normalize_username(username: str) -> str:
    return (username or "").strip().lower()


def hash_pin(pin: str, salt: str) -> str:
    return hashlib.scrypt(
        pin.encode("utf-8"),
        salt=bytes.fromhex(salt),
        n=2**14,
        r=8,
        p=1,
    ).hex()


def create_user(username: str, pin: str) -> int:
    ensure_tracking_schema()
    normalized = normalize_username(username)
    salt = secrets.token_hex(16)
    now = utc_now_iso()
    con = get_con()
    try:
        cur = con.execute(
            """
            INSERT INTO tracking_users (username, pin_salt, pin_hash, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [normalized, salt, hash_pin(pin, salt), now, now],
        )
        con.commit()
        return int(cur.lastrowid)
    finally:
        con.close()


def get_user_by_username(username: str) -> sqlite3.Row | None:
    ensure_tracking_schema()
    con = get_con()
    try:
        return con.execute(
            "SELECT * FROM tracking_users WHERE username = ?",
            [normalize_username(username)],
        ).fetchone()
    finally:
        con.close()


def verify_user(username: str, pin: str) -> sqlite3.Row | None:
    row = get_user_by_username(username)
    if row is None:
        return None
    expected = row["pin_hash"]
    actual = hash_pin(pin, row["pin_salt"])
    if not hmac.compare_digest(expected, actual):
        return None
    return row


def create_session(user_id: int) -> str:
    ensure_tracking_schema()
    token = secrets.token_urlsafe(32)
    now = utc_now_iso()
    con = get_con()
    try:
        con.execute(
            """
            INSERT INTO tracking_sessions (token, user_id, created_at, last_seen_at)
            VALUES (?, ?, ?, ?)
            """,
            [token, user_id, now, now],
        )
        con.commit()
        return token
    finally:
        con.close()


@dataclass
class SessionUser:
    user_id: int
    username: str


def get_session_user(token: str) -> SessionUser | None:
    ensure_tracking_schema()
    con = get_con()
    try:
        row = con.execute(
            """
            SELECT s.user_id, u.username
            FROM tracking_sessions s
            JOIN tracking_users u
              ON u.id = s.user_id
            WHERE s.token = ?
            """,
            [token],
        ).fetchone()
        if row is None:
            return None
        con.execute(
            "UPDATE tracking_sessions SET last_seen_at = ? WHERE token = ?",
            [utc_now_iso(), token],
        )
        con.commit()
        return SessionUser(user_id=int(row["user_id"]), username=row["username"])
    finally:
        con.close()


def delete_session(token: str) -> None:
    ensure_tracking_schema()
    con = get_con()
    try:
        con.execute("DELETE FROM tracking_sessions WHERE token = ?", [token])
        con.commit()
    finally:
        con.close()


def get_tags_for_user(user_id: int) -> list[dict]:
    ensure_tracking_schema()
    con = get_con()
    try:
        rows = con.execute(
            """
            SELECT category_id, product_id, sub_type_name, tag
            FROM tracking_tags
            WHERE user_id = ?
            ORDER BY category_id, product_id, sub_type_name, tag
            """,
            [user_id],
        ).fetchall()
        return [
            {
                "category_id": int(row["category_id"]),
                "product_id": int(row["product_id"]),
                "sub_type_name": row["sub_type_name"],
                "tag": row["tag"],
            }
            for row in rows
        ]
    finally:
        con.close()


def set_tag(user_id: int, category_id: int, product_id: int, sub_type_name: str, tag: str, enabled: bool) -> None:
    ensure_tracking_schema()
    now = utc_now_iso()
    con = get_con()
    try:
        if enabled:
            con.execute(
                """
                INSERT INTO tracking_tags (
                    user_id, category_id, product_id, sub_type_name, tag, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, category_id, product_id, sub_type_name, tag)
                DO UPDATE SET updated_at = excluded.updated_at
                """,
                [user_id, category_id, product_id, sub_type_name, tag, now, now],
            )
        else:
            con.execute(
                """
                DELETE FROM tracking_tags
                WHERE user_id = ? AND category_id = ? AND product_id = ? AND sub_type_name = ? AND tag = ?
                """,
                [user_id, category_id, product_id, sub_type_name, tag],
            )
        con.commit()
    finally:
        con.close()


def merge_tags(user_id: int, items: list[dict]) -> None:
    ensure_tracking_schema()
    now = utc_now_iso()
    con = get_con()
    try:
        for item in items:
            con.execute(
                """
                INSERT INTO tracking_tags (
                    user_id, category_id, product_id, sub_type_name, tag, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, category_id, product_id, sub_type_name, tag)
                DO UPDATE SET updated_at = excluded.updated_at
                """,
                [
                    user_id,
                    int(item["category_id"]),
                    int(item["product_id"]),
                    str(item["sub_type_name"] or ""),
                    str(item["tag"]),
                    now,
                    now,
                ],
            )
        con.commit()
    finally:
        con.close()
