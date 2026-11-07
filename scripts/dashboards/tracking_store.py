"""Accounts, sign-in sessions, tags, saved views, bug reports and the PSA lookup cache (SQLite).

Kept in its own SQLite file, data/processed/tracking.sqlite3, not in the DuckDB price database:
the site only reads DuckDB, and the nightly update stops the site to write it, so small user
writes here never contend with the price pipeline. ensure_tracking_schema() creates or upgrades
the tables on first use. Each function opens and closes its own connection.

Security: PINs are salted scrypt hashes; session tokens are stored only as SHA-256 hashes
(see the constants below). The routes that call this live in api.py under /tracking/*,
/bug_reports and /api/psa/cert.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import secrets
import sqlite3
from pathlib import Path


# Tracking data intentionally lives outside the market-history database so lightweight
# auth/tag writes do not interfere with the analytical DuckDB pipeline.
TRACKING_DB_PATH = Path("/app/data/processed/tracking.sqlite3")

# Sign-in sessions. A session ends after SESSION_IDLE_DAYS without use, or SESSION_MAX_DAYS
# after sign-in however much it is used; signing in with Google again starts a new one.
# Only a SHA-256 hash of each token is stored ("sha256:<hex>"), so a copy of this database
# can't be used to sign in. last_seen_at is written at most once per SESSION_TOUCH_MINUTES.
SESSION_IDLE_DAYS = 30
SESSION_MAX_DAYS = 180
SESSION_TOUCH_MINUTES = 60
TOKEN_HASH_PREFIX = "sha256:"


def hash_session_token(token: str) -> str:
    """How a session token is stored and looked up."""
    return TOKEN_HASH_PREFIX + hashlib.sha256(token.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_con() -> sqlite3.Connection:
    TRACKING_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(TRACKING_DB_PATH))
    con.row_factory = sqlite3.Row
    return con


def ensure_tracking_schema() -> None:
    """Create the lightweight tracking account/session/tag/bug-report tables when missing."""
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

            CREATE TABLE IF NOT EXISTS tracking_saved_views (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                category_id INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                ticker_enabled INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES tracking_users(id)
            );

            CREATE TABLE IF NOT EXISTS bug_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                reporter_username TEXT,
                page_path TEXT NOT NULL,
                page_url TEXT NOT NULL,
                category_id INTEGER,
                tab TEXT,
                segment TEXT,
                chart_mode TEXT,
                product_key TEXT,
                group_id INTEGER,
                search_query TEXT,
                title TEXT NOT NULL,
                expected TEXT,
                details TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'new',
                context_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS psa_certifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cert_number TEXT NOT NULL UNIQUE,
                is_valid_request INTEGER NOT NULL,
                server_message TEXT,
                raw_response_json TEXT NOT NULL,
                normalized_card_name TEXT,
                normalized_set_name TEXT,
                normalized_grade TEXT,
                lookup_status TEXT NOT NULL,
                last_checked_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            -- Budget Builder rotation (api.py, BUDGET_ROTATION_PENALTIES): the cards each
            -- builder showed by default for a price date, so later days can rest them.
            CREATE TABLE IF NOT EXISTS budget_pick_history (
                builder TEXT NOT NULL,
                category_id INTEGER NOT NULL,
                price_date TEXT NOT NULL,
                card_key TEXT NOT NULL,
                PRIMARY KEY (builder, category_id, price_date, card_key)
            );

            -- Track record (pick_tracker.py): the top of each Discover list for a price
            -- date, with the price it was picked at. Kept for good: the public track record
            -- is only honest if old picks can't quietly disappear.
            CREATE TABLE IF NOT EXISTS list_pick_history (
                list_key TEXT NOT NULL,
                category_id INTEGER NOT NULL,
                price_date TEXT NOT NULL,
                rank INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                sub_type_name TEXT NOT NULL,
                entry_price REAL NOT NULL,
                recorded_at TEXT NOT NULL,
                PRIMARY KEY (list_key, category_id, price_date, product_id, sub_type_name)
            );
            """
        )
        # One-time upgrade: sessions created before tokens were hashed are hashed in place,
        # so existing sign-ins keep working.
        plain = con.execute(
            "SELECT token FROM tracking_sessions WHERE token NOT LIKE ?", [TOKEN_HASH_PREFIX + "%"]
        ).fetchall()
        for (token,) in plain:
            con.execute("UPDATE tracking_sessions SET token = ? WHERE token = ?", [hash_session_token(token), token])
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
    """Create a username + PIN account and return its id. The PIN is stored only as a salted scrypt hash (hash_pin).

    Raises sqlite3.IntegrityError if the (normalised) username is taken (UNIQUE column).
    """
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


def create_google_user(email: str) -> int:
    """Create an account for a Google sign-in (username = email) and return its id.

    Google accounts have no PIN: pin_salt and pin_hash are empty, so verify_user can never match
    them and they can only sign in through Google.
    """
    ensure_tracking_schema()
    normalized = normalize_username(email)
    now = utc_now_iso()
    con = get_con()
    try:
        cur = con.execute(
            """
            INSERT INTO tracking_users (username, pin_salt, pin_hash, created_at, updated_at)
            VALUES (?, '', '', ?, ?)
            """,
            [normalized, now, now],
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


def _session_expired(created_at: str, last_seen_at: str, now: datetime) -> bool:
    """True when a session is past its idle or absolute limit (see SESSION_* above)."""
    created = datetime.fromisoformat(created_at)
    last_seen = datetime.fromisoformat(last_seen_at)
    return now - last_seen > timedelta(days=SESSION_IDLE_DAYS) or now - created > timedelta(days=SESSION_MAX_DAYS)


def purge_expired_sessions() -> int:
    """Delete every expired session; returns how many were removed."""
    ensure_tracking_schema()
    now = datetime.now(timezone.utc)
    con = get_con()
    try:
        rows = con.execute("SELECT token, created_at, last_seen_at FROM tracking_sessions").fetchall()
        expired = [row["token"] for row in rows if _session_expired(row["created_at"], row["last_seen_at"], now)]
        con.executemany("DELETE FROM tracking_sessions WHERE token = ?", [[token] for token in expired])
        con.commit()
        return len(expired)
    finally:
        con.close()


def create_session(user_id: int) -> str:
    """Start a session and return its token (the only time the plain token exists)."""
    purge_expired_sessions()
    token = secrets.token_urlsafe(32)
    now = utc_now_iso()
    con = get_con()
    try:
        con.execute(
            """
            INSERT INTO tracking_sessions (token, user_id, created_at, last_seen_at)
            VALUES (?, ?, ?, ?)
            """,
            [hash_session_token(token), user_id, now, now],
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
    """The signed-in user for a token, or None if it is unknown or expired (expired ones are deleted)."""
    ensure_tracking_schema()
    if not token:
        return None
    stored = hash_session_token(token)
    now = datetime.now(timezone.utc)
    con = get_con()
    try:
        row = con.execute(
            """
            SELECT s.user_id, s.created_at, s.last_seen_at, u.username
            FROM tracking_sessions s
            JOIN tracking_users u
              ON u.id = s.user_id
            WHERE s.token = ?
            """,
            [stored],
        ).fetchone()
        if row is None:
            return None
        if _session_expired(row["created_at"], row["last_seen_at"], now):
            con.execute("DELETE FROM tracking_sessions WHERE token = ?", [stored])
            con.commit()
            return None
        if now - datetime.fromisoformat(row["last_seen_at"]) > timedelta(minutes=SESSION_TOUCH_MINUTES):
            con.execute("UPDATE tracking_sessions SET last_seen_at = ? WHERE token = ?", [utc_now_iso(), stored])
            con.commit()
        return SessionUser(user_id=int(row["user_id"]), username=row["username"])
    finally:
        con.close()


def delete_session(token: str) -> None:
    ensure_tracking_schema()
    con = get_con()
    try:
        con.execute("DELETE FROM tracking_sessions WHERE token = ?", [hash_session_token(token)])
        con.commit()
    finally:
        con.close()


def update_user_pin(user_id: int, new_pin: str) -> None:
    """Replace a user's PIN with a new salted hash (new salt each time)."""
    ensure_tracking_schema()
    salt = secrets.token_hex(16)
    now = utc_now_iso()
    con = get_con()
    try:
        con.execute(
            """
            UPDATE tracking_users
            SET pin_salt = ?, pin_hash = ?, updated_at = ?
            WHERE id = ?
            """,
            [salt, hash_pin(new_pin, salt), now, user_id],
        )
        con.commit()
    finally:
        con.close()


def delete_user(user_id: int) -> None:
    ensure_tracking_schema()
    con = get_con()
    try:
        con.execute("DELETE FROM tracking_saved_views WHERE user_id = ?", [user_id])
        con.execute("DELETE FROM tracking_tags WHERE user_id = ?", [user_id])
        con.execute("DELETE FROM tracking_sessions WHERE user_id = ?", [user_id])
        con.execute("DELETE FROM tracking_users WHERE id = ?", [user_id])
        con.commit()
    finally:
        con.close()


def get_tags_for_user(user_id: int) -> list[dict]:
    """Every tag a user has put on a printing (Watching, Own, ...), as dicts, ordered by market, product and tag."""
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
    """Turn one tag on or off for one printing. On is an upsert (refreshes updated_at); off deletes the row."""
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
    """Add many tags at once, keeping any the user already has. Used when tags saved in the browser
    before sign-in are merged into the account; nothing is removed.
    """
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


def list_saved_views_for_user(user_id: int) -> list[dict]:
    """A user's saved views, most recently changed first. state_json stays a JSON string for the page to parse."""
    ensure_tracking_schema()
    con = get_con()
    try:
        rows = con.execute(
            """
            SELECT id, name, category_id, state_json, ticker_enabled, created_at, updated_at
            FROM tracking_saved_views
            WHERE user_id = ?
            ORDER BY updated_at DESC, id DESC
            """,
            [user_id],
        ).fetchall()
        return [
            {
                "id": int(row["id"]),
                "name": row["name"],
                "category_id": int(row["category_id"]),
                "state_json": row["state_json"],
                "ticker_enabled": bool(row["ticker_enabled"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]
    finally:
        con.close()


def save_saved_view(
    user_id: int,
    *,
    name: str,
    category_id: int,
    state_json: str,
    ticker_enabled: bool = False,
    view_id: int | None = None,
) -> dict:
    """Create a saved view (view_id None) or update the user's own view by id, and return the stored row.

    The update only matches the user's own rows, so another user's id raises KeyError (the
    route answers 404).
    """
    ensure_tracking_schema()
    now = utc_now_iso()
    con = get_con()
    try:
        if view_id is None:
            cur = con.execute(
                """
                INSERT INTO tracking_saved_views (
                    user_id, name, category_id, state_json, ticker_enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [user_id, name, category_id, state_json, 1 if ticker_enabled else 0, now, now],
            )
            saved_id = int(cur.lastrowid)
        else:
            cur = con.execute(
                """
                UPDATE tracking_saved_views
                SET name = ?, category_id = ?, state_json = ?, ticker_enabled = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                [name, category_id, state_json, 1 if ticker_enabled else 0, now, int(view_id), user_id],
            )
            if cur.rowcount <= 0:
                raise KeyError(f"Saved view {view_id} was not found")
            saved_id = int(view_id)
        con.commit()
        row = con.execute(
            """
            SELECT id, name, category_id, state_json, ticker_enabled, created_at, updated_at
            FROM tracking_saved_views
            WHERE id = ? AND user_id = ?
            """,
            [saved_id, user_id],
        ).fetchone()
        return {
            "id": int(row["id"]),
            "name": row["name"],
            "category_id": int(row["category_id"]),
            "state_json": row["state_json"],
            "ticker_enabled": bool(row["ticker_enabled"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
    finally:
        con.close()


def delete_saved_view(user_id: int, view_id: int) -> None:
    ensure_tracking_schema()
    con = get_con()
    try:
        con.execute(
            "DELETE FROM tracking_saved_views WHERE user_id = ? AND id = ?",
            [user_id, int(view_id)],
        )
        con.commit()
    finally:
        con.close()


def create_bug_report(payload: dict) -> int:
    """Store one bug report and return its id. Since bug reports moved to Discord, rows come from
    scripts/utilities/import_discord_bug_reports.py, not from an on-site form.
    """
    ensure_tracking_schema()
    now = utc_now_iso()
    con = get_con()
    try:
        cur = con.execute(
            """
            INSERT INTO bug_reports (
                created_at,
                reporter_username,
                page_path,
                page_url,
                category_id,
                tab,
                segment,
                chart_mode,
                product_key,
                group_id,
                search_query,
                title,
                expected,
                details,
                status,
                context_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                now,
                (payload.get("reporter_username") or "").strip() or None,
                (payload.get("page_path") or "").strip() or "/dashboard",
                (payload.get("page_url") or "").strip() or "/dashboard",
                payload.get("category_id"),
                (payload.get("tab") or "").strip() or None,
                (payload.get("segment") or "").strip() or None,
                (payload.get("chart_mode") or "").strip() or None,
                (payload.get("product_key") or "").strip() or None,
                payload.get("group_id"),
                (payload.get("search_query") or "").strip() or None,
                (payload.get("title") or "").strip() or "Bug report",
                (payload.get("expected") or "").strip() or None,
                (payload.get("details") or "").strip() or "",
                (payload.get("status") or "").strip() or "new",
                payload.get("context_json") or "{}",
            ],
        )
        con.commit()
        return int(cur.lastrowid)
    finally:
        con.close()


def list_bug_reports(limit: int = 200) -> list[dict]:
    """The newest bug reports (up to `limit`) for the admin Bug Reports page."""
    ensure_tracking_schema()
    con = get_con()
    try:
        rows = con.execute(
            """
            SELECT
                id,
                created_at,
                reporter_username,
                page_path,
                page_url,
                category_id,
                tab,
                segment,
                chart_mode,
                product_key,
                group_id,
                search_query,
                title,
                expected,
                details,
                status,
                context_json
            FROM bug_reports
            ORDER BY id DESC
            LIMIT ?
            """,
            [max(1, min(int(limit or 200), 1000))],
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def get_psa_certification(cert_number: str) -> dict | None:
    """The cached PSA certificate lookup for a cert number, or None if we never fetched it.

    raw_response_json comes back parsed ({} if the stored text isn't valid JSON). /api/psa/cert
    answers from this cache while it's fresh (psa_cache_is_fresh in api.py); a new or stale
    cert, or refresh=true, calls PSA and is admin-only, to protect our PSA quota.
    """
    ensure_tracking_schema()
    con = get_con()
    try:
        row = con.execute(
            """
            SELECT
                id,
                cert_number,
                is_valid_request,
                server_message,
                raw_response_json,
                normalized_card_name,
                normalized_set_name,
                normalized_grade,
                lookup_status,
                last_checked_at,
                created_at,
                updated_at
            FROM psa_certifications
            WHERE cert_number = ?
            """,
            [str(cert_number).strip()],
        ).fetchone()
        if row is None:
            return None
        raw_payload = row["raw_response_json"] or "{}"
        try:
            parsed_payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            parsed_payload = {}
        return {
            "id": int(row["id"]),
            "cert_number": row["cert_number"],
            "is_valid_request": bool(row["is_valid_request"]),
            "server_message": row["server_message"],
            "raw_response_json": parsed_payload,
            "normalized_card_name": row["normalized_card_name"],
            "normalized_set_name": row["normalized_set_name"],
            "normalized_grade": row["normalized_grade"],
            "lookup_status": row["lookup_status"],
            "last_checked_at": row["last_checked_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
    finally:
        con.close()


def upsert_psa_certification(
    cert_number: str,
    *,
    is_valid_request: bool,
    server_message: str | None,
    raw_response_json: dict,
    normalized_card_name: str | None,
    normalized_set_name: str | None,
    normalized_grade: str | None,
    lookup_status: str,
    last_checked_at: str | None = None,
) -> dict:
    """Store (or refresh) PSA's answer for one cert number and return the cached row as get_psa_certification gives it."""
    ensure_tracking_schema()
    now = utc_now_iso()
    checked_at = last_checked_at or now
    con = get_con()
    try:
        con.execute(
            """
            INSERT INTO psa_certifications (
                cert_number,
                is_valid_request,
                server_message,
                raw_response_json,
                normalized_card_name,
                normalized_set_name,
                normalized_grade,
                lookup_status,
                last_checked_at,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cert_number)
            DO UPDATE SET
                is_valid_request = excluded.is_valid_request,
                server_message = excluded.server_message,
                raw_response_json = excluded.raw_response_json,
                normalized_card_name = excluded.normalized_card_name,
                normalized_set_name = excluded.normalized_set_name,
                normalized_grade = excluded.normalized_grade,
                lookup_status = excluded.lookup_status,
                last_checked_at = excluded.last_checked_at,
                updated_at = excluded.updated_at
            """,
            [
                str(cert_number).strip(),
                1 if is_valid_request else 0,
                server_message,
                json.dumps(raw_response_json or {}, sort_keys=True),
                normalized_card_name,
                normalized_set_name,
                normalized_grade,
                lookup_status,
                checked_at,
                now,
                now,
            ],
        )
        con.commit()
    finally:
        con.close()
    return get_psa_certification(cert_number) or {}


# How long Budget Builder pick history is kept. Rotation only looks back a few days.
# 400 days, not just the 3 the rotation needs: the Track record (pick_tracker.py) scores
# these picks too, and a 30-day result needs at least 30 days of history.
BUDGET_PICK_HISTORY_DAYS = 400


def record_budget_picks(builder: str, category_id: int, price_date: str, card_keys: list[str]) -> None:
    """Remember the cards a Budget Builder showed by default for one price date.

    builder is "raw" or "graded"; card_key is "productId||subTypeName". Recording the same
    card twice for a date does nothing, so every page view can call this. Rows older than
    BUDGET_PICK_HISTORY_DAYS are dropped on the way.
    """
    if not card_keys or not price_date:
        return
    ensure_tracking_schema()
    con = get_con()
    try:
        con.executemany(
            "INSERT OR IGNORE INTO budget_pick_history (builder, category_id, price_date, card_key) VALUES (?, ?, ?, ?)",
            [(builder, int(category_id), str(price_date), key) for key in card_keys],
        )
        con.execute(
            "DELETE FROM budget_pick_history WHERE price_date < date(?, ?)",
            [str(price_date), f"-{BUDGET_PICK_HISTORY_DAYS} day"],
        )
        con.commit()
    finally:
        con.close()


def recent_budget_picks(builder: str, category_id: int, before_date: str, days: int) -> dict[str, str]:
    """Cards a builder showed in the `days` days before `before_date` (not on it): {card_key: last date}.

    Only earlier dates count, so a day's picks don't change as the day's own views are recorded.
    """
    if not before_date:
        return {}
    ensure_tracking_schema()
    con = get_con()
    try:
        rows = con.execute(
            """
            SELECT card_key, MAX(price_date) AS last_date
            FROM budget_pick_history
            WHERE builder = ? AND category_id = ? AND price_date < ? AND price_date >= date(?, ?)
            GROUP BY card_key
            """,
            [builder, int(category_id), str(before_date), str(before_date), f"-{int(days)} day"],
        ).fetchall()
        return {row["card_key"]: row["last_date"] for row in rows}
    finally:
        con.close()


def recorded_pick_dates(category_id: int) -> set[str]:
    """Price dates that already have Track record picks for a market."""
    ensure_tracking_schema()
    con = get_con()
    try:
        rows = con.execute(
            "SELECT DISTINCT price_date FROM list_pick_history WHERE category_id = ?", [int(category_id)]
        ).fetchall()
        return {row["price_date"] for row in rows}
    finally:
        con.close()


def record_list_picks(category_id: int, price_date: str, picks: list[dict]) -> int:
    """Store one day's list picks: dicts with list_key, rank, product_id, sub_type_name, entry_price.

    A card already stored for that list and date is left as it was (first record wins), so
    recording twice in a day changes nothing. Returns the number of rows added.
    """
    if not picks or not price_date:
        return 0
    ensure_tracking_schema()
    con = get_con()
    try:
        before = con.total_changes
        con.executemany(
            """
            INSERT OR IGNORE INTO list_pick_history
                (list_key, category_id, price_date, rank, product_id, sub_type_name, entry_price, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (p["list_key"], int(category_id), str(price_date), int(p["rank"]), int(p["product_id"]),
                 str(p["sub_type_name"]), float(p["entry_price"]), utc_now_iso())
                for p in picks
            ],
        )
        con.commit()
        return con.total_changes - before
    finally:
        con.close()


def load_list_picks(category_id: int, since_date: str | None = None) -> list[dict]:
    """Every stored list pick for a market (optionally from `since_date` on), oldest first."""
    ensure_tracking_schema()
    con = get_con()
    try:
        rows = con.execute(
            """
            SELECT list_key, price_date, rank, product_id, sub_type_name, entry_price
            FROM list_pick_history
            WHERE category_id = ? AND (? IS NULL OR price_date >= ?)
            ORDER BY price_date, list_key, rank
            """,
            [int(category_id), since_date, since_date],
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        con.close()


def load_budget_picks(builder: str, category_id: int) -> list[dict]:
    """Every remembered Budget Builder default pick, as Track record picks (no entry price:
    the builder didn't store one, so the Track record reads it from the price history)."""
    ensure_tracking_schema()
    con = get_con()
    try:
        rows = con.execute(
            "SELECT price_date, card_key FROM budget_pick_history WHERE builder = ? AND category_id = ? ORDER BY price_date",
            [builder, int(category_id)],
        ).fetchall()
    finally:
        con.close()
    picks = []
    for rank, row in enumerate(rows, 1):
        product_id, _, sub_type = row["card_key"].partition("||")
        if product_id.isdigit() and sub_type:
            picks.append({"list_key": "budget", "price_date": row["price_date"], "rank": rank,
                          "product_id": int(product_id), "sub_type_name": sub_type, "entry_price": None})
    return picks
