import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.dashboards import tracking_store


class SessionTests(unittest.TestCase):
    """Sign-in sessions: hashed at rest, idle and absolute expiry, old tokens upgraded."""

    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_patch = patch.object(tracking_store, "TRACKING_DB_PATH", Path(tmp.name) / "tracking.sqlite3")
        db_patch.start()
        self.addCleanup(db_patch.stop)
        tracking_store.ensure_tracking_schema()
        con = tracking_store.get_con()
        con.execute("INSERT INTO tracking_users (username, pin_salt, pin_hash, created_at, updated_at) "
                    "VALUES ('a@example.com', '', '', '2026-01-01', '2026-01-01')")
        con.commit()
        self.user_id = con.execute("SELECT id FROM tracking_users").fetchone()[0]
        con.close()

    def _set_times(self, created_days_ago, seen_days_ago):
        now = datetime.now(timezone.utc)
        con = tracking_store.get_con()
        con.execute("UPDATE tracking_sessions SET created_at = ?, last_seen_at = ?",
                    [(now - timedelta(days=created_days_ago)).isoformat(), (now - timedelta(days=seen_days_ago)).isoformat()])
        con.commit()
        con.close()

    def test_token_is_stored_hashed_and_still_signs_in(self):
        token = tracking_store.create_session(self.user_id)
        con = tracking_store.get_con()
        stored = con.execute("SELECT token FROM tracking_sessions").fetchone()[0]
        con.close()
        self.assertNotEqual(stored, token)
        self.assertTrue(stored.startswith("sha256:"))
        self.assertEqual(tracking_store.get_session_user(token).username, "a@example.com")

    def test_idle_session_expires(self):
        token = tracking_store.create_session(self.user_id)
        self._set_times(created_days_ago=40, seen_days_ago=tracking_store.SESSION_IDLE_DAYS + 1)
        self.assertIsNone(tracking_store.get_session_user(token))

    def test_session_expires_after_max_age_even_if_used(self):
        token = tracking_store.create_session(self.user_id)
        self._set_times(created_days_ago=tracking_store.SESSION_MAX_DAYS + 1, seen_days_ago=0)
        self.assertIsNone(tracking_store.get_session_user(token))

    def test_old_plain_tokens_are_upgraded_and_keep_working(self):
        now = datetime.now(timezone.utc).isoformat()
        con = tracking_store.get_con()
        con.execute("INSERT INTO tracking_sessions (token, user_id, created_at, last_seen_at) VALUES (?, ?, ?, ?)",
                    ["old-plain-token", self.user_id, now, now])
        con.commit()
        con.close()
        tracking_store.ensure_tracking_schema()
        self.assertEqual(tracking_store.get_session_user("old-plain-token").username, "a@example.com")

    def test_sign_out_deletes_session(self):
        token = tracking_store.create_session(self.user_id)
        tracking_store.delete_session(token)
        self.assertIsNone(tracking_store.get_session_user(token))
