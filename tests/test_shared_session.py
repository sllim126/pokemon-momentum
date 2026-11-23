"""One Poke6s account (storefront plan step 9b): the market sets the shared poke6s_session cookie
for .poke6s.com when someone signs in, hands a signed-in visitor to the store, and clears it on
sign-out. Uses a throwaway tracking database."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from fastapi.testclient import TestClient

from scripts.dashboards import api, tracking_store

MARKET = "https://market.poke6s.com"


class SharedSessionTests(unittest.TestCase):
    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db = patch.object(tracking_store, "TRACKING_DB_PATH", Path(tmp.name) / "t.sqlite3")
        db.start()
        self.addCleanup(db.stop)
        self.user_id = tracking_store.create_google_user("collector@example.com")
        self.client = TestClient(api.app, base_url=MARKET, follow_redirects=False)

    def test_google_sign_in_sets_the_shared_cookie(self):
        with patch.object(api, "verify_google_identity_token",
                          return_value={"email": "collector@example.com", "sub": "abc"}):
            response = self.client.post("/tracking/google_session", json={"credential": "g"})
        self.assertEqual(response.status_code, 200)
        cookie = response.headers["set-cookie"]
        self.assertIn(f"poke6s_session={response.json()['token']}", cookie)
        for part in ("Domain=poke6s.com", "HttpOnly", "Secure", "SameSite=lax", "Path=/"):
            self.assertIn(part, cookie)

    def test_no_shared_cookie_off_the_domain(self):
        lan = TestClient(api.app, base_url="http://192.168.1.240:8001")
        with patch.object(api, "verify_google_identity_token",
                          return_value={"email": "collector@example.com", "sub": "abc"}):
            response = lan.post("/tracking/google_session", json={"credential": "g"})
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("poke6s_session", response.headers.get("set-cookie", ""))

    def test_hand_off_to_the_store(self):
        # Not signed in: to the Account page, which comes back after Google sign-in.
        response = self.client.get("/account/store")
        self.assertEqual(response.headers["location"], "/account-settings?return=/account/store")
        # Signed in on the market (the page-script cookie): shared cookie set, off to the store.
        token = tracking_store.create_session(self.user_id)
        self.client.cookies.set("pm_tracking_token", token)
        response = self.client.get("/account/store")
        self.assertEqual(response.status_code, 303)
        self.assertEqual(response.headers["location"], api.STORE_ACCOUNT_URL)
        self.assertIn(f"poke6s_session={token}", response.headers["set-cookie"])
        # A stale token is treated as signed out.
        self.client.cookies.set("pm_tracking_token", "not-a-session")
        self.assertEqual(self.client.get("/account/store").headers["location"],
                         "/account-settings?return=/account/store")

    def test_sign_out_ends_the_session_and_clears_the_cookie(self):
        token = tracking_store.create_session(self.user_id)
        self.client.cookies.set("poke6s_session", token)
        response = self.client.get("/account/sign-out")
        self.assertEqual(response.headers["location"], api.STORE_HOME_URL)
        self.assertIn('poke6s_session=""', response.headers["set-cookie"])
        self.assertIsNone(tracking_store.get_session_user(token))
        # The market's own sign-out clears it too.
        token = tracking_store.create_session(self.user_id)
        response = self.client.request("DELETE", "/tracking/session", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(response.json(), {"ok": True})
        self.assertIn("poke6s_session", response.headers["set-cookie"])
        self.assertIsNone(tracking_store.get_session_user(token))


if __name__ == "__main__":
    unittest.main()
