"""Sign-in for the store admin: one owner password, signed session cookies, form tokens.

Standard library only, so the password tool (passwd.py) runs on the host without the app's
packages.

- **Password.** Only a PBKDF2-SHA256 hash is kept, in .env as STOREFRONT_ADMIN_PASSWORD_HASH
  (`pbkdf2_sha256:<iterations>:<salt hex>:<hash hex>`; colons, because docker-compose
  would read a `$` in .env as the start of a variable). Google sign-in, which the market site
  uses, isn't possible here: Google won't send a sign-in back to a home-network address.
- **Sessions.** The cookie is `<expiry>.<nonce>.<signature>`, signed with HMAC-SHA256. The
  signing key mixes STOREFRONT_ADMIN_SESSION_SECRET with the password hash, so changing the
  password signs every device out. Nothing is stored on the server.
- **Form tokens (CSRF).** Each session has its own token, derived from its nonce; every form
  posts it back. The cookie is also SameSite=Strict, so another site can't post with it.
- **Lockout.** Five wrong passwords from one address within 15 minutes lock that address out
  for 15 minutes (kept in memory, so a restart clears it).
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from collections import defaultdict, deque

ALGORITHM = "pbkdf2_sha256"
ITERATIONS = 600_000          # OWASP's 2023 figure for PBKDF2-SHA256
SESSION_DAYS = 30             # each device stays signed in this long (owner's devices, LAN only)
COOKIE_NAME = "p6_admin"
MAX_FAILURES = 5
FAILURE_WINDOW_SECONDS = 15 * 60
LOCKOUT_SECONDS = 15 * 60
MIN_PASSWORD_LENGTH = 12


def hash_password(password: str, *, iterations: int = ITERATIONS, salt: bytes | None = None) -> str:
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"{ALGORITHM}:{iterations}:{salt.hex()}:{digest.hex()}"


def check_password(password: str, stored: str) -> bool:
    """True when `password` matches the stored hash. A malformed hash never matches."""
    try:
        algorithm, iterations, salt_hex, digest_hex = stored.split(":")
        if algorithm != ALGORITHM:
            return False
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt_hex),
                                     int(iterations))
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(digest.hex(), digest_hex)


class Sessions:
    """Makes and checks session cookies and their form tokens."""

    def __init__(self, secret: str, password_hash: str):
        # Changing either the secret or the password invalidates every existing cookie.
        self._key = hmac.new(secret.encode(), password_hash.encode(), hashlib.sha256).digest()

    def _sign(self, text: str) -> str:
        return hmac.new(self._key, text.encode(), hashlib.sha256).hexdigest()

    def new(self, now: float | None = None) -> str:
        expires = int((now or time.time()) + SESSION_DAYS * 86400)
        body = f"{expires}.{secrets.token_hex(16)}"
        return f"{body}.{self._sign(body)}"

    def valid(self, cookie: str | None, now: float | None = None) -> bool:
        try:
            expires, nonce, signature = (cookie or "").split(".")
            fresh = int(expires) > (now or time.time())
        except ValueError:
            return False
        # Compared as bytes: compare_digest refuses non-ASCII text, and the cookie is user input.
        good = hmac.compare_digest(self._sign(f"{expires}.{nonce}").encode(),
                                   signature.encode("utf-8", "replace"))
        return good and fresh

    def form_token(self, cookie: str) -> str:
        nonce = cookie.split(".")[1]
        return self._sign(f"form:{nonce}")

    def form_token_ok(self, cookie: str | None, token: str | None) -> bool:
        if not self.valid(cookie) or not token:
            return False
        return hmac.compare_digest(self.form_token(cookie).encode(), token.encode("utf-8", "replace"))


class Lockout:
    """Counts wrong passwords per address and locks an address out for a while."""

    def __init__(self):
        self._failures: dict[str, deque] = defaultdict(deque)
        self._locked_until: dict[str, float] = {}

    def locked(self, address: str, now: float | None = None) -> bool:
        return self._locked_until.get(address, 0) > (now or time.time())

    def failed(self, address: str, now: float | None = None) -> bool:
        """Record a wrong password. True when this one locked the address out."""
        now = now or time.time()
        attempts = self._failures[address]
        attempts.append(now)
        while attempts and attempts[0] < now - FAILURE_WINDOW_SECONDS:
            attempts.popleft()
        if len(attempts) >= MAX_FAILURES:
            self._locked_until[address] = now + LOCKOUT_SECONDS
            attempts.clear()
            return True
        return False

    def succeeded(self, address: str) -> None:
        self._failures.pop(address, None)
        self._locked_until.pop(address, None)
