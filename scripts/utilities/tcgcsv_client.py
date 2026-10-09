"""Shared TCGCSV HTTP client that follows https://tcgcsv.com/docs#usage-guidelines.

- identify with a `Name/X.Y.Z` User-Agent
- make requests one at a time with at least 100 ms between them
- never retry a 403/429: those mean we've been blocked or throttled, so the whole
  run stops and the operator investigates instead of hammering the service
"""

import os
import time

import requests


BASE_URL = "https://tcgcsv.com"
DEFAULT_USER_AGENT = "Poke6sMarket/1.0.0"
MIN_REQUEST_INTERVAL_SECONDS = 0.15
MAX_ATTEMPTS = 4
REQUEST_TIMEOUT = (10, 60)

_last_request_at = 0.0


class TcgcsvBlockedError(RuntimeError):
    """TCGCSV answered 403/429; stop all requests for this run."""


def build_tcgcsv_session() -> requests.Session:
    """Build a tcgcsv session with an explicit application identity.

    requests sends `Accept-Encoding: gzip, deflate` by default, so responses arrive
    compressed (TCGCSV enabled compression to cut its bandwidth bill).
    """
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": os.getenv("POKE6S_TCGCSV_USER_AGENT", DEFAULT_USER_AGENT).strip() or DEFAULT_USER_AGENT,
            "Accept": "application/json",
        }
    )
    return session


def _wait_for_turn() -> None:
    global _last_request_at
    remaining = MIN_REQUEST_INTERVAL_SECONDS - (time.monotonic() - _last_request_at)
    if remaining > 0:
        time.sleep(remaining)
    _last_request_at = time.monotonic()


def tcgcsv_get(session: requests.Session, path: str) -> requests.Response | None:
    """GET a TCGCSV path politely; returns None for 404 and raises TcgcsvBlockedError on 403/429.

    Timeouts, connection errors, and 5xx responses are retried with backoff.
    """
    url = f"{BASE_URL}{path}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        _wait_for_turn()
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            error = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code in (403, 429):
                raise TcgcsvBlockedError(
                    f"TCGCSV returned {resp.status_code} for {path}; stopping. "
                    f"Response: {resp.text[:300]!r}"
                )
            if resp.status_code == 404:
                return None
            if resp.status_code < 500:
                resp.raise_for_status()
                return resp
            error = f"HTTP {resp.status_code}"
        if attempt == MAX_ATTEMPTS:
            raise RuntimeError(f"{url} failed after {MAX_ATTEMPTS} attempts: {error}")
        print(f"  retry {attempt}/{MAX_ATTEMPTS - 1} for {path} after {error}")
        time.sleep(5 * attempt)
    return None


def tcgcsv_get_json(session: requests.Session, path: str) -> dict | None:
    resp = tcgcsv_get(session, path)
    return None if resp is None else resp.json()
