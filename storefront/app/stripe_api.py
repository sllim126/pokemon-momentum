"""The few Stripe calls the shop makes, over plain HTTPS (no Stripe library needed).

- create_session / retrieve_session / expire_session: Checkout Sessions.
- verify_webhook: checks the Stripe-Signature header on webhook deliveries.

Tests swap StripeClient for a fake with the same methods (tests/test_checkout.py). The secret
key is sent only in the Authorization header and never logged; error messages carry Stripe's
error text, not the request.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any

import requests

API = "https://api.stripe.com/v1"
TIMEOUT = 20


class StripeError(RuntimeError):
    pass


def form_encode(params: dict, prefix: str = "") -> list[tuple[str, str]]:
    """Stripe's form encoding: {"a": {"b": [1]}} -> [("a[b][0]", "1")]."""
    pairs: list[tuple[str, str]] = []
    for key, value in params.items():
        name = f"{prefix}[{key}]" if prefix else str(key)
        if isinstance(value, dict):
            pairs += form_encode(value, name)
        elif isinstance(value, (list, tuple)):
            pairs += form_encode({str(i): v for i, v in enumerate(value)}, name)
        elif isinstance(value, bool):
            pairs.append((name, "true" if value else "false"))
        elif value is not None:
            pairs.append((name, str(value)))
    return pairs


class StripeClient:
    def __init__(self, secret_key: str) -> None:
        self._key = secret_key

    def _call(self, method: str, path: str, params: dict | None = None,
              idempotency_key: str | None = None) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self._key}"}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        data = form_encode(params or {})
        response = requests.request(method, f"{API}{path}", headers=headers, timeout=TIMEOUT,
                                    data=data if method == "POST" else None,
                                    params=data if method == "GET" else None)
        body = response.json() if response.content else {}
        if response.status_code >= 400:
            message = (body.get("error") or {}).get("message") or f"HTTP {response.status_code}"
            raise StripeError(message)
        return body

    def create_session(self, params: dict, idempotency_key: str) -> dict:
        return self._call("POST", "/checkout/sessions", params, idempotency_key)

    def retrieve_session(self, session_id: str) -> dict:
        return self._call("GET", f"/checkout/sessions/{session_id}",
                          {"expand": ["payment_intent.latest_charge"]})

    def expire_session(self, session_id: str) -> dict:
        return self._call("POST", f"/checkout/sessions/{session_id}/expire")


def verify_webhook(payload: bytes, signature_header: str, secret: str,
                   tolerance: int = 300, now: float | None = None) -> dict:
    """Return the event if the Stripe-Signature header is valid, else raise ValueError.

    Header format: "t=<unix time>,v1=<hex HMAC-SHA256 of '<t>.<payload>'>[,v1=...]".
    Old timestamps are refused so a captured delivery can't be replayed later.
    """
    if not secret:
        raise ValueError("no webhook secret configured")
    parts: dict[str, list[str]] = {}
    for item in (signature_header or "").split(","):
        key, _, value = item.strip().partition("=")
        parts.setdefault(key, []).append(value)
    try:
        timestamp = int(parts["t"][0])
    except (KeyError, ValueError):
        raise ValueError("bad signature header") from None
    expected = hmac.new(secret.encode(), f"{timestamp}.".encode() + payload, hashlib.sha256).hexdigest()
    # Compared as bytes: compare_digest raises TypeError on non-ASCII text, which would be a 500
    # instead of the ValueError the webhook handler answers with 400.
    if not any(hmac.compare_digest(expected.encode(), sig.encode("utf-8", "replace"))
               for sig in parts.get("v1", [])):
        raise ValueError("signature mismatch")
    if abs((now or time.time()) - timestamp) > tolerance:
        raise ValueError("timestamp outside tolerance")
    return json.loads(payload)


def sign_payload(payload: bytes, secret: str, timestamp: int | None = None) -> str:
    """A valid Stripe-Signature header for payload (used by tests)."""
    timestamp = timestamp or int(time.time())
    sig = hmac.new(secret.encode(), f"{timestamp}.".encode() + payload, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={sig}"
