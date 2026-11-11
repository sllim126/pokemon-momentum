"""Odd addresses and headers get a normal answer, not a 500 (store review 2026-11-11, item 6)."""

from __future__ import annotations

import pytest

from storefront.app import stripe_api
from storefront.app.main import _token_matches


@pytest.mark.parametrize("page", ["²", "1" * 5000, "-1", "abc", "١"])
def test_odd_page_numbers_show_page_one(client, page):
    assert client.get("/shop", params={"page": page}).status_code == 200


@pytest.mark.parametrize("width", ["abc", "-1", "²", "999"])
def test_odd_media_widths_are_404_pages(client, width):
    response = client.get(f"/media/{width}/x.webp")
    assert response.status_code == 404 and "text/html" in response.headers["content-type"]


def test_order_token_check_takes_any_text():
    assert _token_matches("abc123", "abc123")
    assert not _token_matches("abc123", "é")
    assert not _token_matches("abc123", "")


def test_order_page_with_non_ascii_token_is_404(client):
    assert client.get("/order/P6-2026-0001", params={"k": "é"}).status_code == 404
    assert client.get("/cart", params={"cancelled": "P6-2026-0001", "k": "é"}).status_code == 200


def test_webhook_with_non_ascii_signature_is_refused_not_crashed():
    with pytest.raises(ValueError):
        stripe_api.verify_webhook(b"{}", "t=1,v1=é", "whsec_x", now=1)
