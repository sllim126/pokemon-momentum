"""HEAD requests get GET's status and headers with no body (storefront/app/head.py)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from storefront.app.main import create_app

BROWSER = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 Safari/605.1.15"


def test_head_matches_get_without_a_body(client):
    for path in ("/", "/shop", "/healthz", "/robots.txt"):
        get, head = client.get(path), client.head(path)
        assert head.status_code == get.status_code == 200, path
        assert head.content == b"", path
        assert head.headers["content-type"] == get.headers["content-type"], path
        assert head.headers["content-length"] == get.headers["content-length"], path
        assert head.headers.get("x-content-type-options") == get.headers.get("x-content-type-options"), path


def test_head_keeps_redirects_and_404s(client):
    assert client.head("/shop/").status_code == 301
    assert client.head("/shop/").headers["location"] == "/shop"
    missing = client.head("/no-such-page")
    assert missing.status_code == 404 and missing.content == b""


def test_head_still_refuses_post_only_routes(client):
    # Only GET routes gain HEAD; a POST-only address still says 405.
    assert client.head("/cart/quote").status_code == client.get("/cart/quote").status_code == 405


def test_head_is_not_a_page_view_but_is_in_the_metrics(imported):
    with TestClient(create_app(imported), follow_redirects=False, client=("172.18.0.1", 50000)) as client:
        client.head("/shop", headers={"User-Agent": BROWSER, "X-Forwarded-For": "203.0.113.5"})
        text = client.get("/metrics").text
    assert 'site_today_total{app="storefront",kind="pageviews"} 0.0' in text
    assert 'http_requests_total{app="storefront",method="HEAD",route="/shop",status="200"}' in text
