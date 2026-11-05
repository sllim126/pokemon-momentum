"""The storefront's private visitor counting (storefront/app/visitors.py)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from storefront.app.main import create_app

BROWSER = "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 Safari/605.1.15"


def test_page_views_are_counted_and_published(imported):
    with TestClient(create_app(imported), follow_redirects=False, client=("172.18.0.1", 50000)) as client:
        client.get("/shop", headers={"User-Agent": BROWSER, "X-Forwarded-For": "203.0.113.5"})
        client.get("/healthz", headers={"User-Agent": BROWSER})  # JSON, not a page view
        text = client.get("/metrics").text
    assert 'site_today_total{app="storefront",kind="pageviews"} 1.0' in text
    assert 'site_today_total{app="storefront",kind="visitors"} 1.0' in text
    assert (imported.data_dir / "visitors.sqlite3").exists()
