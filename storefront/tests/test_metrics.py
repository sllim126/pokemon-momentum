"""The storefront's Prometheus metrics (storefront/app/metrics.py)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from storefront.app.main import create_app


def _client(settings, host: str) -> TestClient:
    return TestClient(create_app(settings), follow_redirects=False, client=(host, 50000))


def test_scrape_from_the_host_counts_requests_by_route(imported):
    # Prometheus' scrapes arrive from the host through the Docker bridge.
    with _client(imported, "172.18.0.1") as client:
        client.get("/shop")
        response = client.get("/metrics")
    assert response.status_code == 200
    assert 'http_requests_total{app="storefront",method="GET",route="/shop",status="200"}' in response.text


def test_metrics_route_is_not_shadowed_or_public(imported):
    with _client(imported, "172.18.0.1") as client:
        assert client.get("/metrics", headers={"X-Forwarded-For": "203.0.113.9"}).status_code == 404
    with _client(imported, "192.168.1.30") as client:
        assert client.get("/metrics").status_code == 404
