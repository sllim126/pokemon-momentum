"""The market app's Prometheus metrics (scripts/dashboards/metrics.py)."""

import unittest

from fastapi.testclient import TestClient

from scripts.dashboards import api

# The address Prometheus' scrapes arrive from: the host, through the Docker bridge.
DOCKER_GATEWAY = ("172.18.0.1", 50000)


class MetricsTests(unittest.TestCase):
    def test_scrape_from_the_host_returns_prometheus_text(self):
        client = TestClient(api.app, client=DOCKER_GATEWAY)
        client.get("/categories")
        response = client.get("/metrics")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/plain", response.headers["content-type"])
        self.assertIn('http_requests_total{app="market",method="GET",route="/categories",status="200"}', response.text)
        self.assertIn("market_latest_data_timestamp_seconds", response.text)

    def test_requests_are_labelled_by_route_template_not_address(self):
        client = TestClient(api.app, client=DOCKER_GATEWAY)
        client.get("/no-such-page-12345")
        text = client.get("/metrics").text
        self.assertNotIn("no-such-page-12345", text)
        self.assertIn('route="<unmatched>",status="404"', text)

    def test_metrics_are_hidden_from_proxied_requests(self):
        # Public visitors come through Caddy, which adds X-Forwarded-For.
        client = TestClient(api.app, client=DOCKER_GATEWAY)
        response = client.get("/metrics", headers={"X-Forwarded-For": "203.0.113.9"})
        self.assertEqual(response.status_code, 404)

    def test_metrics_are_hidden_from_other_addresses(self):
        client = TestClient(api.app, client=("192.168.1.30", 50000))
        self.assertEqual(client.get("/metrics").status_code, 404)


if __name__ == "__main__":
    unittest.main()
