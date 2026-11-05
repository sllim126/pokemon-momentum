"""Prometheus metrics for the market app: request counts, errors and response times.

Prometheus on the host scrapes ``/metrics`` every 15 s and Grafana draws the charts
(see "Monitoring" in docs/operator_runbook.txt). The storefront has its own copy of this
file (storefront/app/metrics.py), because its container can't see this folder; keep the
two in step.

Two things here aren't obvious:

* Requests are labelled with the route *template* (``/api/card/{product_id}``), never the
  raw address. Raw addresses would create a new time series per card and swamp Prometheus.
  Requests that match no route are counted under ``<unmatched>``.
* ``/metrics`` is private. Public visitors arrive through the Caddy proxy on another LAN
  machine, which adds ``X-Forwarded-For``; Prometheus arrives from the host through the
  Docker bridge (172.16.0.0/12) or loopback, with no forwarding header. Anything else
  gets a 404, so the page doesn't exist as far as the internet is concerned.
"""

from __future__ import annotations

import ipaddress
import time
from collections.abc import Callable

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# Loopback and the Docker bridge networks. Prometheus runs on the host, so its scrapes
# reach the container from the bridge gateway (for example 172.18.0.1).
_PRIVATE_SCRAPERS = (ipaddress.ip_network("127.0.0.0/8"), ipaddress.ip_network("172.16.0.0/12"))
# Headers a proxy adds. Their presence means the request came from outside.
_PROXY_HEADERS = (b"x-forwarded-for", b"x-real-ip", b"forwarded", b"cf-connecting-ip")

REQUESTS = Counter(
    "http_requests_total", "HTTP requests handled", ["app", "method", "route", "status"]
)
LATENCY = Histogram(
    "http_request_duration_seconds",
    "Time to produce the full response",
    ["app", "method", "route"],
    # Wider than the default buckets at the top end: some market queries take several seconds.
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
IN_PROGRESS = Gauge("http_requests_in_progress", "Requests being handled right now", ["app"])


def _route_template(scope) -> str:
    route = scope.get("route")
    return getattr(route, "path", None) or "<unmatched>"


def is_private_scrape(request: Request) -> bool:
    """True when the request comes straight from the host or Docker, not through the proxy."""
    if any(name in dict(request.scope.get("headers") or []) for name in _PROXY_HEADERS):
        return False
    try:
        client = ipaddress.ip_address(request.client.host if request.client else "")
    except ValueError:
        return False
    return any(client in net for net in _PRIVATE_SCRAPERS)


class _MetricsMiddleware:
    """Pure ASGI middleware (not @app.middleware) so streamed responses are timed to the
    last byte and the route that matched is visible in the scope afterwards."""

    def __init__(self, app, app_name: str):
        self.app = app
        self.app_name = app_name

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path") == "/metrics":
            await self.app(scope, receive, send)
            return
        status = {"code": 500}  # stays 500 if the app raises before sending a response

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
            await send(message)

        start = time.perf_counter()
        IN_PROGRESS.labels(self.app_name).inc()
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            IN_PROGRESS.labels(self.app_name).dec()
            route = _route_template(scope)
            method = scope.get("method", "")
            REQUESTS.labels(self.app_name, method, route, str(status["code"])).inc()
            LATENCY.labels(self.app_name, method, route).observe(time.perf_counter() - start)


def install_metrics(app: FastAPI, app_name: str, refresh: Callable[[], None] | None = None) -> None:
    """Count every request and add the private ``/metrics`` route.

    ``refresh`` runs before each scrape so the app can update gauges that are read from
    data (such as how old the latest prices are). It must be cheap and must not raise.
    """
    app.add_middleware(_MetricsMiddleware, app_name=app_name)

    @app.get("/metrics", include_in_schema=False)
    def metrics(request: Request):
        if not is_private_scrape(request):
            return PlainTextResponse("Not Found", status_code=404)
        if refresh is not None:
            refresh()
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
