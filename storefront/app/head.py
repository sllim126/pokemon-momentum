"""Answer HEAD requests the way the page would answer GET, without the body.

FastAPI's @app.get routes don't accept HEAD (Starlette's plain routes do), so every page
answered HEAD with 405 Method Not Allowed. Link checkers and uptime monitors often send HEAD;
browsers and Google use GET. This middleware runs the request as a GET and drops the body, so
HEAD gets the same status and headers (Content-Type, Content-Length, redirects, 404s).

Install it before the metrics and visitor-counting middleware (main.py does), so it sits inside
them: they still see "HEAD", and the visit counter, which counts GETs only, keeps ignoring
monitors. Static files (/static) already answered HEAD; they still do.
"""

from __future__ import annotations

from starlette.types import ASGIApp, Message, Receive, Scope, Send


class HeadAsGet:
    """ASGI middleware: run HEAD as GET and send only the status and headers (module docstring)."""
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("method") != "HEAD":
            await self.app(scope, receive, send)
            return

        async def headers_only(message: Message) -> None:
            if message["type"] == "http.response.body":
                if message.get("more_body", False):
                    return                      # swallow streamed chunks; send one empty end
                message = {"type": "http.response.body", "body": b"", "more_body": False}
            await send(message)

        # Change the method in place, not on a copy: the router records the matched route in this
        # scope, and the metrics middleware reads it from there afterwards ("/shop", not unmatched).
        scope["method"] = "GET"
        try:
            await self.app(scope, receive, headers_only)
        finally:
            scope["method"] = "HEAD"
