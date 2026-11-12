"""Order emails through Microsoft Graph, the retry queue and the alerts (notify.py, review item 12).

Microsoft is replaced by FakeMicrosoft (requests.post patched), so nothing touches the network.
"""

from __future__ import annotations

import base64
import dataclasses
import json
import time
from email import message_from_bytes, policy

import pytest

from storefront.app import notify, orders_cli


class Reply:
    def __init__(self, status: int, body: dict | None = None) -> None:
        self.status_code = status
        self._body = body or {}
        self.content = json.dumps(self._body).encode() if body else b""

    def json(self):
        return self._body


class FakeMicrosoft:
    """Answers the token and sendMail calls; `down` makes sendMail fail."""

    def __init__(self) -> None:
        self.sent: list = []
        self.tokens = 0
        self.down = False

    def post(self, url, data=None, headers=None, timeout=None):
        if "login.microsoftonline.com" in url:
            self.tokens += 1
            assert data["client_secret"] == "secret" and data["grant_type"] == "client_credentials"
            return Reply(200, {"access_token": "tok", "expires_in": 3600})
        assert url == "https://graph.microsoft.com/v1.0/users/orders%40poke6s.com/sendMail"
        assert headers["Authorization"] == "Bearer tok" and headers["Content-Type"] == "text/plain"
        if self.down:
            return Reply(503, {"error": {"code": "ServiceUnavailable"}})
        raw = base64.b64decode(data)
        # Like Exchange: the MIME text must use CRLF line endings throughout (no bare LF).
        assert b"\n" not in raw.replace(b"\r\n", b""), "bare LF line endings"
        self.sent.append(message_from_bytes(raw, policy=policy.default))
        return Reply(202)


@pytest.fixture()
def microsoft(monkeypatch):
    fake = FakeMicrosoft()
    monkeypatch.setattr(notify.requests, "post", fake.post)
    notify._graph_token.clear()
    return fake


@pytest.fixture()
def alerts(monkeypatch):
    said: list[str] = []
    monkeypatch.setattr(notify, "discord", lambda settings, text: said.append(text))
    return said


@pytest.fixture()
def graph(settings):
    return dataclasses.replace(settings, graph_tenant_id="tenant", graph_client_id="client",
                               graph_client_secret="secret")


def test_transport_follows_the_settings(settings, graph):
    assert settings.email_transport == "outbox"
    assert graph.email_transport == "graph"
    assert dataclasses.replace(settings, smtp_host="smtp.example").email_transport == "smtp"


def test_graph_sends_the_whole_message_and_reuses_the_token(graph, microsoft, alerts):
    assert notify.send_email(graph, "buyer@example.com", "Hello", "Plain body", "<p>Html</p>", ref="P6-2026-0001")
    assert notify.send_email(graph, "buyer@example.com", "Again", "Second", ref="P6-2026-0002")
    assert microsoft.tokens == 1
    first = microsoft.sent[0]
    assert (first["To"], first["Subject"], first["From"]) == ("buyer@example.com", "Hello", "Poke6s <orders@poke6s.com>")
    assert "Plain body" in first.get_body(("plain",)).get_content()
    assert alerts == []


def test_a_failed_email_is_queued_and_the_owner_told(graph, microsoft, alerts):
    microsoft.down = True
    assert not notify.send_email(graph, "buyer@example.com", "Hello", "Body", ref="P6-2026-0001")
    queued = list((graph.outbox_dir / "retry").glob("*.eml"))
    assert len(queued) == 1
    assert "P6-2026-0001" in alerts[0] and "Retrying" in alerts[0]
    assert "secret" not in alerts[0]


def test_retry_sends_it_once_microsoft_is_back(graph, microsoft, alerts):
    microsoft.down = True
    notify.send_email(graph, "buyer@example.com", "Hello", "Body", ref="P6-2026-0001")
    assert notify.retry_queued(graph) == {"sent": 0, "waiting": 1, "gave_up": 0}
    microsoft.down = False
    assert notify.retry_queued(graph) == {"sent": 1, "waiting": 0, "gave_up": 0}
    assert microsoft.sent[0]["Subject"] == "Hello"
    assert not list((graph.outbox_dir / "retry").iterdir())
    assert "sent on retry" in alerts[-1]


def test_retry_gives_up_after_a_day(graph, microsoft, alerts):
    microsoft.down = True
    notify.send_email(graph, "buyer@example.com", "Hello", "Body", ref="P6-2026-0001")
    note = next((graph.outbox_dir / "retry").glob("*.json"))
    data = json.loads(note.read_text())
    data["first_failed"] = time.time() - 25 * 3600
    note.write_text(json.dumps(data))
    assert notify.retry_queued(graph)["gave_up"] == 1
    assert len(list((graph.outbox_dir / "failed").glob("*.eml"))) == 1
    assert "GAVE UP" in alerts[-1] and "orders_cli resend P6-2026-0001" in alerts[-1]


def test_a_refused_sign_in_is_queued_not_raised(graph, monkeypatch, alerts):
    monkeypatch.setattr(notify.requests, "post", lambda *a, **k: Reply(401, {"error": "invalid_client"}))
    notify._graph_token.clear()
    assert not notify.send_email(graph, "buyer@example.com", "Hello", "Body", ref="P6-2026-0001")
    assert "invalid_client" in alerts[0]


def test_outbox_mode_saves_without_sending(settings, monkeypatch):
    monkeypatch.setattr(notify.requests, "post", lambda *a, **k: pytest.fail("nothing may be sent"))
    assert notify.send_email(settings, "buyer@example.com", "Hello", "Body")
    assert len(list(settings.outbox_dir.glob("*.eml"))) == 1


def test_live_checkout_needs_email_sending(settings):
    live = dataclasses.replace(settings, stripe_secret_key="sk_live_x", stripe_webhook_secret="whsec_x", allow_live=True)
    assert not live.checkout_enabled and "STOREFRONT_GRAPH" in live.setup_problem
    ready = dataclasses.replace(live, graph_tenant_id="t", graph_client_id="c", graph_client_secret="s")
    assert ready.checkout_enabled


def test_cli_test_email(graph, microsoft, monkeypatch, capsys):
    monkeypatch.setattr(orders_cli, "load_settings", lambda: graph)
    assert orders_cli.main(["test-email", "--to", "me@example.com"]) == 0
    assert "sent via graph" in capsys.readouterr().out
    assert microsoft.sent[0]["To"] == "me@example.com"


def test_a_long_line_arrives_whole(graph, microsoft, alerts):
    line = "This is a test from the poke6s.com store. If you can read it, order emails will arrive too."
    notify.send_email(graph, "buyer@example.com", "Hello", line, "<p>" + "x" * 300 + "</p>")
    sent = microsoft.sent[0]
    assert sent.get_body(("plain",)).get_content().strip() == line
    assert "x" * 300 in sent.get_body(("html",)).get_content()
