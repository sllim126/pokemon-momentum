"""Support (one Poke6s account, stage 3; app/support.py): the Contact page's email, Discord and
message box; messages saved, a Discord ping without customer details, limits against spam, and
the admin Overview's list with "Mark answered"."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from storefront.admin.main import create_admin_app
from storefront.app import notify, support
from storefront.app.db import connect

from storefront.tests.test_accounts import add_order, market_db, now_iso
from storefront.tests.test_admin import HOME, admin, config, password_hash, sign_in  # noqa: F401

GOOD = {"name": "Misty Waterflower", "email": "Misty@Example.com", "order_number": "",
        "message": "Does the Ascended Heroes ETB come sealed?"}


def messages(settings) -> list[dict]:
    with connect(settings.db_path) as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM support_messages ORDER BY id")]


def test_contact_page_has_three_ways(client, imported):
    page = client.get("/contact").text
    assert 'href="mailto:' in page and "Email us" in page
    assert 'href="https://discord.gg/' in page and "Often the fastest" in page
    assert 'action="/support"' in page and 'name="website"' in page
    # From "Get help with this order": the number is filled in; a bad one is ignored.
    assert 'value="P6-2026-0010"' in client.get("/contact?order=p6-2026-0010").text
    assert 'value="&lt;b&gt;"' not in client.get("/contact?order=<b>").text


def test_message_is_saved_and_pings_discord_without_details(client, imported, monkeypatch):
    pings = []
    monkeypatch.setattr(notify, "discord", lambda settings, text: pings.append(text))
    response = client.post("/support", data={**GOOD, "order_number": "p6-2026-0010"})
    assert response.status_code == 303 and response.headers["location"] == "/contact?sent=1#support"
    saved = messages(imported)
    assert len(saved) == 1 and saved[0]["email"] == "misty@example.com" and saved[0]["order_number"] == "P6-2026-0010"
    assert pings == ["New support message #1 about P6-2026-0010 on poke6s.com. Read it on the admin Overview."]
    assert "Misty" not in pings[0] and "@" not in pings[0]
    assert "we've got your message" in client.get("/contact?sent=1").text


def test_bad_input_bots_and_floods(client, imported, monkeypatch):
    monkeypatch.setattr(notify, "discord", lambda settings, text: None)
    bad = client.post("/support", data={**GOOD, "email": "not-an-email"})
    assert bad.status_code == 400 and "email address we can reply to" in bad.text and "Ascended Heroes" in bad.text
    assert client.post("/support", data={**GOOD, "order_number": "12345"}).status_code == 400
    # The bot trap: looks sent, saves nothing.
    assert client.post("/support", data={**GOOD, "website": "spam.example"}).status_code == 303
    assert messages(imported) == []
    assert client.post("/support", data=GOOD, headers={"Origin": "https://evil.example"}).status_code == 403
    for _ in range(3):
        assert client.post("/support", data=GOOD).status_code == 303
    assert client.post("/support", data=GOOD).status_code == 429
    assert len(messages(imported)) == 3


def test_signed_in_email_is_filled_in_and_orders_link_here(client, imported, monkeypatch):
    monkeypatch.setattr(notify, "discord", lambda settings, text: None)
    market_db(imported, {"good": ("misty@example.com", now_iso(5), now_iso(1))})
    add_order(imported, "P6-2026-0010", "misty@example.com", "shipped")
    client.cookies.set("poke6s_session", "good")
    assert 'href="/contact?order=P6-2026-0010#support"' in client.get("/account").text
    page = client.get("/contact?order=P6-2026-0010")
    assert 'value="misty@example.com"' in page.text and page.headers["cache-control"] == "private, no-store"
    client.post("/support", data={**GOOD, "email": "misty@example.com"})
    assert messages(imported)[0]["signed_in"] == 1


def test_old_answered_messages_are_dropped(imported):
    with connect(imported.db_path) as conn:
        first = support.add(conn, support.clean(**{"name": "A", "email": "a@example.com", "order": "",
                                                   "message": "Hello there"})[0])
        support.add(conn, support.clean(name="B", email="b@example.com", order="", message="Still open")[0])
        support.mark_answered(conn, first)
        later = datetime.now(UTC) + timedelta(days=support.KEEP_ANSWERED_DAYS + 1)
        assert support.drop_old(conn, later) == 1
        assert [r["name"] for r in conn.execute("SELECT name FROM support_messages")] == ["B"]


def test_admin_overview_lists_and_answers(imported, config):  # noqa: F811
    with connect(imported.db_path) as conn:
        support.add(conn, support.clean(name="Misty", email="misty@example.com", order="P6-2026-0010",
                                        message="Where is my order?")[0])
    with TestClient(create_admin_app(imported, config), client=HOME, follow_redirects=False) as client:
        page = sign_in(client).get("/").text
        assert "Support messages (1 to answer)" in page and "Where is my order?" in page
        assert "1 support message to answer (below)" in page and "mailto:misty@example.com" in page
        token = page.split('name="form_token" value="')[1].split('"')[0]
        response = client.post("/support/1/answered", data={"form_token": token})
        assert response.status_code == 303 and response.headers["location"] == "/#support"
        assert "None waiting." in client.get("/").text
        assert client.post("/support/1/answered", data={"form_token": "nope"}).status_code == 403
