"""Marketing emails to the list (owner, 2026-11-14): the legal pieces are always there, sending
refuses without them, and unsubscribing works from the email with no sign-in."""

from __future__ import annotations

import dataclasses
import sqlite3
from email import message_from_bytes, policy

import pytest

from storefront.app import mailing, notify_list, orders_cli
from storefront.app.db import connect

MESSAGE = """Subject: Fresh stock this week
Preheader: Six ETBs in, shipped from Utah.
Button: Shop now | https://poke6s.com/shop

# Fresh stock is in

Ascended Heroes ETBs are back.

Free shipping over $150 still applies.
"""


@pytest.fixture()
def ready(imported):
    """Store settings with the postal address and a public address set."""
    return dataclasses.replace(imported, base_url="https://poke6s.com", postal_address="PO Box 1, Example, UT 84000")


def join(settings, *emails):
    conn = connect(settings.db_path, writer=True)
    for email in emails:
        notify_list.add(conn, email, "/notify")
    return conn


def sent(settings) -> list:
    return [message_from_bytes(p.read_bytes(), policy=policy.default)
            for p in sorted(settings.outbox_dir.glob("*.eml"))]


def test_parse_message():
    message = mailing.parse_message(MESSAGE)
    assert message.subject == "Fresh stock this week"
    assert message.heading == "Fresh stock is in"
    assert message.paragraphs == ["Ascended Heroes ETBs are back.", "Free shipping over $150 still applies."]
    assert (message.button_text, message.button_url) == ("Shop now", "https://poke6s.com/shop")
    with pytest.raises(ValueError):
        mailing.parse_message("Subjct: typo\n\nText")


def test_refuses_without_the_postal_address_or_a_public_address(imported):
    local = dataclasses.replace(imported, base_url="http://localhost:8010")
    found = mailing.problems(local, mailing.parse_message(MESSAGE), sending=False)
    assert any("STOREFRONT_POSTAL_ADDRESS" in p for p in found)
    assert any("STOREFRONT_BASE_URL" in p for p in found)
    with pytest.raises(ValueError):
        mailing.send_to_list(imported, join(imported, "a@example.com"), mailing.parse_message(MESSAGE), pause=0)
    assert sent(imported) == []


def test_every_email_carries_the_legal_parts(ready):
    # send_one writes to the outbox here (no Graph settings), so the emails can be read back.
    message = mailing.parse_message(MESSAGE)
    conn = join(ready, "a@example.com", "b@example.com")
    for row in notify_list.subscribers(conn):
        mailing.send_one(ready, message, row["email"], notify_list.token_for(conn, row["email"]),
                         mailing.signed_up_on(row["created_at"]))
    emails = sent(ready)
    assert len(emails) == 2
    tokens = set()
    for email in emails:
        token = email["List-Unsubscribe"].strip("<>").rsplit("/", 1)[1]
        tokens.add(token)
        assert email["List-Unsubscribe"] == f"<https://poke6s.com/unsubscribe/{token}>"
        assert email["List-Unsubscribe-Post"] == "List-Unsubscribe=One-Click"
        assert email["Reply-To"] == ready.contact_email
        assert email["From"] == ready.mail_from
        for part in ("plain", "html"):
            content = email.get_body(preferencelist=(part,)).get_content()
            assert f"https://poke6s.com/unsubscribe/{token}" in content
            assert "PO Box 1, Example, UT 84000" in content
            assert "joined the Poke6s email list at poke6s.com on" in content
    assert len(tokens) == 2   # each address has its own


def test_unsubscribe_from_the_link(client, imported):
    conn = join(imported, "a@example.com")
    token = notify_list.token_for(conn, "a@example.com")
    page = client.get(f"/unsubscribe/{token}")
    assert page.status_code == 200 and "a@example.com" in page.text and 'method="post"' in page.text
    assert notify_list.subscribers(conn)   # looking at the page changes nothing
    done = client.post(f"/unsubscribe/{token}")
    assert done.status_code == 200 and "You're unsubscribed" in done.text
    assert notify_list.subscribers(conn) == []
    assert "You're unsubscribed" in client.get(f"/unsubscribe/{token}").text
    assert client.post(f"/unsubscribe/{token}").status_code == 200   # pressing twice is fine


def test_one_click_unsubscribe_from_the_mail_app(client, imported):
    conn = join(imported, "a@example.com")
    token = notify_list.token_for(conn, "a@example.com")
    response = client.post(f"/unsubscribe/{token}", data={"List-Unsubscribe": "One-Click"})
    assert response.status_code == 200 and response.text == "Unsubscribed."
    assert notify_list.subscribers(conn) == []


def test_unknown_or_odd_tokens(client):
    for token in ("x" * 32, "short", "a%27%20or%201=1--xxxxxxxxxxxxxxxx"):
        assert client.get(f"/unsubscribe/{token}").status_code == 404
        assert client.post(f"/unsubscribe/{token}").status_code == 404


def test_signing_up_again_resubscribes(imported):
    conn = join(imported, "a@example.com")
    notify_list.unsubscribe(conn, notify_list.token_for(conn, "a@example.com"))
    assert notify_list.add(conn, "a@example.com", "/notify") is True
    assert [r["email"] for r in notify_list.subscribers(conn)] == ["a@example.com"]
    assert notify_list.add(conn, "a@example.com", "/notify") is False


def test_cli_preview_writes_files_and_sends_nothing(ready, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(orders_cli, "load_settings", lambda: ready)
    join(ready, "a@example.com")
    path = tmp_path / "news.txt"
    path.write_text(MESSAGE, encoding="utf-8")
    assert orders_cli.main(["mailing", str(path)]) == 0
    assert "1 subscribed address" in capsys.readouterr().out
    assert "Unsubscribe" in (tmp_path / "news.preview.html").read_text(encoding="utf-8")
    assert sent(ready) == []


def test_cli_send_refuses_without_email_sending(ready, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(orders_cli, "load_settings", lambda: ready)
    path = tmp_path / "news.txt"
    path.write_text(MESSAGE, encoding="utf-8")
    assert orders_cli.main(["mailing", str(path), "--send"]) == 1
    assert "No email sending is set up" in capsys.readouterr().err


def test_privacy_page_matches(client):
    html = client.get("/privacy").text
    assert "unsubscribe" in html.lower()


# --- product cards -------------------------------------------------------------------------

def add_photo(settings, sku: str) -> None:
    """Give a product a (transparent, to test the white fill) photo, as the importer would."""
    from PIL import Image

    folder = settings.images_dir / "480"
    folder.mkdir(parents=True, exist_ok=True)
    Image.new("RGBA", (480, 600), (200, 30, 30, 0)).save(folder / "photo-test.webp", "WEBP")
    with sqlite3.connect(settings.db_path) as conn:
        conn.execute("INSERT INTO product_images (product_id, file_name, original_ext) "
                     "SELECT id, 'photo-test', 'png' FROM products WHERE sku = ?", (sku,))


def test_products_line_and_store_checks(ready):
    message = mailing.parse_message(MESSAGE.replace("Button:", "Products: ENG-BB-BB, JP-BB-BOX, NOPE, ENG-G25-TIN\nButton:"))
    assert message.skus == ["ENG-BB-BB", "JP-BB-BOX", "NOPE", "ENG-G25-TIN"]
    with sqlite3.connect(ready.db_path) as conn:
        conn.execute("UPDATE products SET stock = 0 WHERE sku = 'ENG-G25-TIN'")
    conn = connect(ready.db_path)
    cards, found = mailing.load_products(ready, conn, message.skus)
    assert [c["sku"] for c in cards] == ["ENG-BB-BB"]
    assert cards[0]["url"].startswith("https://poke6s.com/p/") and cards[0]["image"] is None
    assert any("JP-BB-BOX is hidden" in p for p in found)
    assert any("no product with SKU NOPE" in p for p in found)
    assert any("ENG-G25-TIN is sold out" in p for p in found)
    assert len(mailing.problems(ready, message, sending=False, conn=conn)) == 3


def test_cards_show_in_both_parts_with_attached_photos(ready):
    add_photo(ready, "ENG-BB-BB")
    message = mailing.parse_message(MESSAGE.replace("Button:", "Products: ENG-BB-BB, JP-AE-BB\nButton:"))
    conn = join(ready, "a@example.com")
    cards, found = mailing.load_products(ready, conn, message.skus)
    assert not found and cards[0]["image"][:3] == b"\xff\xd8\xff"   # a JPEG
    mailing.send_one(ready, message, "a@example.com", notify_list.token_for(conn, "a@example.com"), "", cards)
    email = sent(ready)[0]
    html = email.get_body(preferencelist=("html",)).get_content()
    text = email.get_body(preferencelist=("plain",)).get_content()
    for card in cards:
        assert card["url"] in html and card["url"] in text
    pictures = [part for part in email.walk() if part.get_content_maintype() == "image"]
    assert sorted(p.get_content_subtype() for p in pictures) == ["jpeg", "png"]   # one photo + the logo
    photo = next(p for p in pictures if p.get_content_subtype() == "jpeg")
    assert f"cid:{photo['Content-ID'].strip('<>')}" in html


def test_preview_shows_the_photos_from_files(ready, tmp_path, monkeypatch):
    monkeypatch.setattr(orders_cli, "load_settings", lambda: ready)
    add_photo(ready, "ENG-BB-BB")
    path = tmp_path / "news.txt"
    path.write_text(MESSAGE.replace("Button:", "Products: ENG-BB-BB\nButton:"), encoding="utf-8")
    assert orders_cli.main(["mailing", str(path)]) == 0
    html = (tmp_path / "news.preview.html").read_text(encoding="utf-8")
    assert "cid:" not in html and "news.preview-files/picture-2.jpg" in html
    assert (tmp_path / "news.preview-files" / "picture-2.jpg").exists()


def test_postal_line_names_the_company_once(imported):
    """The .env address may already start with the company name (owner's test email, 2026-11-23)."""
    from storefront.app.mailing import postal_line
    assert postal_line(dataclasses.replace(imported, postal_address="PO Box 1, Example, UT 84000")) == \
        "Poke6s LLC, PO Box 1, Example, UT 84000"
    assert postal_line(dataclasses.replace(imported, postal_address="Poke6s LLC, PO Box 1, Example, UT 84000")) == \
        "Poke6s LLC, PO Box 1, Example, UT 84000"


def test_email_logo_has_a_transparent_background():
    """Dark-mode mail apps recolour the header, not pictures: the logo must have no background."""
    from PIL import Image
    from storefront.app import notify
    with Image.open(notify.LOGO) as logo:
        assert logo.mode == "RGBA" and logo.getpixel((0, 0))[3] == 0
