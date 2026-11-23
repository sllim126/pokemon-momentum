"""Marketing emails to the store's email list (owner, 2026-11-14).

One template (templates/email/marketing.html, plus a plain-text part built here) for the
store's few notifications (owner, 2026-11-14: "basic notifications", not promo mail): we're
open, new stock, back in stock. Products are named by SKU and shown as cards (photo, price,
link) read from the store at send time, so a price or photo is never out of date and a
product that sold out or was hidden stops the send instead of being advertised. The owner writes the words in a small text
file; this module adds everything the law and the big mail providers require, and refuses to
send when something is missing. Sent with `orders_cli mailing` (preview first, then --send).

What every message carries, and why (CAN-SPAM Act, 15 U.S.C. 7704; FTC's compliance guide):
- An honest From and Reply-To: "Poke6s <orders@poke6s.com>", replies to the contact address.
- A subject the owner writes; it must describe the message truthfully (the owner's job: this
  code only refuses an empty one).
- Why the reader is getting it: "you joined the Poke6s email list at poke6s.com on <date>".
  The list is opt-in only (people type their address into the site's form), so the "this is
  an advertisement" notice that CAN-SPAM asks of unsolicited mail isn't needed. Never add
  addresses that didn't sign up themselves (customers included): then that notice would be
  required, and this module doesn't add it.
- The business's postal address (settings.postal_address, from .env).
- A working unsubscribe link: one page, one button, no sign-in. It must keep working for at
  least 30 days after a send, and requests must be honoured within 10 business days; ours are
  instant. Plus the List-Unsubscribe and List-Unsubscribe-Post headers (RFC 8058), which
  Gmail and Yahoo require of bulk senders and which put an "Unsubscribe" button in the mail
  app itself.
- Unsubscribed addresses are skipped for good (notify_list.subscribers); they can't be sold
  or shared (the Privacy page says we never share the list).
Utah adds no general rules for this kind of email. Its Child Protection Registry covers only
messages about things minors may not buy (alcohol, tobacco, gambling, adult content).

Message file format (UTF-8 text):

    Subject: New this week: Ascended Heroes ETBs
    Preheader: Six in stock, shipped from Utah.          (optional: the inbox preview line)
    Button: Shop now | https://poke6s.com/shop             (optional: text | link)
    Products: EN-AH-ETB, ENG-PO-BOX                        (optional: up to 6 store SKUs)

    # Fresh stock is in                                    (the big heading)

    First paragraph. Blank lines separate paragraphs.

    Second paragraph.
"""

from __future__ import annotations

import io
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import make_msgid

from . import notify, notify_list
from .notify import MOUNTAIN, _email_templates
from .settings import Settings

# Microsoft 365 accepts about 30 messages a minute from one mailbox; stay under it.
PAUSE_BETWEEN_SENDS = 2.5
MAX_PRODUCTS = 6
# Card photos are attached to the email as JPEG (Outlook for Windows can't show WebP), from the
# store's 480 px copies, centred on a same-size white canvas so every card in a row lines up
# whatever the photo's shape (shown at 200 px wide: sharp on phones). About 20-40 KB each.
CARD_IMAGE_WIDTH = 480
CARD_CANVAS = (480, 400)
CARD_PADDING = 16


@dataclass
class Message:
    subject: str = ""
    preheader: str = ""
    heading: str = ""
    paragraphs: list[str] = field(default_factory=list)
    button_text: str = ""
    button_url: str = ""
    skus: list[str] = field(default_factory=list)


def parse_message(text: str) -> Message:
    """Read a message file (format in the module docstring)."""
    message = Message()
    header, _, body = text.replace("\r\n", "\n").partition("\n\n")
    for line in header.split("\n"):
        name, _, value = line.partition(":")
        name, value = name.strip().lower(), value.strip()
        if name == "subject":
            message.subject = value
        elif name == "preheader":
            message.preheader = value
        elif name == "button":
            text_part, _, url = value.partition("|")
            message.button_text, message.button_url = text_part.strip(), url.strip()
        elif name == "products":
            message.skus = [sku.strip() for sku in value.split(",") if sku.strip()]
        elif line.strip():
            raise ValueError(f"Unknown header line in the message file: {line!r}")
    for block in body.strip().split("\n\n"):
        block = " ".join(block.split())
        if block.startswith("# ") and not message.heading:
            message.heading = block[2:].strip()
        elif block:
            message.paragraphs.append(block)
    return message


def load_products(settings: Settings, conn: sqlite3.Connection, skus: list[str]) -> tuple[list[dict], list[str]]:
    """The cards for these SKUs, in the order given, and the problems found (unknown, hidden,
    sold out). Each card: sku, title, price_cents, url, image (JPEG bytes or None)."""
    cards, found = [], []
    for sku in skus:
        row = conn.execute("SELECT * FROM products WHERE sku = ?", (sku,)).fetchone()
        if row is None:
            found.append(f"Products: no product with SKU {sku}.")
        elif not row["visible"]:
            found.append(f"Products: {sku} is hidden on the store.")
        elif row["track_stock"] and row["stock"] <= 0:
            found.append(f"Products: {sku} is sold out; take it out of the email.")
        else:
            image = conn.execute("SELECT file_name FROM product_images WHERE product_id = ? "
                                 "ORDER BY sort_order, id LIMIT 1", (row["id"],)).fetchone()
            cards.append({"sku": sku, "title": row["title"], "price_cents": row["price_cents"],
                          "url": f"{settings.base_url}/p/{row['slug']}",
                          "image": _card_image(settings, image["file_name"]) if image else None})
    return cards, found


def _card_image(settings: Settings, file_name: str) -> bytes | None:
    """The product photo as a JPEG for email, from the store's 480 px WebP copy."""
    from PIL import Image, ImageChops

    source = settings.images_dir / str(CARD_IMAGE_WIDTH) / f"{file_name}.webp"
    if not source.exists():
        return None
    with Image.open(source) as picture:
        picture = picture.convert("RGBA")
        on_white = Image.new("RGB", picture.size, "#ffffff")   # JPEG has no transparency: white behind
        on_white.paste(picture, mask=picture.getchannel("A"))
        # Many shop photos sit in wide white borders; trim them (anything near-white) so the
        # product fills its card. A photo that is all white is left as it is.
        ink = ImageChops.difference(on_white, Image.new("RGB", on_white.size, "#ffffff")).convert("L")
        box = ink.point(lambda value: 255 if value > 12 else 0).getbbox()
        if box:
            on_white = on_white.crop(box)
        # Fit the box, scaling small photos up too: some shop photos are only 200 px, and the
        # card shows this at 200 px wide, so they still look as sharp as they can.
        room = (CARD_CANVAS[0] - 2 * CARD_PADDING, CARD_CANVAS[1] - 2 * CARD_PADDING)
        scale = min(room[0] / on_white.width, room[1] / on_white.height)
        on_white = on_white.resize((max(1, round(on_white.width * scale)), max(1, round(on_white.height * scale))),
                                   Image.Resampling.LANCZOS)
        flat = Image.new("RGB", CARD_CANVAS, "#ffffff")
        flat.paste(on_white, ((CARD_CANVAS[0] - on_white.width) // 2, (CARD_CANVAS[1] - on_white.height) // 2))
        out = io.BytesIO()
        flat.save(out, "JPEG", quality=82, optimize=True, progressive=True)
    return out.getvalue()


def problems(settings: Settings, message: Message, sending: bool,
             conn: sqlite3.Connection | None = None) -> list[str]:
    """Everything that stops this message going out. Empty = fine. With conn, the products
    are checked against the store too."""
    found = []
    if not settings.postal_address:
        found.append("STOREFRONT_POSTAL_ADDRESS is not set in .env: the law requires a postal address in "
                     "every marketing email (a street address, a USPS PO box or a private mailbox).")
    if not message.subject:
        found.append("The message has no Subject line.")
    if not message.paragraphs:
        found.append("The message has no text.")
    if bool(message.button_text) != bool(message.button_url):
        found.append("Button needs both text and a link: 'Button: Shop now | https://poke6s.com/shop'.")
    if message.button_url and not message.button_url.startswith("https://"):
        found.append("The button link must start with https://.")
    if not settings.base_url.startswith("https://"):
        found.append(f"STOREFRONT_BASE_URL is {settings.base_url}: unsubscribe links must work from the "
                     "internet (https://poke6s.com).")
    if sending and settings.email_transport == "outbox":
        found.append("No email sending is set up (STOREFRONT_GRAPH_*).")
    if len(message.skus) > MAX_PRODUCTS:
        found.append(f"Products: at most {MAX_PRODUCTS} (short emails get read).")
    if conn is not None and message.skus:
        found += load_products(settings, conn, message.skus)[1]
    return found


def signed_up_on(created_at: str) -> str:
    """'2026-11-11T21:40:00Z' -> 'November 11, 2026' (shop time)."""
    try:
        moment = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).astimezone(MOUNTAIN)
    except (TypeError, ValueError):
        return ""
    return f"{moment:%B} {moment.day}, {moment.year}"


def render(settings: Settings, message: Message, unsubscribe_url: str, signed_up: str,
           cards: list[dict] | None = None) -> tuple[str, str, list[tuple[str, bytes, str]]]:
    """(plain text, HTML, pictures to attach as (content ID, bytes, subtype)) for one recipient.
    The logo comes first; then one JPEG per product card that has a photo."""
    cards = [dict(card) for card in cards or []]
    attachments: list[tuple[str, bytes, str]] = []
    for card in cards:
        card["cid"] = ""
        if card.get("image"):
            card["cid"] = make_msgid(idstring=card["sku"].lower(), domain="poke6s.com")[1:-1]
            attachments.append((card["cid"], card["image"], "jpeg"))
    why = (f"You're getting this because you joined the Poke6s email list at poke6s.com"
           f"{' on ' + signed_up if signed_up else ''}.")
    text_parts = ([message.heading] if message.heading else []) + message.paragraphs
    if cards:
        text_parts.append("\n".join(f"- {c['title']}, {notify.money(c['price_cents'])}\n  {c['url']}" for c in cards))
    if message.button_url:
        text_parts.append(f"{message.button_text}: {message.button_url}")
    text_parts += ["Poke6s", "--", why, f"Unsubscribe (one click, no sign-in): {unsubscribe_url}",
                   postal_line(settings),
                   f"Questions? Reply to this email or write to {settings.contact_email}."]
    logo_cid = make_msgid(idstring="logo", domain="poke6s.com")[1:-1]
    html = _email_templates.get_template("marketing.html").render(
        message=message, why=why, unsubscribe_url=unsubscribe_url, postal_address=postal_line(settings),
        contact_email=settings.contact_email, site_url=settings.base_url, logo_cid=logo_cid, cards=cards)
    logo = [(logo_cid, notify.LOGO.read_bytes(), "png")] if notify.LOGO.exists() else []
    return "\n\n".join(text_parts) + "\n", html, logo + attachments


def postal_line(settings: Settings) -> str:
    """The footer's sender line: the company name and postal address. The address in .env may
    already start with the name ("Poke6s LLC, ..."); then it isn't added twice."""
    address = settings.postal_address.strip()
    return address if address.lower().startswith("poke6s") else f"Poke6s LLC, {address}"


def unsubscribe_url(settings: Settings, token: str) -> str:
    return f"{settings.base_url}/unsubscribe/{token}"


def send_one(settings: Settings, message: Message, to: str, token: str, signed_up: str,
             cards: list[dict] | None = None) -> bool:
    url = unsubscribe_url(settings, token)
    body, html, pictures = render(settings, message, url, signed_up, cards)
    headers = {"Reply-To": settings.contact_email,
               "List-Unsubscribe": f"<{url}>",
               "List-Unsubscribe-Post": "List-Unsubscribe=One-Click"}
    return notify.send_email(settings, to, message.subject, body, html, ref=f"the mailing to {to}",
                             headers=headers, related=pictures)


def send_to_list(settings: Settings, conn: sqlite3.Connection, message: Message,
                 pause: float = PAUSE_BETWEEN_SENDS) -> dict[str, int]:
    """Send the message to everyone still subscribed. Stops before sending anything if the
    message or settings break a rule. Returns counts."""
    found = problems(settings, message, sending=True, conn=conn)
    if found:
        raise ValueError("Not sent:\n- " + "\n- ".join(found))
    cards = load_products(settings, conn, message.skus)[0]
    counts = {"sent": 0, "failed": 0}
    for index, row in enumerate(notify_list.subscribers(conn)):
        if index and pause:
            time.sleep(pause)
        token = notify_list.token_for(conn, row["email"])
        ok = send_one(settings, message, row["email"], token, signed_up_on(row["created_at"]), cards)
        counts["sent" if ok else "failed"] += 1
    notify.discord(settings, f"Mailing \"{message.subject}\": {counts['sent']} sent, {counts['failed']} queued "
                             f"for retry.")
    return counts
