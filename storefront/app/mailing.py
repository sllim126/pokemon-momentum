"""Marketing emails to the store's email list (owner, 2026-11-14).

One template (templates/email/marketing.html, plus a plain-text part built here) for every
future announcement: new stock, a sale, a restock. The owner writes the words in a small text
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

    # Fresh stock is in                                    (the big heading)

    First paragraph. Blank lines separate paragraphs.

    Second paragraph.
"""

from __future__ import annotations

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


@dataclass
class Message:
    subject: str = ""
    preheader: str = ""
    heading: str = ""
    paragraphs: list[str] = field(default_factory=list)
    button_text: str = ""
    button_url: str = ""


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
        elif line.strip():
            raise ValueError(f"Unknown header line in the message file: {line!r}")
    for block in body.strip().split("\n\n"):
        block = " ".join(block.split())
        if block.startswith("# ") and not message.heading:
            message.heading = block[2:].strip()
        elif block:
            message.paragraphs.append(block)
    return message


def problems(settings: Settings, message: Message, sending: bool) -> list[str]:
    """Everything that stops this message going out. Empty = fine."""
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
    return found


def signed_up_on(created_at: str) -> str:
    """'2026-11-11T21:40:00Z' -> 'November 11, 2026' (shop time)."""
    try:
        moment = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).astimezone(MOUNTAIN)
    except (TypeError, ValueError):
        return ""
    return f"{moment:%B} {moment.day}, {moment.year}"


def render(settings: Settings, message: Message, unsubscribe_url: str, signed_up: str) -> tuple[str, str, str]:
    """(plain text, HTML, logo content ID) for one recipient."""
    why = (f"You're getting this because you joined the Poke6s email list at poke6s.com"
           f"{' on ' + signed_up if signed_up else ''}.")
    text_parts = ([message.heading] if message.heading else []) + message.paragraphs
    if message.button_url:
        text_parts.append(f"{message.button_text}: {message.button_url}")
    text_parts += ["Poke6s", "--", why, f"Unsubscribe (one click, no sign-in): {unsubscribe_url}",
                   f"Poke6s LLC, {settings.postal_address}",
                   f"Questions? Reply to this email or write to {settings.contact_email}."]
    logo_cid = make_msgid(idstring="logo", domain="poke6s.com")[1:-1]
    html = _email_templates.get_template("marketing.html").render(
        message=message, why=why, unsubscribe_url=unsubscribe_url, postal_address=settings.postal_address,
        contact_email=settings.contact_email, site_url=settings.base_url, logo_cid=logo_cid)
    return "\n\n".join(text_parts) + "\n", html, logo_cid


def unsubscribe_url(settings: Settings, token: str) -> str:
    return f"{settings.base_url}/unsubscribe/{token}"


def send_one(settings: Settings, message: Message, to: str, token: str, signed_up: str) -> bool:
    url = unsubscribe_url(settings, token)
    body, html, cid = render(settings, message, url, signed_up)
    headers = {"Reply-To": settings.contact_email,
               "List-Unsubscribe": f"<{url}>",
               "List-Unsubscribe-Post": "List-Unsubscribe=One-Click"}
    return notify.send_email(settings, to, message.subject, body, html, cid, ref=f"the mailing to {to}",
                             headers=headers)


def send_to_list(settings: Settings, conn: sqlite3.Connection, message: Message,
                 pause: float = PAUSE_BETWEEN_SENDS) -> dict[str, int]:
    """Send the message to everyone still subscribed. Stops before sending anything if the
    message or settings break a rule. Returns counts."""
    found = problems(settings, message, sending=True)
    if found:
        raise ValueError("Not sent:\n- " + "\n- ".join(found))
    counts = {"sent": 0, "failed": 0}
    for index, row in enumerate(notify_list.subscribers(conn)):
        if index and pause:
            time.sleep(pause)
        token = notify_list.token_for(conn, row["email"])
        ok = send_one(settings, message, row["email"], token, signed_up_on(row["created_at"]))
        counts["sent" if ok else "failed"] += 1
    notify.discord(settings, f"Mailing \"{message.subject}\": {counts['sent']} sent, {counts['failed']} queued "
                             f"for retry.")
    return counts
