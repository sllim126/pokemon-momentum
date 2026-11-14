"""Order alerts (Discord) and order emails (Microsoft 365, or the outbox folder).

A failed alert or email never undoes an order. Neither the Discord webhook URL, the Graph
client secret nor the SMTP password is ever logged.

How emails leave (settings.email_transport):
- graph:  Microsoft Graph sendMail, signed in as the app registration (client credentials).
          Exchange lets that app send only as orders@poke6s.com (a shared mailbox the owner
          sees in Outlook). Copies land in its Sent Items.
- smtp:   the older SMTP AUTH path (Microsoft turns password SMTP off by default end of 2026).
- outbox: no sending set up; written as .eml files to data/storefront/outbox/, so local and
          test-mode runs can be checked without sending anything. Live checkout refuses to
          open in this mode (settings.setup_problem).

An email that fails to send isn't lost (security review item 12): it's saved to
outbox/retry/ with a note, Discord is told, and retry_queued() (the app's 15-minute job) tries
again until it goes or 24 hours pass; then it moves to outbox/failed/ and Discord is told to
resend it by hand (orders_cli resend <number>).

Order emails have two parts: plain text, and a designed HTML version
(templates/email/order.html) with the logo attached to the email itself, so it shows even
before the site is public.
"""

from __future__ import annotations

import base64
import json
import logging
import smtplib
import time
from pathlib import Path
from urllib.parse import quote, urlsplit
from datetime import UTC, datetime
from email import message_from_bytes, policy
from email.message import EmailMessage
from email.utils import make_msgid, parseaddr
from zoneinfo import ZoneInfo

import requests
from jinja2 import Environment, FileSystemLoader, select_autoescape

from .settings import Settings

log = logging.getLogger("storefront.notify")
MOUNTAIN = ZoneInfo("America/Denver")
APP_DIR = Path(__file__).resolve().parent
LOGO = APP_DIR / "static" / "img" / "logo.png"
GRAPH = "https://graph.microsoft.com/v1.0"
GIVE_UP_AFTER_SECONDS = 24 * 3600
_graph_token: dict = {}   # {"value": ..., "expires": monotonic time}; one per process
_email_templates = Environment(loader=FileSystemLoader(APP_DIR / "templates" / "email"),
                               autoescape=select_autoescape(["html"]))

# Carrier tracking pages, for the "Track package" button (carrier names as typed when shipping).
TRACKING_URLS = {
    "usps": "https://tools.usps.com/go/TrackConfirmAction?tLabels={}",
    "ups": "https://www.ups.com/track?tracknum={}",
    "fedex": "https://www.fedex.com/fedextrack/?trknbr={}",
    "dhl": "https://www.dhl.com/us-en/home/tracking/tracking-express.html?tracking-id={}",
}


def money(cents: int) -> str:
    return f"${(cents or 0) / 100:,.2f}"


_email_templates.filters["money"] = money


def tracking_url(carrier: str | None, number: str | None) -> str:
    """The carrier's tracking page for a number, or '' for a carrier we don't know."""
    template = TRACKING_URLS.get((carrier or "").strip().lower())
    return template.format(quote(number.strip())) if template and number else ""


def local_time(iso: str | None) -> str:
    """'2026-11-05T19:04:00Z' -> 'Nov 5, 2026, 12:04 PM MST' (shop time, Utah)."""
    if not iso:
        return ""
    moment = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).astimezone(MOUNTAIN)
    return f"{moment:%b} {moment.day}, {moment.year}, {moment:%I:%M %p %Z}".replace(" 0", " ")


def discord(settings: Settings, text: str) -> None:
    if not settings.discord_webhook_url:
        return
    try:
        requests.post(settings.discord_webhook_url, json={"content": text[:1900]}, timeout=10)
    except requests.RequestException as exc:
        log.warning("Discord alert failed: %s", type(exc).__name__)


class EmailError(Exception):
    """An email could not be handed to Microsoft; the message is safe to log (no secrets)."""


def _graph_access_token(settings: Settings) -> str:
    """An app-only token for Graph, reused until a minute before it expires."""
    if _graph_token.get("value") and _graph_token.get("expires", 0) > time.monotonic():
        return _graph_token["value"]
    response = requests.post(
        f"https://login.microsoftonline.com/{settings.graph_tenant_id}/oauth2/v2.0/token",
        data={"grant_type": "client_credentials", "client_id": settings.graph_client_id,
              "client_secret": settings.graph_client_secret, "scope": "https://graph.microsoft.com/.default"},
        timeout=20)
    if response.status_code != 200:
        code = (response.json() if response.content else {}).get("error", "")
        raise EmailError(f"Microsoft sign-in refused (HTTP {response.status_code} {code})")
    body = response.json()
    _graph_token.update(value=body["access_token"], expires=time.monotonic() + int(body.get("expires_in", 3600)) - 60)
    return body["access_token"]


def _deliver(settings: Settings, message: EmailMessage) -> None:
    """Hand one finished message to Microsoft; raise EmailError if it isn't accepted."""
    try:
        if settings.email_transport == "graph":
            sender = parseaddr(str(message["From"]))[1]
            response = requests.post(
                f"{GRAPH}/users/{quote(sender)}/sendMail",
                # Graph takes the whole MIME message, base64-encoded, as text/plain. It must use
                # the email standard's CRLF line endings: with bare LF, Exchange misreads the
                # quoted-printable soft line breaks and a long line arrives as "wi=l" (seen on
                # the first test email, 2026-11-12).
                data=base64.b64encode(message.as_bytes(policy=policy.SMTP)),
                headers={"Authorization": f"Bearer {_graph_access_token(settings)}", "Content-Type": "text/plain"},
                timeout=30)
            if response.status_code != 202:
                try:
                    code = response.json()["error"]["code"]
                except (ValueError, KeyError, TypeError):
                    code = ""
                if response.status_code == 401:
                    _graph_token.clear()   # a stale token: sign in again next time
                raise EmailError(f"Graph sendMail refused (HTTP {response.status_code} {code})".strip())
        else:
            with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20) as smtp:
                smtp.starttls()
                if settings.smtp_user:
                    smtp.login(settings.smtp_user, settings.smtp_password)
                smtp.send_message(message)
    except (smtplib.SMTPException, OSError, requests.RequestException) as exc:
        raise EmailError(f"{type(exc).__name__}") from None


def _queue_dir(settings: Settings, name: str) -> Path:
    path = settings.outbox_dir / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def _queue(settings: Settings, message: EmailMessage, ref: str, error: str) -> None:
    """Keep a message that failed, for retry_queued(), and tell the owner."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
    folder = _queue_dir(settings, "retry")
    (folder / f"{stamp}.eml").write_bytes(bytes(message))
    note = {"ref": ref, "subject": str(message["Subject"]), "first_failed": time.time(), "attempts": 1,
            "last_error": error}
    (folder / f"{stamp}.json").write_text(json.dumps(note), encoding="utf-8")
    log.warning("Email for %s failed (%s); queued for retry", ref or "the shop", error)
    discord(settings, f"Email for {ref or 'the shop'} failed to send ({error}). Retrying every 15 minutes; "
                      f"nothing to do unless it gives up.")


def retry_queued(settings: Settings) -> dict[str, int]:
    """Try the queued emails again (the app's 15-minute job). Gives up after a day."""
    counts = {"sent": 0, "waiting": 0, "gave_up": 0}
    folder = settings.outbox_dir / "retry"
    if settings.email_transport == "outbox" or not folder.is_dir():
        return counts
    for note_path in sorted(folder.glob("*.json")):
        eml = note_path.with_suffix(".eml")
        note = json.loads(note_path.read_text(encoding="utf-8"))
        try:
            _deliver(settings, message_from_bytes(eml.read_bytes(), policy=policy.default))
        except EmailError as exc:
            note.update(attempts=note["attempts"] + 1, last_error=str(exc))
            if time.time() - note["first_failed"] > GIVE_UP_AFTER_SECONDS:
                failed = _queue_dir(settings, "failed")
                note_path.write_text(json.dumps(note), encoding="utf-8")
                eml.rename(failed / eml.name)
                note_path.rename(failed / note_path.name)
                counts["gave_up"] += 1
                discord(settings, f"GAVE UP on the email for {note['ref'] or 'the shop'} after "
                                  f"{note['attempts']} tries ({exc}). Fix sending, then resend it: "
                                  f"orders_cli resend {note['ref']}")
            else:
                note_path.write_text(json.dumps(note), encoding="utf-8")
                counts["waiting"] += 1
            continue
        eml.unlink()
        note_path.unlink()
        counts["sent"] += 1
        discord(settings, f"Email for {note['ref'] or 'the shop'} sent on retry {note['attempts'] + 1}.")
    return counts


def send_email(settings: Settings, to: str, subject: str, body: str, html: str = "",
               logo_cid: str = "", ref: str = "") -> bool:
    """Send (or, without email set up, save to the outbox) one email. With html, the message
    is multipart: plain text first, then HTML; logo_cid attaches the logo for <img src="cid:...">.
    `ref` (the order number) names it in alerts. Returns True if it was handed over or saved;
    False means it failed and was queued for retry."""
    if not to:
        return True
    message = EmailMessage()
    message["From"] = settings.mail_from
    message["To"] = to
    message["Subject"] = subject
    message["Message-ID"] = make_msgid(domain="poke6s.com")
    message.set_content(body)
    if html:
        message.add_alternative(html, subtype="html")
        if logo_cid and LOGO.exists():
            message.get_payload()[1].add_related(LOGO.read_bytes(), "image", "png", cid=f"<{logo_cid}>",
                                                 filename="poke6s.png")
    if settings.email_transport == "outbox":
        settings.outbox_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
        (settings.outbox_dir / f"{stamp}.eml").write_bytes(bytes(message))
        return True
    try:
        _deliver(settings, message)
    except EmailError as exc:
        _queue(settings, message, ref, str(exc))
        return False
    return True


def _lines(order: dict) -> str:
    rows = [f"  {item['quantity']} x {item['title']}  {money(item['unit_price_cents'] * item['quantity'])}"
            for item in order["items"]]
    totals = [f"  Subtotal  {money(order['subtotal_cents'])}"]
    if order["discount_cents"]:
        totals.append(f"  Discount  -{money(order['discount_cents'])}")
    totals.append(f"  Shipping  {money(order['shipping_cents']) if order['shipping_cents'] else 'Free'}")
    if order["tax_cents"]:
        totals.append(f"  Sales tax  {money(order['tax_cents'])}")
    totals.append(f"  Total  {money(order['total_cents'])}")
    return "\n".join(rows + [""] + totals)


def _address_lines(order: dict) -> list[str]:
    ship = order.get("shipping") or {}
    addr = ship.get("address") or {}
    parts = [ship.get("name") or order.get("name") or "", addr.get("line1") or "", addr.get("line2") or "",
             " ".join(p for p in [f"{addr.get('city') or ''},", addr.get("state") or "", addr.get("postal_code") or ""]
                      if p.strip(",")).strip()]
    return [p for p in parts if p]


def _address(order: dict) -> str:
    return "\n".join(f"  {p}" for p in _address_lines(order))


def _html(settings: Settings, kind: str, order: dict, status_url: str, subject: str, preheader: str) -> tuple[str, str]:
    """The HTML part of an order email and the logo's content ID."""
    parts = urlsplit(status_url)
    site_url = f"{parts.scheme}://{parts.netloc}" if parts.netloc else settings.base_url
    logo_cid = make_msgid(idstring="logo", domain="poke6s.com")[1:-1]
    html = _email_templates.get_template("order.html").render(
        kind=kind, order=order, status_url=status_url, site_url=site_url, subject=subject,
        preheader=preheader, contact_email=settings.contact_email, logo_cid=logo_cid,
        placed=local_time(order["paid_at"] or order["created_at"]), test=order["mode"] == "test",
        tracking_url=tracking_url(order.get("carrier"), order.get("tracking_number")),
        address_lines=_address_lines(order))
    return html, logo_cid


def order_confirmation(settings: Settings, order: dict, status_url: str) -> bool:
    test = "[TEST] " if order["mode"] == "test" else ""
    body = f"""Thanks for your order!

Order {order['number']}, placed {local_time(order['paid_at'] or order['created_at'])}.

{_lines(order)}

Shipping to:
{_address(order)}

We'll email you again with tracking when it ships. Order status: {status_url}

Questions? Reply to this email or write to {settings.contact_email}.

Poke6s
"""
    subject = f"{test}Poke6s order {order['number']} confirmed"
    html, cid = _html(settings, "confirmed", order, status_url, subject,
                      f"Thanks for your order! {money(order['total_cents'])}, we'll email tracking when it ships.")
    return send_email(settings, order["email"], subject, body, html, cid, ref=order["number"])


def shipped_email(settings: Settings, order: dict, status_url: str) -> bool:
    test = "[TEST] " if order["mode"] == "test" else ""
    tracking = f"{order['carrier'] or 'Tracking'}: {order['tracking_number']}" if order["tracking_number"] else ""
    link = tracking_url(order["carrier"], order["tracking_number"])
    if link:
        tracking += f"\nTrack it: {link}"
    body = f"""Good news: your Poke6s order {order['number']} has shipped.

{tracking}

{_lines(order)}

Shipping to:
{_address(order)}

Order status: {status_url}

Questions? Reply to this email or write to {settings.contact_email}.

Poke6s
"""
    subject = f"{test}Poke6s order {order['number']} has shipped"
    html, cid = _html(settings, "shipped", order, status_url, subject,
                      f"Your order is on its way. {order['carrier'] or 'Tracking'} {order['tracking_number'] or ''}".strip())
    return send_email(settings, order["email"], subject, body, html, cid, ref=order["number"])
