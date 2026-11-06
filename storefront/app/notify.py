"""Order alerts (Discord) and order emails (Microsoft 365 SMTP, or the outbox folder).

Both are best-effort: a failed alert or email is logged and never undoes an order. Neither the
Discord webhook URL nor the SMTP password is ever logged.

Without STOREFRONT_SMTP_HOST, emails are written as .eml files to data/storefront/outbox/,
so local and test-mode runs can be checked without sending anything.

Order emails have two parts: plain text, and a designed HTML version
(templates/email/order.html) with the logo attached to the email itself, so it shows even
before the site is public.
"""

from __future__ import annotations

import logging
import smtplib
from pathlib import Path
from urllib.parse import quote, urlsplit
from datetime import UTC, datetime
from email.message import EmailMessage
from email.utils import make_msgid
from zoneinfo import ZoneInfo

import requests
from jinja2 import Environment, FileSystemLoader, select_autoescape

from .settings import Settings

log = logging.getLogger("storefront.notify")
MOUNTAIN = ZoneInfo("America/Denver")
APP_DIR = Path(__file__).resolve().parent
LOGO = APP_DIR / "static" / "img" / "logo.png"
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


def send_email(settings: Settings, to: str, subject: str, body: str, html: str = "",
               logo_cid: str = "") -> None:
    """Send (or, without SMTP, save to the outbox) one email. With html, the message is
    multipart: plain text first, then HTML; logo_cid attaches the logo for <img src="cid:...">."""
    if not to:
        return
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
    if not settings.smtp_host:
        settings.outbox_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
        (settings.outbox_dir / f"{stamp}.eml").write_bytes(bytes(message))
        return
    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=20) as smtp:
            smtp.starttls()
            if settings.smtp_user:
                smtp.login(settings.smtp_user, settings.smtp_password)
            smtp.send_message(message)
    except (smtplib.SMTPException, OSError) as exc:
        log.warning("Email to order customer failed: %s", type(exc).__name__)


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


def order_confirmation(settings: Settings, order: dict, status_url: str) -> None:
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
    send_email(settings, order["email"], subject, body, html, cid)


def shipped_email(settings: Settings, order: dict, status_url: str) -> None:
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
    send_email(settings, order["email"], subject, body, html, cid)
