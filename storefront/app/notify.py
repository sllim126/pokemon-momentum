"""Order alerts (Discord) and order emails (Microsoft 365 SMTP, or the outbox folder).

Both are best-effort: a failed alert or email is logged and never undoes an order. Neither the
Discord webhook URL nor the SMTP password is ever logged.

Without STOREFRONT_SMTP_HOST, emails are written as .eml files to data/storefront/outbox/,
so local and test-mode runs can be checked without sending anything.
"""

from __future__ import annotations

import logging
import smtplib
from datetime import UTC, datetime
from email.message import EmailMessage
from email.utils import make_msgid
from zoneinfo import ZoneInfo

import requests

from .settings import Settings

log = logging.getLogger("storefront.notify")
MOUNTAIN = ZoneInfo("America/Denver")


def money(cents: int) -> str:
    return f"${(cents or 0) / 100:,.2f}"


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


def send_email(settings: Settings, to: str, subject: str, body: str) -> None:
    if not to:
        return
    message = EmailMessage()
    message["From"] = settings.mail_from
    message["To"] = to
    message["Subject"] = subject
    message["Message-ID"] = make_msgid(domain="poke6s.com")
    message.set_content(body)
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


def _address(order: dict) -> str:
    ship = order.get("shipping") or {}
    addr = ship.get("address") or {}
    parts = [ship.get("name") or order.get("name") or "", addr.get("line1") or "", addr.get("line2") or "",
             " ".join(p for p in [f"{addr.get('city') or ''},", addr.get("state") or "", addr.get("postal_code") or ""]
                      if p.strip(",")).strip()]
    return "\n".join(f"  {p}" for p in parts if p)


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
    send_email(settings, order["email"], f"{test}Poke6s order {order['number']} confirmed", body)


def shipped_email(settings: Settings, order: dict, status_url: str) -> None:
    test = "[TEST] " if order["mode"] == "test" else ""
    tracking = f"{order['carrier'] or 'Tracking'}: {order['tracking_number']}" if order["tracking_number"] else ""
    body = f"""Good news: your Poke6s order {order['number']} has shipped.

{tracking}

{_lines(order)}

Shipping to:
{_address(order)}

Order status: {status_url}

Questions? Reply to this email or write to {settings.contact_email}.

Poke6s
"""
    send_email(settings, order["email"], f"{test}Poke6s order {order['number']} has shipped", body)
