"""Packing slips: one US Letter page per order, as a PDF, to print and put in the box.

Layout (owner, 2026-11-18): logo and "Packing Slip" with the order number and date; Ship to,
From and Order details side by side; the items with a "Packed" tick box; the totals; a short
thank-you note with the contact email; the returns line. 1-inch margins, so it prints at
"Actual size" on any printer. `prices=False` makes a gift slip: items and quantities only.

The store's logo is blue on navy. Printed, that would be a dark block, so the navy is made
transparent here (once per process) and the blue prints on white paper.
"""

from __future__ import annotations

import io
import statistics
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

from PIL import Image
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import Image as PdfImage
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from reportlab.platypus.flowables import Flowable
from xml.sax.saxutils import escape

from ..app import notify
from ..app.settings import Settings

LOGO = Path(__file__).resolve().parent.parent / "app" / "static" / "img" / "logo.png"
NAVY = colors.HexColor("#2a3550")
GREY = colors.HexColor("#5b6475")
RULE = colors.HexColor("#c9cfda")
NOTE_BG = colors.HexColor("#eef1f6")
BUSINESS = "Poke6s LLC"


@lru_cache(maxsize=1)
def print_logo() -> tuple[bytes, int, int]:
    """The logo as PNG bytes with the navy background made transparent, and its size."""
    image = Image.open(LOGO).convert("RGB")
    pixels = image.load()
    width, height = image.size
    background = pixels[2, 2]
    bright = [pixels[x, y] for x in range(0, width, 2) for y in range(0, height, 2) if pixels[x, y][2] > 180]
    blue = tuple(int(statistics.median(p[i] for p in bright)) for i in range(3)) if bright else (0, 171, 248)
    out = Image.new("RGBA", (width, height))
    target = out.load()
    span = max(1, blue[2] - background[2])
    for x in range(width):
        for y in range(height):
            # How far the pixel is from navy towards blue = how opaque it is (keeps smooth edges).
            alpha = max(0.0, min(1.0, (pixels[x, y][2] - background[2]) / span))
            target[x, y] = (*blue, int(alpha * 255))
    buffer = io.BytesIO()
    out.save(buffer, "PNG")
    return buffer.getvalue(), width, height


class _TickBox(Flowable):
    """An empty square to tick by hand when the item is in the box."""

    def wrap(self, *_args):
        return (11, 11)

    def draw(self):
        self.canv.setStrokeColor(colors.black)
        self.canv.setLineWidth(0.8)
        self.canv.rect(0, 0, 11, 11)


def _style(name: str, **kw) -> ParagraphStyle:
    return ParagraphStyle(name, fontName=kw.pop("font", "Helvetica"), fontSize=kw.pop("size", 10),
                          leading=kw.pop("lead", 14), textColor=kw.pop("color", colors.black), **kw)


def _date(iso: str | None) -> str:
    """'2026-11-18T07:54:52Z' -> 'November 18, 2026' (the shop's local date, Utah)."""
    if not iso:
        return ""
    moment = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).astimezone(notify.MOUNTAIN)
    return f"{moment:%B} {moment.day}, {moment.year}"


def _tidy_city(order: dict) -> dict:
    """'garland' -> 'Garland' when the customer typed the city all lower-case (print only;
    the order itself keeps what was entered)."""
    shipping = order.get("shipping") or {}
    address = shipping.get("address") or {}
    city = address.get("city") or ""
    if not city.islower():
        return order
    return {**order, "shipping": {**shipping, "address": {**address, "city": city.title()}}}


def build(order: dict, settings: Settings, prices: bool = True) -> bytes:
    """The packing slip for one order (from orders.get_order), as PDF bytes."""
    money = notify.money
    body = _style("body")
    small = _style("small", size=8.5, lead=12, color=GREY)
    label = _style("label", font="Helvetica-Bold", size=8, lead=11, color=GREY)
    label_right = _style("label_r", font="Helvetica-Bold", size=8, lead=11, color=GREY, alignment=2)
    label_centre = _style("label_c", font="Helvetica-Bold", size=8, lead=11, color=GREY, alignment=1)
    title = _style("title", font="Helvetica-Bold", size=20, lead=24, color=NAVY, alignment=2)
    meta = _style("meta", alignment=2)
    right = _style("right", alignment=2)
    bold_right = _style("bold_right", font="Helvetica-Bold", alignment=2)

    buffer = io.BytesIO()
    number = escape(order["number"])
    doc = SimpleDocTemplate(buffer, pagesize=letter, leftMargin=inch, rightMargin=inch, topMargin=inch,
                            bottomMargin=inch, title=f"Packing slip {order['number']}", author="Poke6s")
    width = letter[0] - 2 * inch
    placed = _date(order.get("paid_at") or order.get("created_at"))

    png, logo_w, logo_h = print_logo()
    logo_width = 2.0 * inch
    logo = PdfImage(io.BytesIO(png), width=logo_width, height=logo_width * logo_h / logo_w)
    head = Table([[logo, [Paragraph("Packing Slip" if prices else "Gift Packing Slip", title), Spacer(1, 4),
                          Paragraph(f"Order <b>{number}</b>", meta), Paragraph(placed, meta)]]],
                 colWidths=[width * 0.5, width * 0.5])
    head.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("LEFTPADDING", (0, 0), (-1, -1), 0),
                              ("RIGHTPADDING", (0, 0), (-1, -1), 0), ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
                              ("LINEBELOW", (0, 0), (-1, 0), 1.5, NAVY)]))
    story: list = [head, Spacer(1, 18)]

    lines = [escape(line) for line in notify.address_lines(_tidy_city(order))] or ["(no address on the order)"]
    sender = "<br/>".join(escape(x) for x in [BUSINESS, settings.base_url.split("//")[-1], settings.contact_email] if x)
    details = f"Order: {number}<br/>Placed: {escape(placed)}" + (
        f"<br/>Items: {sum(i['quantity'] for i in order['items'])}" if not prices else "")
    info = Table([[Paragraph("SHIP TO", label), Paragraph("FROM", label), Paragraph("ORDER DETAILS", label)],
                  [Paragraph("<br/>".join(lines), body), Paragraph(sender, body), Paragraph(details, body)]],
                 colWidths=[width * 0.38, width * 0.30, width * 0.32])
    info.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 0),
                              ("BOTTOMPADDING", (0, 0), (-1, 0), 3)]))
    story += [info, Spacer(1, 22)]

    if prices:
        widths = [0.50, 0.08, 0.15, 0.15, 0.12]
        rows = [[Paragraph("ITEM", label), Paragraph("QTY", label_right), Paragraph("PRICE", label_right),
                 Paragraph("AMOUNT", label_right), Paragraph("PACKED", label_centre)]]
        for item in order["items"]:
            rows.append([Paragraph(escape(item["title"]), body), Paragraph(str(item["quantity"]), right),
                         Paragraph(money(item["unit_price_cents"]), right),
                         Paragraph(money(item["unit_price_cents"] * item["quantity"]), right), _TickBox()])
    else:
        widths = [0.76, 0.12, 0.12]
        rows = [[Paragraph("ITEM", label), Paragraph("QTY", label_right), Paragraph("PACKED", label_centre)]]
        for item in order["items"]:
            rows.append([Paragraph(escape(item["title"]), body), Paragraph(str(item["quantity"]), right), _TickBox()])
    item_rows = len(rows)
    style = [("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("LEFTPADDING", (0, 0), (-1, -1), 4),
             ("RIGHTPADDING", (0, 0), (-1, -1), 4), ("LINEBELOW", (0, 0), (-1, 0), 1, NAVY),
             ("TOPPADDING", (0, 1), (-1, item_rows - 1), 8), ("BOTTOMPADDING", (0, 1), (-1, item_rows - 1), 8),
             ("LINEBELOW", (0, 1), (-1, item_rows - 1), 0.5, RULE),
             ("ALIGN", (-1, 1), (-1, item_rows - 1), "CENTER")]
    if prices:
        totals = [("Subtotal", order["subtotal_cents"]), ("Shipping", order["shipping_cents"])]
        if order.get("discount_cents"):
            totals.append(("Discount", -order["discount_cents"]))
        totals += [("Sales tax", order["tax_cents"]), ("Total paid", order["total_cents"])]
        for name, cents in totals:
            strong = name == "Total paid"
            rows.append(["", "", Paragraph(name, bold_right if strong else right),
                         Paragraph(money(cents), bold_right if strong else right), ""])
        style.append(("LINEABOVE", (2, len(rows) - 1), (3, len(rows) - 1), 1, colors.black))
    table = Table(rows, colWidths=[width * w for w in widths], repeatRows=1)
    table.setStyle(TableStyle(style))
    story += [table, Spacer(1, 28)]

    contact = escape(settings.contact_email)
    note = Table([[Paragraph(
        "<b>Thank you for your order!</b> Please check that the factory shrink wrap is intact when your "
        f"package arrives. If anything is wrong, email <b>{contact}</b> with your order number and we "
        "will make it right.", body)]], colWidths=[width])
    note.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), NOTE_BG), ("BOX", (0, 0), (-1, -1), 0.5, RULE),
                              ("LEFTPADDING", (0, 0), (-1, -1), 12), ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                              ("TOPPADDING", (0, 0), (-1, -1), 10), ("BOTTOMPADDING", (0, 0), (-1, -1), 10)]))
    store = escape(settings.base_url.split("//")[-1])
    market = escape(settings.market_base_url.split("//")[-1])
    story += [note, Spacer(1, 14),
              Paragraph("<b>Returns:</b> unopened items in original wrap within 14 days of delivery. "
                        f"Details at {store}/returns.", small),
              Paragraph(f"Track sealed and card prices free at {market}.", small)]
    doc.build(story)
    return buffer.getvalue()
