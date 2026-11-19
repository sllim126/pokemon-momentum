"""Shipping through Pirate Ship (Phase 3, step 3).

The owner buys labels on pirateship.com. This module covers both directions:

1. **Out:** a CSV of paid, unshipped orders for Pirate Ship's spreadsheet import
   (https://www.pirateship.com/integrations/spreadsheets). Pirate Ship takes any column headers
   and asks once which column is which; it needs Name, Address, City, State and Zipcode, and also
   takes Company, Address Line 2, Country, Order ID, package weight and size, and "Rubber Stamp"
   text printed on the label. The file uses Pirate Ship's own field names so the mapping is
   obvious.
   - The weight is the sum of each item's packed weight (the product's weight_oz, set on its
     product page). The box size is filled in only for a single-item order, since two items may
     share a bigger box. Anything unknown is left blank for the owner to type in Pirate Ship.
   - No email column on purpose: the store sends its own tracking email when the order is marked
     shipped, so Pirate Ship shouldn't send a second one.
   - Signature confirmation (every order, plan "Shipping") can't be set in the file. The owner
     ticks it for the batch in Pirate Ship; the Shipping page says so next to the button.
2. **Back:** Pirate Ship's shipment export (a CSV with tracking numbers). Its exact columns
   aren't documented, so the reader is forgiving: the order number is found by its pattern
   (P6-2026-0006) in any cell, the tracking number in the column whose header says "tracking",
   and the carrier from a "carrier" column or the tracking number's shape (1Z... is UPS). Rows
   without one of our order numbers (labels bought for TCGplayer or eBay sales) are only counted.
   Nothing is changed until the owner confirms the preview; each confirmed order is marked shipped
   through orders.mark_shipped, which emails the customer, same as the order page's button.

Read-only helpers take any connection; functions that write take the admin's writer connection.
"""

from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
from datetime import datetime

from ..app import notify
from ..app.orders import now_iso
from ..app.product_changes import clean_reason
from ..app.product_details import _log
from .orders_view import CARRIERS

# Order numbers as the store makes them (P6-2026-0001, test mode P6T-2026-0001).
ORDER_NUMBER = re.compile(r"^P6T?-\d{4}-\d{4,6}$")
ORDER_NUMBER_IN_TEXT = re.compile(r"\bP6T?-\d{4}-\d{4,6}\b")
# Tracking numbers: letters and digits only once spaces are taken out (USPS 20-22 digits, UPS
# 1Z..., FedEx 12-15, DHL 10). Loose on purpose; this only catches paste mistakes.
TRACKING = re.compile(r"^[A-Za-z0-9]{8,40}$")

EXPORT_COLUMNS = ["Order ID", "Name", "Company", "Address", "Address Line 2", "City", "State", "Zipcode",
                  "Country", "Weight (oz)", "Length (in)", "Width (in)", "Height (in)", "Rubber Stamp 1"]
STAMP_LENGTH = 60           # keep the label's corner text short
MAX_UPLOAD_BYTES = 2 * 1024 * 1024
MAX_UPLOAD_ROWS = 5000
# Packed sizes the product page accepts: up to 100 lb, and USPS's 108-inch length limit.
MAX_WEIGHT_OZ = 1600
MAX_SIDE_IN = 108


# --- packed size (product page) ----------------------------------------------------------------

def _number(text: str, *, what: str, top: float) -> float | None:
    text = text.strip().lower()
    if not text:
        return None
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(lbs?|oz)?", text)
    if not match:
        raise ValueError(f"The {what} should be a number, like 12 or 7.5")
    value = float(match.group(1)) * (16 if (match.group(2) or "").startswith("lb") else 1)
    if not 0 < value <= top:
        raise ValueError(f"The {what} should be more than 0 and at most {top:g}")
    return round(value, 2)


def describe_size(weight, length, width, height) -> str:
    """'24 oz, 11 x 7.5 x 5.5 in' (or the parts that are known); '' when nothing is."""
    parts = []
    if weight:
        parts.append(f"{weight:g} oz")
    if length and width and height:
        parts.append(f"{length:g} x {width:g} x {height:g} in")
    return ", ".join(parts)


def set_packed_size(conn: sqlite3.Connection, product_id: int, weight: str, length: str, width: str,
                    height: str, reason: str = "", now: datetime | None = None) -> bool:
    """Save what one unit weighs packed and the box it ships in. Weight in ounces ("1.5 lb" is
    also understood); the box is all three sides or none. Returns False if nothing changed."""
    new = {"weight_oz": _number(weight, what="weight", top=MAX_WEIGHT_OZ),
           "length_in": _number(length, what="length", top=MAX_SIDE_IN),
           "width_in": _number(width, what="width", top=MAX_SIDE_IN),
           "height_in": _number(height, what="height", top=MAX_SIDE_IN)}
    sides = [new["length_in"], new["width_in"], new["height_in"]]
    if any(sides) and not all(sides):
        raise ValueError("Type all three box sides (or leave all three empty)")
    at = now_iso(now)
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
        if row is None:
            raise ValueError("That product doesn't exist")
        if all(row[key] == value for key, value in new.items()):
            conn.rollback()
            return False
        conn.execute("UPDATE products SET weight_oz = ?, length_in = ?, width_in = ?, height_in = ?, updated_at = ? "
                     "WHERE id = ?", (*new.values(), at, product_id))
        old_text = describe_size(row["weight_oz"], row["length_in"], row["width_in"], row["height_in"])
        _log(conn, product_id, "packed_size", old_text or None, describe_size(*new.values()) or None,
             clean_reason(reason), at)
        conn.commit()
        return True
    except BaseException:
        conn.rollback()
        raise


# --- out: unshipped orders and the Pirate Ship file ----------------------------------------------

def unshipped(conn: sqlite3.Connection) -> list[dict]:
    """Paid, unshipped orders, oldest first (the packing order), with what the export would fill
    in and what it couldn't (`missing`)."""
    orders = [dict(row) for row in conn.execute(
        "SELECT id, number, mode, name, shipping_json, paid_at, labels_exported_at FROM orders "
        "WHERE status = 'paid' ORDER BY paid_at, id")]
    for order in orders:
        order["shipping"] = _json(order.pop("shipping_json"))
        order["address"] = notify.address_lines(order)
        items = [dict(row) for row in conn.execute(
            "SELECT oi.sku, oi.title, oi.quantity, oi.product_id, p.weight_oz, p.length_in, p.width_in, p.height_in "
            "FROM order_items oi LEFT JOIN products p ON p.id = oi.product_id WHERE oi.order_id = ? ORDER BY oi.id",
            (order["id"],))]
        order["items"] = items
        units = sum(item["quantity"] for item in items)
        order["weight_oz"] = (round(sum(item["weight_oz"] * item["quantity"] for item in items), 2)
                              if items and all(item["weight_oz"] for item in items) else None)
        only = items[0] if units == 1 else None
        order["box"] = ((only["length_in"], only["width_in"], only["height_in"])
                        if only and only["length_in"] and only["width_in"] and only["height_in"] else None)
        order["missing"] = [item for item in items if not item["weight_oz"]
                            or (units == 1 and not order["box"])]
        order["stamp"] = ", ".join(f"{item['quantity']}x {item['sku']}" for item in items)[:STAMP_LENGTH]
    return orders


def missing_sizes(orders: list[dict]) -> list[dict]:
    """Products in these orders with no packed weight (or, for a one-item order, no box), once each."""
    seen, result = set(), []
    for order in orders:
        for item in order["missing"]:
            if item["product_id"] and item["product_id"] not in seen:
                seen.add(item["product_id"])
                result.append(item)
    return result


def _json(text: str | None) -> dict:
    return (json.loads(text) if text else None) or {}


def _cell(value) -> str:
    """One CSV cell. Leading = + @ and control characters are dropped so a spreadsheet opening the
    file never treats a customer-typed address as a formula (Pirate Ship itself doesn't need them)."""
    text = " ".join(str(value if value is not None else "").split())
    return text.lstrip("=+@\t\r")


def export_rows(orders: list[dict]) -> list[list[str]]:
    rows = []
    for order in orders:
        address = order["shipping"].get("address") or {}
        box = order["box"] or ("", "", "")
        rows.append([_cell(v) for v in (
            order["number"], order["shipping"].get("name") or order["name"], "", address.get("line1"),
            address.get("line2"), address.get("city"), address.get("state"), address.get("postal_code"),
            address.get("country") or "US", f"{order['weight_oz']:g}" if order["weight_oz"] else "",
            *(f"{side:g}" if side else "" for side in box), order["stamp"])])
    return rows


def export_csv(orders: list[dict]) -> str:
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\r\n")
    writer.writerow(EXPORT_COLUMNS)
    writer.writerows(export_rows(orders))
    return out.getvalue()


def mark_exported(conn: sqlite3.Connection, numbers: list[str], now: datetime | None = None) -> None:
    at = now_iso(now)
    conn.execute("BEGIN IMMEDIATE")
    conn.executemany("UPDATE orders SET labels_exported_at = ? WHERE number = ? AND status = 'paid'",
                     [(at, number) for number in numbers])
    conn.commit()


# --- back: Pirate Ship's tracking export ---------------------------------------------------------

def _decode(data: bytes) -> str:
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return data.decode("latin-1")


def carrier_for(text: str, tracking: str) -> str:
    """The carrier as the store names it: from the file's carrier column when it says, otherwise
    from the tracking number (UPS numbers start 1Z; Pirate Ship sells USPS and UPS only)."""
    text = text.lower()
    for name in ("USPS", "UPS", "FedEx", "DHL"):
        if name.lower() in text:
            return name
    return "UPS" if tracking.upper().startswith("1Z") else "USPS"


def read_tracking(data: bytes) -> dict:
    """Read Pirate Ship's export. Returns {"rows": [{number, tracking, carrier, line}],
    "other": count of rows with none of our order numbers, "problems": [(line, text)]}.
    Raises ValueError when the file can't be used at all."""
    if len(data) > MAX_UPLOAD_BYTES:
        raise ValueError("That file is bigger than 2 MB. Export only the recent shipments from Pirate Ship.")
    if data[:4] == b"PK\x03\x04":
        raise ValueError("That's an Excel file. Export the shipments from Pirate Ship as CSV instead.")
    lines = list(csv.reader(io.StringIO(_decode(data))))
    if not lines:
        raise ValueError("The file is empty.")
    header = [h.strip().lower() for h in lines[0]]
    tracking_col = next((i for i, h in enumerate(header) if "tracking" in h
                         and not any(word in h for word in ("url", "link"))), None)
    if tracking_col is None:
        raise ValueError("No column called “Tracking” in that file. Is it Pirate Ship's shipment export?")
    carrier_col = next((i for i, h in enumerate(header) if "carrier" in h), None)
    status_col = next((i for i, h in enumerate(header) if h == "status" or h.endswith(" status")), None)
    rows, problems, other = [], [], 0
    for line, cells in enumerate(lines[1:MAX_UPLOAD_ROWS + 1], start=2):
        if not any(c.strip() for c in cells):
            continue
        numbers = sorted({n for c in cells for n in ORDER_NUMBER_IN_TEXT.findall(c)})
        if not numbers:
            other += 1
            continue
        status = cells[status_col].strip().lower() if status_col is not None and status_col < len(cells) else ""
        if any(word in status for word in ("void", "refund", "cancel")):
            problems.append((line, f"{', '.join(numbers)}: the label was {status}, skipped"))
            continue
        raw = cells[tracking_col] if tracking_col < len(cells) else ""
        tracking = "".join(raw.split())
        if "e+" in tracking.lower():
            problems.append((line, f"{', '.join(numbers)}: the tracking number reads {raw!r}. A spreadsheet "
                                   "turned it into a number; download the CSV again and upload it unopened"))
            continue
        if not TRACKING.match(tracking):
            problems.append((line, f"{', '.join(numbers)}: no usable tracking number"))
            continue
        if len(numbers) > 1:
            problems.append((line, f"one label names several orders ({', '.join(numbers)}): mark them on their pages"))
            continue
        carrier = carrier_for(cells[carrier_col] if carrier_col is not None and carrier_col < len(cells) else "",
                              tracking)
        rows.append({"number": numbers[0], "tracking": tracking.upper(), "carrier": carrier, "line": line})
    if len(lines) > MAX_UPLOAD_ROWS + 1:
        problems.append((MAX_UPLOAD_ROWS + 2, f"only the first {MAX_UPLOAD_ROWS} rows were read"))
    return {"rows": rows, "other": other, "problems": problems}


def check(conn: sqlite3.Connection, rows: list[dict]) -> list[dict]:
    """What each matched row would do. `ready` rows can be marked shipped; the rest say why not.
    An order with two different tracking numbers in the file (a reprinted label, or two boxes)
    is not shipped from here: the owner picks on the order's page."""
    by_order: dict[str, list[dict]] = {}
    for row in rows:
        by_order.setdefault(row["number"], [])
        if row["tracking"] not in [r["tracking"] for r in by_order[row["number"]]]:
            by_order[row["number"]].append(row)
    result = []
    for number, found in by_order.items():
        order = conn.execute("SELECT number, name, status, mode, tracking_number FROM orders WHERE number = ?",
                             (number,)).fetchone()
        entry = {**found[0], "name": order["name"] if order else "", "mode": order["mode"] if order else "",
                 "ready": False}
        if order is None:
            entry["why"] = "no such order in the store"
        elif len(found) > 1:
            entry["why"] = ("two labels in the file (" + ", ".join(r["tracking"] for r in found)
                            + "): mark it shipped on the order's page with the right one")
        elif order["status"] == "paid":
            entry["ready"] = True
        elif order["status"] == "shipped" and (order["tracking_number"] or "").upper() == found[0]["tracking"]:
            entry["why"] = "already marked shipped with this tracking number"
        elif order["status"] == "shipped":
            entry["why"] = f"already marked shipped with {order['tracking_number']}"
        else:
            entry["why"] = f"the order is {order['status'].replace('_', ' ')}"
        result.append(entry)
    result.sort(key=lambda e: (not e["ready"], e["number"]))
    return result


def pick_key(entry: dict) -> str:
    return f"{entry['number']}|{entry['carrier']}|{entry['tracking']}"


def parse_pick(value: str) -> tuple[str, str, str] | None:
    """A ticked line from the preview, checked again (it came back from the browser)."""
    parts = value.split("|")
    if len(parts) != 3:
        return None
    number, carrier, tracking = parts
    if not ORDER_NUMBER.match(number) or carrier not in CARRIERS or not TRACKING.match(tracking):
        return None
    return number, carrier, tracking
