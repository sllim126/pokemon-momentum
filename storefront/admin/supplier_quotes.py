"""The admin's Supplier Pricing page (`/supplier-pricing`, templates/supplier_pricing.html).

Moved from market.poke6s.com on 2026-11-22 (docs/market_inventory.md, option B, the owner
tools); the market address now forwards here. The owner turns a supplier's price list (a
Messenger screenshot of Japanese boxes priced in yen) into rows of the quote ledger, which
Supplier Profitability (supplier_profit.py) and the nightly price targets
(scripts/build_store_price_targets.py, its supplier-cost floor) read.

The flow is three plain form posts, no script, like the rest of the admin:
1. "Read the text": the price list's text (pasted, or read from the screenshot) is split into
   rows of item name and yen price (`parse_text`, the market page's rule, unchanged).
2. "Check again": the owner fixes names, prices and SKUs in the table; each row is matched
   (`check`): a typed SKU wins, otherwise the name is looked up in the name-to-SKU list, and the
   store's and the market's prices are shown beside it.
3. "Save": the rows are appended to the ledger under one quote_id (`save`). SKUs the owner typed
   for names the list didn't know are added to the list too, unless they untick that.

Files, both under data/storefront/ (the only data folder the admin can write; moved there from
data/ on 2026-11-22). data/storefront/ is git-ignored, so neither is committed. That's because
they're live data, not because they're secret: suppliers post these prices publicly (owner,
2026-11-22), and the old copies up to May 2026 stay in the repo's history.
- supplier_quotes.csv: the ledger, append-only, one row per item per quote (QUOTE_FIELDS).
- supplier_name_mapping.csv: supplier item name -> SKU (MAPPING_FIELDS), matched on
  `normalize_name`, so case, spacing and punctuation don't matter.
"""

from __future__ import annotations

import csv
import re
import threading
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path

QUOTE_FIELDS = ["quote_id", "quote_date", "supplier_name", "source_name", "source_type",
                "item_name_raw", "sku", "cost_jpy", "image_name", "notes"]
MAPPING_FIELDS = ["supplier_name_raw", "sku", "note"]
DEFAULT_SUPPLIER = "Facebook Supplier"   # the market page's default
MAX_TEXT = 300_000                        # a whole copied shop page is about 25,000 characters
MAX_ROWS = 200                            # one price list; the form refuses more

# One price-list line: the item name, then a yen price at the end ("Mega Dream ¥12,800",
# "Nihil Zero 9800"). Ported from the market page's parseSupplierText.
_LINE = re.compile(r"^(.*?)(?:\s+|^)(?:¥|Y|JPY)?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{3,6})$", re.IGNORECASE)

# Saves append to two files; one admin process, so a lock is enough to keep two saves apart.
_write_lock = threading.Lock()


def normalize_name(value: str) -> str:
    """The lookup key for a supplier item name: lowercase letters and digits, single spaces."""
    return " ".join(re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).split())


def parse_text(text: str) -> list[dict]:
    """Rows of {item, cost, sku, note} from a price list's text: a whole supplier shop page when it
    looks like one (parse_shop_page), otherwise one item per line with its price at the end."""
    return parse_shop_page(text) or parse_lines(text)


def parse_lines(text: str) -> list[dict]:
    """Rows from a one-item-per-line list. Lines without a price at the end are skipped."""
    rows = []
    for line in str(text or "").splitlines():
        line = " ".join(line.replace("|", " ").split())
        match = _LINE.match(line)
        if not match:
            continue
        item = re.sub(r"^[\W_]+", "", match.group(1).strip())
        if item:
            rows.append({"item": item, "cost": match.group(2).replace(",", ""), "sku": "", "note": ""})
    return rows[:MAX_ROWS]


# --- a whole shop page, copied from the browser (Ctrl+A, Ctrl+C) -----------------------------
#
# Written for MACHO TCG (shop.alc-japan.co.jp, owner 2026-11-22) but keyed on the shape, not the
# site: each product is a few heading lines (badge, English name, Japanese name, set code and
# "JAN <barcode>", in that order, some missing), then one block per version:
#
#     SEALED (SHRINK)          <- the version's label
#     Est. ship Nov 30         <- pre-orders only, maybe several dates each with a price
#     ¥18,300                  <- the price, alone on its line
#     ≈ 115.35 USD / 12 in stock / Ask us / − / +   <- ignored
#
# A label is any line followed by a lone yen price (directly or after an "Est. ship" line); a
# product starts at the first label after new heading lines. The row's price is the sealed box
# (the first of BOX_LABELS the product has); the other versions, stock and barcode go in the note.

_PRICE_LINE = re.compile(r"^[¥￥]\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)$")
_SHIP_LINE = re.compile(r"^est\.?\s*ship\b", re.IGNORECASE)
_JAN = re.compile(r"JAN\s*(\d{8,14})")
_SET_CODE = re.compile(r"^[A-Za-z]{1,5}-?[A-Za-z]?\d{1,3}[A-Za-z]?$")  # M7, M2a, SV11B, ALC-G85
_CJK = re.compile(r"[぀-ヿ㐀-鿿ｦ-ﾟ]")
_IN_STOCK = re.compile(r"^(\d[\d,]*) in stock$", re.IGNORECASE)
_NOISE = re.compile(r"^(?:≈.*|[−+\-–—]|ask us|notify me|whatsapp|sold in units of \d+|on request\b.*|"
                    r"\d[\d,]* in stock|out of stock|sold out)$", re.IGNORECASE)
_NOT_A_VERSION = re.compile(r"total|subtotal|shipping|discount|coupon|save\b|min\.? order", re.IGNORECASE)
BADGES = {"hot", "new", "sale", "preorder", "pre-order", "low stock", "sold out", "restock", "limited",
          "popular", "best seller", "few left"}
# Which version is "the box", in order of preference (labels compared in lower case).
BOX_LABELS = ("sealed (shrink)", "sealed", "box", "shrink", "no shrink", "no shrink / no pull tab")
NOT_BOX = ("carton", "loose", "unsearched", "unified", "pack")
MAX_NOTE = 300
# Shops whose copied page names them: the supplier field is filled in when it still says
# DEFAULT_SUPPLIER (line on the page -> supplier name saved with the quote).
KNOWN_SHOPS = {"macho tcg": "MACHO TCG (ALC Japan)"}


def shop_name(text: str) -> str:
    """The supplier's name when the pasted page is a shop listed in KNOWN_SHOPS, else ''."""
    lines = {" ".join(line.split()).lower() for line in str(text or "").splitlines()}
    return next((name for line, name in KNOWN_SHOPS.items() if line in lines), "")


def _yen(line: str) -> int | None:
    match = _PRICE_LINE.match(line)
    return int(match.group(1).replace(",", "")) if match else None


def _label_price_at(lines: list[str], i: int) -> bool:
    """Whether lines[i] is a version label: a lone yen price follows, maybe after a ship date."""
    line = lines[i]
    if _yen(line) is not None or _SHIP_LINE.match(line) or _NOISE.match(line) or _NOT_A_VERSION.search(line):
        return False
    nxt = lines[i + 1] if i + 1 < len(lines) else ""
    if _yen(nxt) is not None:
        return True
    return bool(_SHIP_LINE.match(nxt)) and i + 2 < len(lines) and _yen(lines[i + 2]) is not None


def _heading(lines: list[str]) -> dict | None:
    """Name, Japanese name, set code and barcode from a product's heading lines (read from the
    bottom, so page text above the first product is ignored). None when there's no name."""
    found = {"name": "", "jp": "", "set": "", "jan": ""}
    for line in reversed(lines):
        jan = _JAN.search(line)
        if jan:
            found["jan"] = jan.group(1)
            found["set"] = line[:jan.start()].strip(" ·|") or found["set"]
        elif _SET_CODE.match(line) and not found["set"] and line.lower() not in BADGES:
            found["set"] = line
        elif _CJK.search(line):
            found["jp"] = found["jp"] or line
        elif line.lower() in BADGES:
            continue
        elif re.search(r"[A-Za-z0-9]", line):   # "151" is a set's whole English name
            found["name"] = line
            return found
    # No English name: still a new product (so its prices never join the one above), named by
    # its Japanese name or set code; None only when the lines held nothing at all.
    found["name"] = found["jp"] or found["set"]
    return found if found["name"] else None


def parse_shop_page(text: str) -> list[dict]:
    """Rows from a copied supplier shop page ([] when the text isn't one). See the notes above."""
    lines = [" ".join(line.split()) for line in str(text or "").splitlines()]
    lines = [line for line in lines if line]
    products: list[dict] = []
    heading: list[str] = []
    current: dict | None = None
    i = 0
    while i < len(lines):
        if not _label_price_at(lines, i):
            if not _NOISE.match(lines[i]) and _yen(lines[i]) is None and not _SHIP_LINE.match(lines[i]):
                heading.append(lines[i])
            i += 1
            continue
        if heading:
            found = _heading(heading)
            heading = []
            if found:
                current = {**found, "versions": []}
                products.append(current)
        label, i = lines[i], i + 1
        prices: list[tuple[int, str]] = []
        stock, ship = "", ""
        # The version's block: ship dates, prices and noise, up to the next label or heading line.
        while i < len(lines) and not _label_price_at(lines, i):
            line = lines[i]
            if _SHIP_LINE.match(line):
                ship = line
            elif _yen(line) is not None:
                prices.append((_yen(line), ship))
            elif _NOISE.match(line):
                stocked = _IN_STOCK.match(line)
                if stocked or line.lower() == "ask us":
                    stock = stock or line.lower()
            else:
                break
            i += 1
        if current is not None and prices:
            current["versions"].append({"label": label, "yen": prices[0][0], "ship": prices[0][1], "stock": stock})
    return _shop_rows(products)


def _shop_rows(products: list[dict]) -> list[dict]:
    rows, seen = [], set()
    for product in products:
        key = normalize_name(product["name"])
        versions = product["versions"]
        if not versions or key in seen:
            continue   # a product with no prices, or listed twice (featured pre-orders sit above the list)
        by_label = {v["label"].lower(): v for v in versions}
        box = next((by_label[label] for label in BOX_LABELS if label in by_label), None)
        box = box or next((v for v in versions if not any(word in v["label"].lower() for word in NOT_BOX)), None)
        if box is None:
            continue   # packs or cartons only: not a box price
        seen.add(key)
        note = [" ".join(part for part in (product["set"], f"JAN {product['jan']}" if product["jan"] else "") if part)]
        state = box["ship"].lower() if box["ship"] else box["stock"]
        note.append(f"{box['label'].lower()}" + (f", {state}" if state else ""))
        others = [f"{v['label'].lower()} ¥{v['yen']:,}" for v in versions if v is not box]
        if others:
            note.append("also " + ", ".join(others))
        rows.append({"item": product["name"], "cost": str(box["yen"]), "sku": "",
                     "note": "; ".join(part for part in note if part)[:MAX_NOTE]})
    return rows[:MAX_ROWS]


def load_mapping(path: Path) -> dict[str, str]:
    """{normalized supplier name: SKU}; {} when the file isn't there yet."""
    if not path.is_file():
        return {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return {normalize_name(row.get("supplier_name_raw") or ""): str(row.get("sku") or "").strip().upper()
                for row in csv.DictReader(handle)
                if normalize_name(row.get("supplier_name_raw") or "") and str(row.get("sku") or "").strip()}


def clean_cost(value: str) -> str | None:
    """A yen price as whole digits ("12,800" -> "12800"), or None when it isn't one above zero."""
    text = str(value or "").replace(",", "").replace("¥", "").strip()
    return text if text.isdigit() and int(text) > 0 else None


def form_rows(items: Sequence[str], costs: Sequence[str], skus: Sequence[str],
              notes: Sequence[str] = ()) -> list[dict]:
    """The review table's rows back from the form; a row with no name and no price is dropped
    (that's how the owner removes one, and how the spare blank row stays out). `notes` are the
    rows' hidden notes (a shop page's other versions and stock), carried through to the save."""
    rows = []
    for index, item in enumerate(items[:MAX_ROWS]):
        cost = costs[index] if index < len(costs) else ""
        sku = skus[index] if index < len(skus) else ""
        note = notes[index] if index < len(notes) else ""
        if str(item).strip() or str(cost).strip():
            rows.append({"item": str(item).strip(), "cost": str(cost).strip(), "sku": str(sku).strip().upper(),
                         "note": " ".join(str(note).split())[:MAX_NOTE]})
    return rows


def check(rows: list[dict], mapping: Mapping[str, str], store: Mapping[str, dict],
          market: Mapping[str, dict]) -> list[dict]:
    """Each row with its SKU match, the store's and market's prices, and a status:
    "ok", "no_sku" (saved, but Supplier Profitability can't use it until it has one),
    "new_sku" (a SKU neither the store nor the market list knows; saved as typed),
    or "bad" (no name, or the price isn't a whole number of yen: can't be saved)."""
    out = []
    for row in rows:
        typed = row.get("sku", "")
        mapped = mapping.get(normalize_name(row["item"]), "")
        sku = typed or mapped
        cost = clean_cost(row["cost"])
        store_row, market_row = store.get(sku, {}), market.get(sku, {})
        if not row["item"] or cost is None:
            status = "bad"
        elif not sku:
            status = "no_sku"
        elif not store_row and not market_row:
            status = "new_sku"
        else:
            status = "ok"
        out.append({**row, "cost": cost or row["cost"], "sku": sku, "status": status,
                    "from_list": bool(mapped) and (not typed or typed == mapped),
                    "store_title": store_row.get("title") or market_row.get("title") or "",
                    "store_price": store_row.get("price"),
                    "market_price": market_row.get("market_price") or None,
                    "target_price": market_row.get("target_price") or None})
    return out


def summary(rows: list[dict]) -> dict:
    return {"rows": len(rows), "matched": sum(1 for r in rows if r["sku"]),
            "with_market": sum(1 for r in rows if r["market_price"]),
            "review": sum(1 for r in rows if r["status"] != "ok")}


def valid_date(value: str) -> str | None:
    try:
        return date.fromisoformat(str(value or "").strip()).isoformat()
    except ValueError:
        return None


def save(quotes_path: Path, mapping_path: Path, checked: list[dict], *, supplier: str, quote_date: str,
         source: str, remember: bool, now: datetime | None = None) -> dict:
    """Append the checked rows to the ledger as one quote, and (if `remember`) add the typed
    name -> SKU matches the list didn't have. The caller has already refused "bad" rows.
    Returns {"quote_id", "saved", "matched", "remembered"}."""
    quote_id = (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    source_type = ("shop page" if any(row.get("note") for row in checked) else
                   "screenshot" if source else "pasted text")
    with _write_lock:
        mapping = load_mapping(mapping_path)
        quotes_path.parent.mkdir(parents=True, exist_ok=True)
        new_file = not quotes_path.is_file() or quotes_path.stat().st_size == 0
        with quotes_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=QUOTE_FIELDS)
            if new_file:
                writer.writeheader()
            for row in checked:
                writer.writerow({"quote_id": quote_id, "quote_date": quote_date, "supplier_name": supplier,
                                 "source_name": source, "source_type": source_type, "item_name_raw": row["item"],
                                 "sku": row["sku"], "cost_jpy": row["cost"], "image_name": source,
                                 "notes": "; ".join(filter(None, ["saved in the store admin",
                                                                  row.get("note", "")]))})
        learned = {}
        if remember:
            for row in checked:
                key = normalize_name(row["item"])
                if row["sku"] and key and key not in mapping and key not in learned:
                    learned[key] = (row["item"], row["sku"])
        if learned:
            new_map = not mapping_path.is_file() or mapping_path.stat().st_size == 0
            with mapping_path.open("a", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=MAPPING_FIELDS)
                if new_map:
                    writer.writeheader()
                for item, sku in learned.values():
                    writer.writerow({"supplier_name_raw": item, "sku": sku,
                                     "note": f"added in the store admin {quote_date}"})
    return {"quote_id": quote_id, "saved": len(checked), "matched": sum(1 for r in checked if r["sku"]),
            "remembered": len(learned)}


def recent_quotes(path: Path, limit: int = 8) -> list[dict]:
    """The newest saved quotes, one line each: {quote_id, quote_date, supplier, rows, matched}."""
    if not path.is_file():
        return []
    quotes: dict[str, dict] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            qid = str(row.get("quote_id") or "")
            q = quotes.setdefault(qid, {"quote_id": qid, "quote_date": row.get("quote_date") or "",
                                        "supplier": row.get("supplier_name") or "", "rows": 0, "matched": 0})
            q["rows"] += 1
            q["matched"] += 1 if str(row.get("sku") or "").strip() else 0
    return sorted(quotes.values(), key=lambda q: (q["quote_date"], q["quote_id"]), reverse=True)[:limit]
