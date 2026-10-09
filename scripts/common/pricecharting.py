"""Parse PriceCharting's daily Pokemon CSV and match rows to TCGplayer product + printing.

PriceCharting keys products by its own id and gives one TCGplayer id per row, but a
TCGplayer product can have several PriceCharting rows that differ only by a bracket tag
in the name: "Regice #45" vs "Regice [Reverse Holo] #45", "Charizard [1st Edition] #12".
Our price history is keyed by (productId, subTypeName), so the tag picks the printing.

License: data is shown on the site with permission (PriceCharting, 2026-10-09) on the
condition that every price links back to its PriceCharting product page. If the
subscription ends, delete the stored PriceCharting data.
"""

from __future__ import annotations

import re

PRODUCT_URL = "https://www.pricecharting.com/game/{id}"

# CSV column -> our column. Grade meanings for trading cards per PriceCharting's docs.
PRICE_COLUMNS = {
    "loose-price": "ungraded",
    "cib-price": "grade7",
    "new-price": "grade8",
    "graded-price": "grade9",
    "box-only-price": "grade9_5",
    "manual-only-price": "psa10",
    "bgs-10-price": "bgs10",
    "condition-17-price": "cgc10",
    "condition-18-price": "sgc10",
}

_TAG = re.compile(r"\[([^\]]+)\]")


def parse_money(value: str | None) -> float | None:
    text = str(value or "").replace("$", "").replace(",", "").strip()
    if not text:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if number > 0 else None


def parse_int(value: str | None) -> int | None:
    text = str(value or "").strip()
    return int(text) if text.isdigit() else None


def printing_tags(product_name: str) -> list[str]:
    return [tag.strip().lower() for tag in _TAG.findall(product_name or "")]


def choose_subtype(tags: list[str], subtypes: set[str], sibling_has_first_edition: bool = False) -> str | None:
    """Pick the TCGplayer printing a PriceCharting row describes, or None if unclear.

    `subtypes` are the printings we have prices for on that TCGplayer product.
    `sibling_has_first_edition` says whether another PriceCharting row for the same
    product is tagged [1st Edition].
    """
    if not subtypes:
        return None
    joined = " ".join(tags)
    reverse = [s for s in subtypes if "reverse" in s.lower()]
    first = [s for s in subtypes if s.lower().startswith("1st edition")]
    base = [s for s in subtypes if s not in reverse and s not in first]

    if "reverse" in joined:
        pool = reverse
    elif "1st edition" in joined:
        pool = first
    else:
        pool = base
        # Many Japanese cards only exist as 1st Edition and PriceCharting lists them
        # untagged; if no sibling row claims 1st Edition, the untagged row is it.
        if not pool and not sibling_has_first_edition:
            pool = first
    if not pool:
        return None
    if len(pool) == 1:
        return pool[0]
    # Two candidates left (e.g. Normal vs Holofoil, Unlimited vs Unlimited Holofoil).
    wants_holo = "holo" in joined and "reverse" not in joined
    holo = [s for s in pool if "holo" in s.lower()]
    plain = [s for s in pool if "holo" not in s.lower()]
    if wants_holo and len(holo) == 1:
        return holo[0]
    if not wants_holo and len(plain) == 1:
        return plain[0]
    return None


def parse_row(row: dict) -> dict | None:
    """Normalize one CSV row; returns None for rows without a PriceCharting id."""
    pc_id = parse_int(row.get("id"))
    if pc_id is None:
        return None
    name = (row.get("product-name") or "").strip()
    out = {
        "pc_id": pc_id,
        "set_name": (row.get("console-name") or "").strip(),
        "product_name": name,
        "tcg_id": parse_int(row.get("tcg-id")),
        "printing_tag": ", ".join(printing_tags(name)),
        "sales_volume": parse_int(row.get("sales-volume")),
        "release_date": (row.get("release-date") or "").strip() or None,
    }
    for column, field in PRICE_COLUMNS.items():
        out[field] = parse_money(row.get(column))
    return out


def match_rows(rows: list[dict], subtypes_by_product: dict[int, set[str]]) -> tuple[list[dict], dict[str, int]]:
    """Map parsed rows to (productId, subTypeName); one PriceCharting row per printing.

    When several rows land on the same printing (e.g. a base card and a "[Prize Pack]"
    reprint sharing a TCGplayer id), the row with the fewest extra tags wins, then the
    one with the most yearly sales.
    """
    stats = {"rows": len(rows), "no_tcg_id": 0, "not_in_catalog": 0, "ambiguous_printing": 0, "matched": 0, "duplicates_dropped": 0}
    best: dict[tuple[int, str], dict] = {}
    first_edition_tagged = {
        row["tcg_id"] for row in rows
        if row["tcg_id"] is not None and "1st edition" in " ".join(printing_tags(row["product_name"]))
    }
    for row in rows:
        tcg_id = row["tcg_id"]
        if tcg_id is None:
            stats["no_tcg_id"] += 1
            continue
        subtypes = subtypes_by_product.get(tcg_id)
        if not subtypes:
            stats["not_in_catalog"] += 1
            continue
        tags = printing_tags(row["product_name"])
        subtype = choose_subtype(tags, subtypes, tcg_id in first_edition_tagged)
        if subtype is None:
            stats["ambiguous_printing"] += 1
            continue
        key = (tcg_id, subtype)
        rank = (len(tags), -(row["sales_volume"] or 0))
        current = best.get(key)
        if current is not None:
            stats["duplicates_dropped"] += 1
            if rank >= current["_rank"]:
                continue
        best[key] = {**row, "productId": tcg_id, "subTypeName": subtype, "_rank": rank}
    matched = []
    for row in best.values():
        row.pop("_rank")
        matched.append(row)
    stats["matched"] = len(matched)
    return matched, stats
