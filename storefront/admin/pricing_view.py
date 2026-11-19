"""The admin's Pricing page (Phase 3, step 6): storefront/tools/reprice.py with a preview.

The page runs reprice.plan() (nothing changes), lists the prices that would move, and applies
only the ones the owner leaves ticked. The rules, the break-even floors and the logging are
reprice.py's own, so the page and the command line can't disagree.

Inputs, all read fresh on every page load:
- the private costs file (never in this repository): STOREFRONT_ADMIN_COSTS_FILE, by default
  /app/costs/store_sku_costs.csv, a read-only mount of the owner's folder set in .env
  (STOREFRONT_COSTS_DIR; docker-compose.yml). Costs are never shown: only prices and a
  "raised to break-even" note, as in reprice.py's output.
- data/store_price_rules.csv (which rule each SKU follows)
- data/storefront/market_reference.json (today's market prices)

Applying checks each ticked line against a fresh plan: a line whose current or new price no
longer matches what the page showed (the market file was refreshed, or the price was changed
on the product page in between) is skipped and reported, never applied blind.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from ..tools import reprice


def load(conn: sqlite3.Connection, costs_path: Path, rules_path: Path, market_path: Path) -> dict:
    """reprice.plan() for the page, or {"error": ...} saying which input is missing."""
    missing = [label for label, path in (("costs file", costs_path), ("price rules", rules_path),
                                         ("market prices", market_path)) if not path.is_file()]
    if missing:
        return {"error": "Can't read the " + " or the ".join(missing) + "."}
    try:
        costs = reprice.load_costs(costs_path)
        rules = reprice.load_rules(rules_path)
        reference = json.loads(market_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, KeyError) as exc:
        return {"error": f"Couldn't read the pricing inputs ({type(exc).__name__})."}
    result = reprice.plan(reprice.load_products(conn), costs, reference, rules)
    result["changes"] = [item for item in result["priced"] if item["change"]]
    result["steady"] = [item for item in result["priced"] if not item["change"]]
    return result


def pick_key(item: dict) -> str:
    """What a ticked line posts: the product and the prices the page showed for it."""
    return f"{item['id']}:{item['now']}:{item['new']}"


def chosen(result: dict, picks: list[str]) -> tuple[list[dict], int]:
    """(changes still matching a ticked line, how many ticked lines no longer match)."""
    wanted = set(picks)
    current = {pick_key(item): item for item in result.get("changes", [])}
    keep = [item for key, item in current.items() if key in wanted]
    return keep, len(wanted) - len(keep)
