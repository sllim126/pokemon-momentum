"""301 redirects from old Squarespace addresses (plan "Behind the scenes").

Two sources, checked in this order when a request would otherwise be a 404:
1. `storefront/redirects.csv` (committed): page and category addresses, e.g. /faq-1 -> /faq.
   Columns: old_path,new_path. Matching ignores letter case and a trailing slash.
2. The `product_redirects` table: each product's old /shop/p/<slug>, written by the importer.
   Any other /shop/p/... address (a single that now lives on TCGplayer, a deleted product)
   goes to /shop.

Squarespace's `/shop?tag=...` filter links are translated in the /shop route itself.
"""

from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path

REDIRECTS_CSV = Path(__file__).resolve().parents[1] / "redirects.csv"

# Squarespace shop tag filters -> our /shop query parameters.
SHOP_TAG_FILTERS = {
    "japanese": {"lang": "ja"},
    "english": {"lang": "en"},
    "booster bundle": {"type": "booster-bundle"},
    "booster box": {"type": "booster-box"},
    "booster pack": {"type": "booster-pack"},
    "elite trainer box": {"type": "elite-trainer-box"},
    "singles": {},
}


def normalize(path: str) -> str:
    path = "/" + path.strip().lstrip("/")
    if len(path) > 1:
        path = path.rstrip("/")
    return path.lower()


@lru_cache(maxsize=4)
def load(path: Path = REDIRECTS_CSV) -> dict[str, str]:
    table: dict[str, str] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(line for line in handle if not line.startswith("#")):
            old, new = (row.get("old_path") or "").strip(), (row.get("new_path") or "").strip()
            if old and new:
                table[normalize(old)] = new
    return table


def lookup(path: str, table: dict[str, str] | None = None) -> str | None:
    return (table if table is not None else load()).get(normalize(path))
