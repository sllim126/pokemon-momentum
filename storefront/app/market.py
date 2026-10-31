"""Market prices from market.poke6s.com, for product pages.

The market app writes `data/storefront/market_reference.json` after its daily update
(`scripts/build_storefront_market_reference.py`). The store only reads that file; it never
queries the market database. If the file is missing or old, product pages simply leave the
market price out.

File layout (see the writer for the full description):

    {"generated_at": "...", "latest_date": "2026-10-30",
     "products": {"630431": {"price": 93.12, "roc_30d_pct": 3.2, "url": "https://...",
                             "name": "...", "group_name": "SV: Black Bolt", "market": "EN", ...}}}
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MarketSummary:
    price: float
    roc_30d_pct: float | None
    url: str | None
    latest_date: str | None
    # More than one TCGplayer product behind one listing (e.g. a random-design tin):
    # the price is the average across them.
    averaged_over: int


class MarketReference:
    """Loads the JSON file and reloads it when it changes on disk."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._mtime: float | None = None
        self._data: dict = {}

    def data(self) -> dict:
        try:
            mtime = self.path.stat().st_mtime
        except FileNotFoundError:
            return {}
        with self._lock:
            if mtime != self._mtime:
                try:
                    self._data = json.loads(self.path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    self._data = {}
                self._mtime = mtime
            return self._data

    def entry(self, product_id: str) -> dict | None:
        return (self.data().get("products") or {}).get(str(product_id).strip())

    def summary(self, tcgplayer_product_ids: str) -> MarketSummary | None:
        ids = [pid.strip() for pid in (tcgplayer_product_ids or "").split("|") if pid.strip()]
        entries = [e for e in (self.entry(pid) for pid in ids) if e and e.get("price")]
        if not entries:
            return None
        prices = [float(e["price"]) for e in entries]
        trends = [float(e["roc_30d_pct"]) for e in entries if e.get("roc_30d_pct") is not None]
        return MarketSummary(
            price=round(sum(prices) / len(prices), 2),
            roc_30d_pct=round(sum(trends) / len(trends), 1) if trends else None,
            url=entries[0].get("url"),
            latest_date=self.data().get("latest_date"),
            averaged_over=len(entries),
        )
