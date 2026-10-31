#!/usr/bin/env python3
"""Write the market prices the storefront shows on product pages.

The storefront (poke6s.com, `storefront/`) never queries the market database. Instead this
script, run after the market's daily update, writes one small JSON file it reads:

    data/storefront/market_reference.json

    {
      "generated_at": "2026-10-31T09:00:00Z",
      "latest_date": "2026-10-30",              # newest price date in the snapshots
      "source": "TCGplayer market prices via TCGCSV, from market.poke6s.com",
      "products": {
        "630431": {                              # TCGplayer product ID
          "market": "EN",                        # EN or JP
          "name": "Black Bolt Booster Bundle",
          "group_name": "SV: Black Bolt",        # TCGplayer set (group) name
          "number": "",                          # card number, singles only
          "sub_type": "Normal",                  # printing whose price is given
          "kind": "sealed",                      # sealed or card
          "price": 93.12,                        # latest TCGplayer market price, USD
          "roc_30d_pct": 3.2,                    # change over 30 days, percent (may be null)
          "url": "https://market.poke6s.com/?kind=sealed&product=630431%7C%7CNormal"
        }
      }
    }

Which products: every sealed product in the English and Japanese snapshots, plus any card
named in the store's SKU mapping or intake CSVs. One entry per product ID; when a product has
several printings the Normal one is used, then Holofoil, then the first listed.

Usage (inside the market container):
    python scripts/build_storefront_market_reference.py [--output PATH]

Not yet in the daily job: adding it to cron is an owner step (docs/storefront_plan.md).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode

REPO_ROOT = Path(__file__).resolve().parents[1]
EN_SIGNAL_CSV = REPO_ROOT / "data" / "extracted" / "pokemon_product_signal_snapshot.csv"
JP_SIGNAL_CSV = REPO_ROOT / "data" / "extracted" / "pokemon_jp_product_signal_snapshot.csv"
STORE_ID_FILES = (
    REPO_ROOT / "data" / "squarespace_tcgplayer_mapping.csv",
    REPO_ROOT / "data" / "new_sealed_intake.csv",
    REPO_ROOT / "data" / "new_singles_intake.csv",
)
DEFAULT_OUTPUT = REPO_ROOT / "data" / "storefront" / "market_reference.json"
MARKET_SITE = "https://market.poke6s.com"
SUBTYPE_PREFERENCE = ("Normal", "Holofoil")


def store_product_ids(paths=STORE_ID_FILES) -> set[str]:
    """TCGplayer product IDs the store's CSVs mention (mapping and intake files)."""
    ids: set[str] = set()
    for path in paths:
        if not path.exists():
            continue
        with path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                for column in ("tcgplayer_product_id", "product_id"):
                    for pid in (row.get(column) or "").split("|"):
                        if pid.strip().isdigit():
                            ids.add(pid.strip())
    return ids


def market_url(market: str, kind: str, product_id: str, sub_type: str) -> str:
    """Deep link into the desktop app at market.poke6s.com (see desktop_home.html syncUrl)."""
    params = {}
    if market == "JP":
        params["market"] = "JP"
    if kind == "sealed":
        params["kind"] = "sealed"
    params["product"] = f"{product_id}||{sub_type}"
    return f"{MARKET_SITE}/?{urlencode(params)}"


def to_float(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def read_snapshot(path: Path, market: str, wanted: set[str]) -> tuple[dict[str, dict], str]:
    """Entries for sealed products and wanted IDs, plus the newest price date seen."""
    chosen: dict[str, tuple[int, dict]] = {}
    latest = ""
    if not path.exists():
        return {}, latest
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            pid = (row.get("productId") or "").strip()
            kind = "sealed" if row.get("productKind") == "sealed" else "card"
            if not pid or (kind != "sealed" and pid not in wanted):
                continue
            price = to_float(row.get("latest_price"))
            if not price or price <= 0:
                continue
            sub_type = (row.get("subTypeName") or "").strip()
            rank = SUBTYPE_PREFERENCE.index(sub_type) if sub_type in SUBTYPE_PREFERENCE else len(SUBTYPE_PREFERENCE)
            if pid in chosen and chosen[pid][0] <= rank:
                continue
            roc = to_float(row.get("roc_30d_pct"))
            latest = max(latest, (row.get("latest_date") or "").strip())
            chosen[pid] = (rank, {
                "market": market,
                "name": (row.get("productName") or "").strip(),
                "group_name": (row.get("groupName") or "").strip(),
                "number": (row.get("number") or "").strip(),
                "sub_type": sub_type,
                "kind": kind,
                "price": round(price, 2),
                "roc_30d_pct": round(roc, 1) if roc is not None else None,
                "url": market_url(market, kind, pid, sub_type),
            })
    return {pid: entry for pid, (_rank, entry) in chosen.items()}, latest


def build(en_csv: Path = EN_SIGNAL_CSV, jp_csv: Path = JP_SIGNAL_CSV, wanted: set[str] | None = None) -> dict:
    wanted = store_product_ids() if wanted is None else wanted
    en, en_date = read_snapshot(en_csv, "EN", wanted)
    jp, jp_date = read_snapshot(jp_csv, "JP", wanted)
    # TCGplayer product IDs are unique across categories, so the two markets never collide.
    return {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latest_date": max(en_date, jp_date) or None,
        "source": "TCGplayer market prices via TCGCSV, from market.poke6s.com",
        "products": {**en, **jp},
    }


def write(data: dict, output: Path) -> None:
    """Write atomically, so the storefront never reads a half-written file."""
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, output)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write data/storefront/market_reference.json")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    data = build()
    if not data["products"]:
        print("No market rows found; leaving the existing file alone.", file=sys.stderr)
        return 1
    write(data, args.output)
    print(f"wrote {len(data['products'])} products (prices of {data['latest_date']}) to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
