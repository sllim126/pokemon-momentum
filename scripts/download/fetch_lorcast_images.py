#!/usr/bin/env python3
"""Download Lorcana card images from Lorcast once, so the site can serve them itself.

Lorcast (https://lorcast.com/docs/api) is a free fan-run Lorcana database: no key, no cost.
Its card records carry `tcgplayer_id`, which is our productId, so each image matches a
TCGplayer product exactly. The owner chose this over Scrydex on 2026-11-05: Scrydex's API is
paid and its terms forbid mirroring (docs/lorcana_market_plan.md, "Data sources").

What it does:

1. Reads the set list and each set's cards from the API (about 25 requests). The answers are
   cached in data/lorcana_images/cards/ and reused for 7 days; Lorcast asks for at least 24.
2. Downloads the "large" image (674x940 AVIF) of every card not saved yet, or whose image
   address changed (Lorcast changes the address's query string when an image is replaced),
   to data/lorcana_images/large/<tcgplayer_id>.avif.
3. Writes data/lorcana_images/manifest.json: tcgplayer_id -> file, source address, set code
   and collector number, for the site to look up.

Politeness: one request at a time, 150 ms apart (Lorcast asks for 50-100 ms between API
requests; its image server has no limit, but the same pace is used). The run stops on HTTP
403 or 429 and does not retry. Files are written to a temporary name and renamed when
complete, so an interrupted run never leaves half an image. Re-running only fetches what's
missing. data/lorcana_images/ is git-ignored: the art belongs to Disney/Ravensburger.

Run inside the container:

    python scripts/download/fetch_lorcast_images.py             # everything missing
    python scripts/download/fetch_lorcast_images.py --limit 20  # try a few first
    python scripts/download/fetch_lorcast_images.py --refresh   # re-read the card lists now

Exit codes: 0 = done, 1 = some images failed, 3 = Lorcast blocked or throttled us (403/429).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

API_BASE = "https://api.lorcast.com/v0"
OUT_DIR = Path("/app/data/lorcana_images")
IMAGE_SIZE = "large"  # 674x940 AVIF; "full" is a 1468x2048 JPG, several times bigger
CARD_LIST_MAX_AGE_SECONDS = 7 * 24 * 3600
REQUEST_SPACING_SECONDS = 0.15
TIMEOUT_SECONDS = 30
USER_AGENT = "Poke6sMarket/1.0.0 (Lorcana price research; images cached once)"


class LorcastBlockedError(RuntimeError):
    """Lorcast answered 403 or 429: stop the whole run."""


class PoliteClient:
    """One requests session that spaces every request and stops on 403/429."""

    def __init__(self, spacing: float = REQUEST_SPACING_SECONDS):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = os.getenv("POKE6S_LORCAST_USER_AGENT", USER_AGENT)
        self.spacing = spacing
        self._last = 0.0

    def get(self, url: str) -> requests.Response:
        wait = self._last + self.spacing - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        try:
            response = self.session.get(url, timeout=TIMEOUT_SECONDS)
        finally:
            self._last = time.monotonic()
        if response.status_code in (403, 429):
            raise LorcastBlockedError(f"HTTP {response.status_code} from {url.split('?')[0]}")
        response.raise_for_status()
        return response


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Lorcana card images from Lorcast.")
    parser.add_argument("--limit", type=int, default=0, help="Download at most N images (for a trial run).")
    parser.add_argument("--refresh", action="store_true", help="Re-read the card lists even if the cache is fresh.")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def write_atomic(path: Path, data: bytes) -> None:
    """Write to <name>.part, then rename, so readers never see a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".part")
    tmp.write_bytes(data)
    tmp.replace(path)


def cached_json(client: PoliteClient, url: str, cache_path: Path, refresh: bool):
    """Return the API answer, from the cache when it's younger than CARD_LIST_MAX_AGE_SECONDS."""
    if not refresh and cache_path.exists():
        if time.time() - cache_path.stat().st_mtime < CARD_LIST_MAX_AGE_SECONDS:
            return json.loads(cache_path.read_text())
    data = client.get(url).json()
    write_atomic(cache_path, json.dumps(data).encode())
    return data


def image_url(card: dict) -> str | None:
    """The card's image address in IMAGE_SIZE, as Lorcast gives it (its docs say not to build
    these addresses ourselves)."""
    return ((card.get("image_uris") or {}).get("digital") or {}).get(IMAGE_SIZE)


def collect_cards(client: PoliteClient, out_dir: Path, refresh: bool) -> list[dict]:
    """Every card of every set, read through the cache."""
    sets = cached_json(client, f"{API_BASE}/sets", out_dir / "cards" / "_sets.json", refresh)
    sets = sets.get("results", sets) if isinstance(sets, dict) else sets
    cards: list[dict] = []
    for lorcana_set in sets:
        set_id = lorcana_set["id"]
        code = str(lorcana_set.get("code") or set_id)
        set_cards = cached_json(client, f"{API_BASE}/sets/{set_id}/cards", out_dir / "cards" / f"{code}.json", refresh)
        cards.extend(set_cards.get("results", set_cards) if isinstance(set_cards, dict) else set_cards)
    return cards


def plan_downloads(cards: list[dict], manifest: dict, out_dir: Path) -> tuple[dict, list[tuple[str, str]]]:
    """Build the new manifest and the (tcgplayer_id, url) pairs still to download.

    A card is skipped when it has no tcgplayer_id (nothing to match it to) or no image yet.
    An image is (re)downloaded when its file is missing or its source address changed.
    """
    new_manifest: dict = {}
    todo: list[tuple[str, str]] = []
    for card in cards:
        tcgplayer_id = card.get("tcgplayer_id")
        url = image_url(card)
        if not tcgplayer_id or not url:
            continue
        key = str(tcgplayer_id)
        relative = f"{IMAGE_SIZE}/{key}.avif"
        entry = {
            "file": relative,
            "source": url,
            "lorcast_id": card.get("id"),
            "set": (card.get("set") or {}).get("code"),
            "number": card.get("collector_number"),
        }
        previous = manifest.get(key) or {}
        if not (out_dir / relative).exists() or previous.get("source") != url:
            todo.append((key, url))
        new_manifest[key] = entry
    return new_manifest, todo


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    out_dir: Path = args.out_dir
    manifest_path = out_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text()).get("images", {}) if manifest_path.exists() else {}
    client = PoliteClient()

    try:
        cards = collect_cards(client, out_dir, args.refresh)
    except LorcastBlockedError as exc:
        print(f"STOPPED: Lorcast blocked or throttled us ({exc}); not retrying.")
        return 3

    new_manifest, todo = plan_downloads(cards, manifest, out_dir)
    if args.limit:
        todo = todo[: args.limit]
    print(f"{len(cards)} cards from Lorcast, {len(new_manifest)} with a TCGplayer id and an image, {len(todo)} to download")

    failed: set[str] = set()
    status = 0
    for index, (key, url) in enumerate(todo, start=1):
        try:
            write_atomic(out_dir / new_manifest[key]["file"], client.get(url).content)
        except LorcastBlockedError as exc:
            print(f"STOPPED: Lorcast blocked or throttled us ({exc}); not retrying.")
            status = 3
            break
        except requests.RequestException as exc:
            print(f"FAILED {key}: {exc.__class__.__name__}")
            failed.add(key)
        if index % 250 == 0:
            print(f"  {index}/{len(todo)} downloaded")

    # Only list images that are on disk, so the site never points at a missing file.
    saved = {k: v for k, v in new_manifest.items() if k not in failed and (out_dir / v["file"]).exists()}
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "size": IMAGE_SIZE, "images": saved}
    write_atomic(manifest_path, json.dumps(payload, indent=1, sort_keys=True).encode())
    print(f"Manifest: {len(saved)} images on disk, {len(failed)} failed")
    if status:
        return status
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
