#!/usr/bin/env python3
"""Build the KPDH Series 2 catalog and local image assets from Kayou.

Kayou's Series 2 page metadata has been observed using the Series 1 ``KPDH01``
prefix.  The official card images themselves print ``KPDH02``.  This script
therefore assigns codes from the reviewed printed-card convention and never
imports a code from the page metadata.

The official pages are fetched once per run. Card downloads are paced and the
script stops immediately on HTTP 403 or 429 rather than retrying aggressively.
Existing images are retained unless ``--replace-images`` is supplied.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

from bs4 import BeautifulSoup
from PIL import Image


ROOT = Path(__file__).resolve().parents[1]
USER_AGENT = "PokemonMomentum-KPDH-catalog/1.0 (one-time publisher asset build)"
REQUEST_DELAY_SECONDS = 0.15
CATALOG_VERSION = 4
SHARED_RARITIES = {"UR", "TR", "QR", "SSR", "SR"}

SETS = (
    {
        "key": "energy-2",
        "edition": "Energy",
        "series": 2,
        "title": "Energy Edition Series 2",
        "page_url": "https://www.kayouofficial.com/en-US/series/series-41jmupca",
        "product_name": "KPop Demon Hunters-Collectible Trading Cards-Energy Edition-002-NA",
        "model": "KPDH-KP-NL-002A-NA",
        "release_date": "October 2026",
        # Classic Series 2 owns CR-001 through CR-006. Kayou confirmed that
        # Energy Series 2 continues the same Series 2 sequence at CR-007.
        "number_starts": {"CR": 7},
        "groups": (
            ("SE", "Scenario-E", 6),
            ("BP", "Brush Rare", 8),
            ("CR", "Creative Rare", 9),
            ("SP", "Special Rare", 6),
            ("LR", "Legacy Rare", 6),
            ("UR", "Ultra Rare", 8),
            ("TR", "Titan Rare", 20),
            ("QR", "Cute-Rare", 19),
            ("HR", "Holo Rare", 24),
            ("SSR", "Super Spark Rare", 16),
            ("SR", "Super Rare", 20),
        ),
    },
    {
        "key": "classic-2",
        "edition": "Classic",
        "series": 2,
        "title": "Classic Edition Series 2",
        "page_url": "https://www.kayouofficial.com/en-US/series/series-udf0eb3v",
        "product_name": "KPop Demon Hunters-Collectible Trading Cards-Classic Edition-002-NA",
        "model": "KPDH-KP-JD-002-NA",
        "release_date": "October 2026",
        "groups": (
            ("CR", "Creative Rare", 6),
            ("ZR", "Zenith Rare", 6),
            ("UR", "Ultra Rare", 8),
            ("TR", "Titan Rare", 20),
            ("QR", "Cute-Rare", 19),
            ("SSR", "Super Spark Rare", 13),
            ("SR", "Super Rare", 15),
            ("R", "Rare", 24),
        ),
    },
)

NAME_NORMALIZATIONS = {
    "Abbey": "Abby",
    "DERPY&SUSSIE": "Derpy & Sussie",
    "Dokkaebi Demon": "Dokkaebi Demon",
    "Huntrix": "HUNTR/X",
    "HUNTR/X": "HUNTR/X",
    "MiRomAbby": "Mira, Romance & Abby",
    "RuJinu": "Rumi & Jinu",
    "SAJABOYS": "Saja Boys",
    "Saja boys": "Saja Boys",
    "Sussie": "Sussie",
    "your idol": "Your Idol",
    "Zoeystery": "Zoey & Mystery",
}


def fetch(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 429):
            raise SystemExit(f"Kayou returned HTTP {exc.code}; stopping without retries") from exc
        raise


def official_card_images(page_html: bytes, expected: int) -> list[dict[str, str]]:
    soup = BeautifulSoup(page_html, "html.parser")
    # All Series 2 card assets were published in this dated directory. Restricting
    # the selection avoids product art, logos, trailers and footer images.
    images = [
        {"label": image.get("alt", "").strip(), "url": image.get("src", "")}
        for image in soup.find_all("img")
        if "/overseas/website/2026/10/22/" in image.get("src", "")
    ]
    if len(images) != expected:
        raise ValueError(f"Expected {expected} official card images, found {len(images)}")
    if any(not image["label"] or not image["url"].endswith(".png") for image in images):
        raise ValueError("Every official card image must have a label and PNG source")
    return images


def image_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def save_image(url: str, destination: Path, replace: bool) -> None:
    if destination.exists() and not replace:
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(fetch(url))
    time.sleep(REQUEST_DELAY_SECONDS)


def save_thumbnail(source: Path, destination: Path, replace: bool) -> None:
    if destination.exists() and not replace:
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    with Image.open(source) as image:
        image.thumbnail((420, 420))
        image.save(destination, "WEBP", quality=78, method=6)


def build_set(
    set_info: dict,
    page_html: bytes,
    replace_images: bool,
    canonical_cards: dict[str, dict] | None = None,
) -> dict:
    expected = sum(group[2] for group in set_info["groups"])
    source_images = iter(official_card_images(page_html, expected))
    cards = []
    for rarity, rarity_name, count in set_info["groups"]:
        start = set_info.get("number_starts", {}).get(rarity, 1)
        for position in range(count):
            source = next(source_images)
            code = f"KPDH02-{rarity}-{start + position:03d}"
            if not code.startswith("KPDH02-"):
                raise AssertionError(f"Invalid Series 2 prefix: {code}")
            filename = f"{code.lower()}.png"
            canonical = (canonical_cards or {}).get(code) if rarity in SHARED_RARITIES else None
            # Normal rebuilds reuse the already verified Energy copy for shared
            # Classic cards. --replace-images downloads both products again so
            # deduplication can prove that Kayou still serves identical bytes.
            if canonical and not replace_images:
                local_path = ROOT / canonical["localImage"]
                thumb_path = ROOT / canonical["thumbnail"]
            else:
                local_path = ROOT / "cards" / set_info["key"] / filename
                thumb_path = ROOT / "cards" / "thumbs" / set_info["key"] / f"{code.lower()}.webp"
                save_image(source["url"], local_path, replace_images)
                save_thumbnail(local_path, thumb_path, replace_images)
            with Image.open(local_path) as image:
                width, height = image.size
            cards.append(
                {
                    "id": f'{set_info["key"]}:{code}',
                    "code": code,
                    "name": NAME_NORMALIZATIONS.get(source["label"], source["label"]),
                    "sourceLabel": source["label"],
                    "rarity": rarity,
                    "rarityName": rarity_name,
                    "edition": set_info["edition"],
                    "series": set_info["series"],
                    "setKey": set_info["key"],
                    "orientation": "landscape" if width > height else "portrait",
                    "sourcePage": set_info["page_url"],
                    "sourceImage": source["url"],
                    "localImage": local_path.relative_to(ROOT).as_posix(),
                    "thumbnail": thumb_path.relative_to(ROOT).as_posix(),
                    "sha256": image_sha256(local_path),
                    "verifiedOn": "2026-10-28",
                    "discrepancy": "Kayou page metadata may say KPDH01; official image prints KPDH02. Energy Series 2 CR numbering continues after Classic Series 2 CR-006.",
                }
            )
    return {
        "key": set_info["key"],
        "edition": set_info["edition"],
        "series": set_info["series"],
        "title": set_info["title"],
        "productName": set_info["product_name"],
        "model": set_info["model"],
        "releaseDate": set_info["release_date"],
        "sourcePage": set_info["page_url"],
        "verifiedOn": "2026-10-28",
        "rarityCounts": {rarity: count for rarity, _, count in set_info["groups"]},
        "cards": cards,
    }


def write_catalog(sets: list[dict]) -> None:
    payload = {
        "catalogVersion": CATALOG_VERSION,
        "generatedOn": "2026-10-28",
        "codeRule": "Series 2 uses the printed KPDH02 prefix; Energy CR numbering continues after Classic CR-006",
        "sets": sets,
    }
    destination = ROOT / "cards" / "series2-catalog.js"
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    destination.write_text(f"window.KPDH_SERIES2={serialized};\n", encoding="utf-8")


def deduplicate_shared_assets(sets: list[dict]) -> None:
    """Point byte-identical Classic cards at the Energy copy.

    The two product pages use different source URLs, which remain on their
    respective catalog records. Keeping one local file avoids shipping about
    75 duplicate full-resolution images in every container and Git checkout.
    """
    energy = {card["code"]: card for card in sets[0]["cards"]}
    for classic_card in sets[1]["cards"]:
        if classic_card["rarity"] not in SHARED_RARITIES:
            continue
        energy_card = energy[classic_card["code"]]
        if classic_card["sha256"] != energy_card["sha256"]:
            raise ValueError(f'Shared artwork differs for {classic_card["code"]}')
        duplicate_image = ROOT / classic_card["localImage"]
        duplicate_thumbnail = ROOT / classic_card["thumbnail"]
        if duplicate_image != ROOT / energy_card["localImage"]:
            duplicate_image.unlink(missing_ok=True)
        if duplicate_thumbnail != ROOT / energy_card["thumbnail"]:
            duplicate_thumbnail.unlink(missing_ok=True)
        classic_card["localImage"] = energy_card["localImage"]
        classic_card["thumbnail"] = energy_card["thumbnail"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--page-dir",
        type=Path,
        help="Read energy-2.html and classic-2.html here instead of fetching pages",
    )
    parser.add_argument("--replace-images", action="store_true")
    args = parser.parse_args()

    built_sets = []
    for set_info in SETS:
        if args.page_dir:
            page_html = (args.page_dir / f'{set_info["key"]}.html').read_bytes()
        else:
            page_html = fetch(set_info["page_url"])
        canonical_cards = (
            {card["code"]: card for card in built_sets[0]["cards"]}
            if built_sets and set_info["key"] == "classic-2"
            else None
        )
        built_sets.append(
            build_set(set_info, page_html, args.replace_images, canonical_cards=canonical_cards)
        )
    deduplicate_shared_assets(built_sets)
    write_catalog(built_sets)
    print(
        "Built "
        + ", ".join(f'{set_data["title"]}: {len(set_data["cards"])}' for set_data in built_sets)
    )


if __name__ == "__main__":
    main()
