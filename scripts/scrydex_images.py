#!/usr/bin/env python3
"""Build Scrydex card image URLs from local draft metadata."""

from __future__ import annotations

from typing import Optional


SCRYDEX_SET_IDS_BY_SET_NAME = {
    "EX Delta Species": "ex11",
    "EX Crystal Guardians": "ex14",
    "WoTC Promo": "basep",
    "SWSH: Crown Zenith: Galarian Gallery": "swsh12pt5gg",
    "SWSH11: Lost Origin Trainer Gallery": "swsh11tg",
    "SV: Scarlet & Violet Promo Cards": "svp",
    "Generations: Radiant Collection": "g1",
    "Neo Discovery": "neo2",
    "SWSH04: Vivid Voltage": "swsh4",
}


def normalize_scrydex_number(card_number: str | None) -> str:
    # Scrydex card ids use the printed number token before the slash, preserving
    # prefixes like TG/GG/RC while normalizing leading-zero numeric cards.
    raw = str(card_number or "").strip()
    if not raw:
        return ""
    token = raw.split("/", 1)[0].strip()
    if token.isdigit():
        return str(int(token))
    return token


def build_scrydex_card_image_url(set_name: str | None, card_number: str | None) -> Optional[str]:
    set_id = SCRYDEX_SET_IDS_BY_SET_NAME.get(str(set_name or "").strip())
    normalized_number = normalize_scrydex_number(card_number)
    if not set_id or not normalized_number:
        return None
    return f"https://images.scrydex.com/pokemon/{set_id}-{normalized_number}/medium"
