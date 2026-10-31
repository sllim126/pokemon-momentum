"""Text helpers and the rules that sort a product into kind, type, language and category.

Used by the importer (and later by the admin) so every product is classified the same way.
The rules follow docs/store_category_tree_proposal.md: one product-type category per
product; language and set are filters, not extra categories.
"""

from __future__ import annotations

import re
import unicodedata

# product_type -> (category slug, shipping class). Shipping classes: plan "Shipping".
PRODUCT_TYPES: dict[str, tuple[str, str]] = {
    "booster-box": ("booster-boxes", "large-sealed"),
    "elite-trainer-box": ("elite-trainer-boxes", "large-sealed"),
    "booster-bundle": ("booster-bundles", "small-sealed"),
    "booster-pack": ("booster-packs", "small-sealed"),
    "collection-box": ("collection-boxes", "large-sealed"),
    "tin": ("tins", "large-sealed"),
    "accessory": ("accessories", "accessory"),
    "single": ("singles", "single"),
    "other": ("other", "large-sealed"),
}

# Shown in filters and on product pages.
PRODUCT_TYPE_LABELS = {
    "booster-box": "Booster box",
    "elite-trainer-box": "Trainer box",
    "booster-bundle": "Booster bundle",
    "booster-pack": "Booster pack",
    "collection-box": "Collection box",
    "tin": "Tin",
    "accessory": "Accessory",
    "single": "Single card",
    "other": "Other",
}

LANGUAGE_LABELS = {"en": "English", "ja": "Japanese"}

SHIP_CLASS_LABELS = {
    "single": "Single card (envelope or bubble mailer, tracked)",
    "small-sealed": "Small sealed (packs, bundles)",
    "large-sealed": "Large sealed (boxes)",
    "accessory": "Accessory",
}


def fold(text: str) -> str:
    """Lower-case and strip accents, so 'Pokémon' and 'pokemon' match in search."""
    decomposed = unicodedata.normalize("NFKD", text or "")
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch)).lower()


def slugify(text: str, max_length: int = 80) -> str:
    """'Pokémon Black Bolt Booster Box – Japanese' -> 'pokemon-black-bolt-booster-box-japanese'."""
    slug = re.sub(r"[^a-z0-9]+", "-", fold(text).replace("&", " and ")).strip("-")
    if len(slug) > max_length:
        slug = slug[:max_length].rsplit("-", 1)[0]
    return slug or "item"


def search_text(*parts: str | None) -> str:
    """One folded string over every searchable field; search matches each query word in it."""
    return " ".join(fold(p) for p in parts if p).strip()


def detect_language(title: str, sku: str) -> str | None:
    t, s = fold(title), sku.upper()
    if "japanese" in t or s.startswith("JP-"):
        return "ja"
    if "english" in t or s.startswith(("ENG-", "EN-")):
        return "en"
    return None


# Order matters: the first rule that matches wins ("Booster Box" must beat "Box").
_TYPE_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("elite-trainer-box", re.compile(r"\b(elite trainer box|etb|trainer box)\b")),
    ("booster-bundle", re.compile(r"\bbooster bundle\b")),
    ("booster-box", re.compile(r"\bbooster box\b|\bex box japanese\b|\bshiny treasures? ex box\b")),
    ("booster-pack", re.compile(r"\b(booster pack|sleeved booster|art set|blister)\b")),
    ("tin", re.compile(r"\btins?\b")),
    ("collection-box", re.compile(r"\b(collection|ex box|premium|binder collection|box set)\b")),
    ("accessory", re.compile(r"\b(mat|playmat|stand|binder|sleeves|toploader|deck box|display)\b")),
]


def detect_product_type(title: str, tags: list[str], categories: list[str], sku: str = "") -> str:
    folded_tags = {fold(t) for t in tags}
    if "singles" in folded_tags:
        return "single"
    text = fold(title)
    for product_type, pattern in _TYPE_RULES:
        if pattern.search(text):
            return product_type
    # Our own SKU suffixes: -MCAP (mini collection / ex box), -PC (premium collection), -COLL.
    if re.search(r"-(MCAP|PC|COLL)$", sku.upper()):
        return "collection-box"
    # Fall back to the Squarespace category paths (docs/store_category_tree_proposal.md).
    joined = " ".join(categories)
    for path, product_type in (
        ("/booster-box", "booster-box"),
        ("/elite-trainer-box", "elite-trainer-box"),
        ("/booster-bundle", "booster-bundle"),
        ("/booster-pack", "booster-pack"),
        ("/other/collection-boxes", "collection-box"),
        ("/other/tins", "tin"),
    ):
        if path in joined:
            return product_type
    return "other"


def kind_for_type(product_type: str) -> str:
    if product_type == "single":
        return "single"
    if product_type == "accessory":
        return "accessory"
    return "sealed"


def category_for_type(product_type: str) -> str:
    return PRODUCT_TYPES.get(product_type, PRODUCT_TYPES["other"])[0]


def ship_class_for_type(product_type: str) -> str:
    return PRODUCT_TYPES.get(product_type, PRODUCT_TYPES["other"])[1]


_GROUP_PREFIX = re.compile(r"^(?P<code>[A-Za-z0-9.\-]+):\s*(?P<name>.+)$")


def split_group_name(group_name: str) -> tuple[str | None, str]:
    """Split a TCGplayer group name into (code, set name).

    'SV11B: Black Bolt' -> ('sv11b', 'Black Bolt'); 'ME02: Phantasmal Flames' -> ('me02', ...);
    'SV: Black Bolt' -> ('sv', 'Black Bolt'); 'Crown Zenith' -> (None, 'Crown Zenith').
    """
    group_name = (group_name or "").strip()
    match = _GROUP_PREFIX.match(group_name)
    if not match:
        return None, group_name
    return match.group("code").lower(), match.group("name").strip()
