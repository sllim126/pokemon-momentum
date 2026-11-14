"""Lorcana rarity tiers, used to filter and group Lorcana cards like Pokemon's rarity buckets.

The tiers are the owner-approved best judgement in docs/lorcana_market_plan.md
("Lorcana-specific choices"); change them there and here together. TCGplayer's Rarity
values for Lorcana (checked 2026-11-14 across all 21 sets): Common, Uncommon, Rare,
Super Rare, Legendary, Epic, Enchanted, Iconic, Promo, Quest, and "None" on some promos.

    chase  Enchanted, Iconic, Epic      the cards collectors chase
    high   Legendary, Super Rare
    base   Rare, Uncommon, Common
    promo  Promo, None                  split further by PromoType where it matters
    quest  Quest                        Illumineer's Quest cards, shown on their own

Printings (Normal, Cold Foil, Holofoil) are not part of the tier: each printing is its own
tracked item, as Pokemon reverse holos are.
"""

from __future__ import annotations

LORCANA_RARITY_TIERS: dict[str, tuple[str, ...]] = {
    "chase": ("Enchanted", "Iconic", "Epic"),
    "high": ("Legendary", "Super Rare"),
    "base": ("Rare", "Uncommon", "Common"),
    "promo": ("Promo", "None"),
    "quest": ("Quest",),
}

TIER_LABELS = {
    "chase": "Chase (Enchanted, Iconic, Epic)",
    "high": "Legendary and Super Rare",
    "base": "Rare and below",
    "promo": "Promos",
    "quest": "Illumineer's Quest",
}

_TIER_BY_RARITY = {
    rarity.lower(): tier for tier, rarities in LORCANA_RARITY_TIERS.items() for rarity in rarities
}


def lorcana_rarity_tier(rarity: str | None) -> str | None:
    """Return the tier key for a TCGplayer Lorcana rarity, or None if it's empty or unknown.

    Unknown values return None rather than a guess, so a new rarity shows up as
    unclassified instead of landing quietly in the wrong tier.
    """
    if not rarity:
        return None
    return _TIER_BY_RARITY.get(str(rarity).strip().lower())


def lorcana_rarity_tier_sql(column: str = "p.rarity") -> str:
    """The same mapping as a SQL CASE expression, for DuckDB queries over product rows."""
    whens = "\n".join(
        f"  WHEN lower(trim(COALESCE({column}, ''))) IN ({', '.join(repr(r.lower()) for r in rarities)}) THEN '{tier}'"
        for tier, rarities in LORCANA_RARITY_TIERS.items()
    )
    return f"CASE\n{whens}\n  ELSE NULL\nEND"
