"""Collector nicknames for search: "moonbreon" finds Umbreon VMAX 215/203 (Evolving Skies).

Each nickname maps to an ordinary search query that /search already answers well (name plus
card number). Keys are folded like the search folds text: lower case, no spaces, hyphens,
apostrophes or full stops. Add a name only when collectors widely use it and the query lands
on the right card first; tests/test_market_insights.py checks the list's shape.
"""

import re

NICKNAMES = {
    "moonbreon": "umbreon vmax 215",
    "umbreonalt": "umbreon vmax 215",
    "rayquazaalt": "rayquaza vmax 218",
    "giratinaalt": "giratina v 186",
    "lugiaalt": "lugia v 186",
    "gengaralt": "gengar vmax 271",
    "espeonalt": "espeon vmax 270",
    "sylveonalt": "sylveon vmax 212",
    "leafeonalt": "leafeon vmax 205",
    "glaceonalt": "glaceon vmax 209",
    "dragonitealt": "dragonite v 192",
    "vangoghpikachu": "pikachu with grey felt hat",
    "prismaticumbreon": "umbreon ex 161",
    "151zard": "charizard ex 199",
    "151charizard": "charizard ex 199",
    "zard": "charizard",
}


def fold(text: str) -> str:
    """Lower case without spaces or punctuation, so "Moon-breon" and "moonbreon" match."""
    return re.sub(r"[\s'’.\-:/]", "", str(text or "").lower())


def expand_nickname(term: str) -> str | None:
    """The search query for a nickname, or None when `term` isn't one."""
    return NICKNAMES.get(fold(term))
