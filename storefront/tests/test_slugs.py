"""Slugs and text helpers."""

from __future__ import annotations

from storefront.app import classify

from .test_import import products


def test_slugify():
    assert classify.slugify("Pokémon Black Bolt Booster Box – Japanese") == "pokemon-black-bolt-booster-box-japanese"
    assert classify.slugify("Scarlet & Violet 151") == "scarlet-and-violet-151"
    assert classify.slugify("!!!") == "item"
    assert len(classify.slugify("word " * 40)) <= 80


def test_slugs_are_clean_and_unique(imported):
    rows = products(imported)
    # Squarespace's random slug is replaced by one made from the title.
    assert rows["ENG-BB-BB"]["slug"] == "black-bolt-booster-bundle-english"
    # Two products with the same title get distinct slugs.
    assert rows["JP-PACK-A"]["slug"] == "pokemon-booster-pack-japanese"
    assert rows["JP-PACK-B"]["slug"] == "pokemon-booster-pack-japanese-2"
    slugs = [r["slug"] for r in rows.values()]
    assert len(slugs) == len(set(slugs))


def test_fold_for_search():
    assert classify.fold("Pokémon") == "pokemon"


def test_split_group_name():
    assert classify.split_group_name("SV11B: Black Bolt") == ("sv11b", "Black Bolt")
    assert classify.split_group_name("Crown Zenith") == (None, "Crown Zenith")
