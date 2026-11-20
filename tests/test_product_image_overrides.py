"""Rules for replacing missing TCGplayer pictures (scripts/utilities/apply_product_image_overrides.py)."""

import csv
from pathlib import Path

from scripts.utilities.apply_product_image_overrides import (
    STANDINS_CSV,
    own_images,
    resolve_overrides,
    standins,
)

CDN = "https://tcgplayer-cdn.tcgplayer.com/product/{}_200w.jpg"


def test_standin_uses_the_other_products_picture():
    urls = {1: CDN.format(1), 2: CDN.format(2)}
    assert resolve_overrides(urls, own={}, subs={1: 2}) == {1: CDN.format(2)}


def test_own_picture_beats_standin_and_is_used_by_products_standing_in_for_it():
    urls = {1: CDN.format(1), 2: CDN.format(2), 3: CDN.format(3)}
    own = {1: "/images/products/1.jpg", 2: "/images/products/2.png"}
    # 1 has its own picture and a stand-in: its own wins. 3 stands in with 2, which has our picture.
    assert resolve_overrides(urls, own, subs={1: 2, 3: 2}) == {
        1: "/images/products/1.jpg",
        2: "/images/products/2.png",
        3: "/images/products/2.png",
    }


def test_nothing_to_do_when_already_applied_or_unknown():
    urls = {1: CDN.format(2), 2: CDN.format(2)}
    # Already applied; 9 isn't in this catalog; 1 -> 8 where 8 is missing.
    assert resolve_overrides(urls, own={9: "/images/products/9.jpg"}, subs={1: 2, 5: 8}) == {}


def test_own_images_reads_only_numbered_picture_files(tmp_path: Path):
    for name in ["123.jpg", "456.WEBP", "notes.txt", "abc.png"]:
        (tmp_path / name).write_bytes(b"x")
    assert own_images(tmp_path) == {123: "/images/products/123.jpg", 456: "/images/products/456.WEBP"}
    assert own_images(tmp_path / "missing") == {}


def test_standins_list_is_well_formed():
    with STANDINS_CSV.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows, "the stand-in list is empty"
    ids = [row["productId"] for row in rows]
    assert len(ids) == len(set(ids)), "a product is listed twice"
    for row in rows:
        assert row["productId"].isdigit() and row["standin_productId"].isdigit()
        assert row["productId"] != row["standin_productId"]
        assert row["why"].strip()
    assert len(standins()) == len(rows)
