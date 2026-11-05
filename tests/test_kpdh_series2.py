"""Integrity checks for the standalone KPDH Series 2 binder catalog."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BINDER = ROOT / "kpdh-binder"


def _catalog() -> dict:
    source = (BINDER / "cards" / "series2-catalog.js").read_text(encoding="utf-8")
    prefix = "window.KPDH_SERIES2="
    assert source.startswith(prefix)
    assert source.endswith(";\n")
    return json.loads(source[len(prefix) : -2])


def _cards_by_set() -> dict[str, list[dict]]:
    return {set_data["key"]: set_data["cards"] for set_data in _catalog()["sets"]}


def test_kpdh_series2_official_counts_and_prefixes():
    cards = _cards_by_set()
    assert len(cards["energy-2"]) == 142
    assert len(cards["classic-2"]) == 111
    assert all(card["code"].startswith("KPDH02-") for group in cards.values() for card in group)
    assert not any("KPDH01" in card["code"] for group in cards.values() for card in group)


def test_kpdh_series2_rarity_counts_match_kayou():
    expected = {
        "energy-2": {"SE": 6, "BP": 8, "CR": 9, "SP": 6, "LR": 6, "UR": 8,
                     "TR": 20, "QR": 19, "HR": 24, "SSR": 16, "SR": 20},
        "classic-2": {"CR": 6, "ZR": 6, "UR": 8, "TR": 20, "QR": 19,
                      "SSR": 13, "SR": 15, "R": 24},
    }
    catalog = _catalog()
    assert {set_data["key"]: set_data["rarityCounts"] for set_data in catalog["sets"]} == expected


def test_kpdh_series2_cr_numbering_continues_from_classic_to_energy():
    cards = _cards_by_set()
    classic_codes = [card["code"] for card in cards["classic-2"] if card["rarity"] == "CR"]
    energy_codes = [card["code"] for card in cards["energy-2"] if card["rarity"] == "CR"]
    assert classic_codes == [f"KPDH02-CR-{number:03d}" for number in range(1, 7)]
    assert energy_codes == [f"KPDH02-CR-{number:03d}" for number in range(7, 16)]


def test_kpdh_series2_assets_and_provenance_are_complete():
    seen_ids = set()
    seen_pairs = set()
    for set_data in _catalog()["sets"]:
        assert set_data["sourcePage"].startswith("https://www.kayouofficial.com/")
        assert set_data["releaseDate"] == "October 2026"
        for card in set_data["cards"]:
            assert card["id"] not in seen_ids
            seen_ids.add(card["id"])
            pair = (card["setKey"], card["code"])
            assert pair not in seen_pairs
            seen_pairs.add(pair)
            assert card["sourcePage"].startswith("https://www.kayouofficial.com/")
            assert card["sourceImage"].startswith("https://static-sg.kayouofficial.com/")
            image = BINDER / card["localImage"]
            thumbnail = BINDER / card["thumbnail"]
            assert image.is_file()
            assert thumbnail.is_file()
            assert hashlib.sha256(image.read_bytes()).hexdigest() == card["sha256"]


def test_kpdh_series2_shared_cards_are_explicit_and_ui_is_collection_safe():
    cards = _cards_by_set()
    shared_rarities = {"UR", "TR", "QR", "SSR", "SR"}
    shared_codes = {
        card["code"] for card in cards["classic-2"] if card["rarity"] in shared_rarities
    }
    energy_codes = {card["code"] for card in cards["energy-2"]}
    classic_exclusives = [
        card for card in cards["classic-2"] if card["rarity"] not in shared_rarities
    ]
    # CR artwork differs between products and its numbering now continues from
    # Classic into Energy, so only the five reviewed shared rarities merge.
    assert len(shared_codes) == 75
    assert shared_codes <= energy_codes
    assert len(classic_exclusives) == 36
    assert len(cards["energy-2"]) + len(classic_exclusives) == 178

    html = (BINDER / "my_card_planner.html").read_text(encoding="utf-8")
    assert "cards/series2-catalog.js" in html
    assert "catalogVersion:4" in html
    assert "const KEY='kpdh-master-binder-v2'" in html
    assert "series2Shared=new Set(['UR','TR','QR','SSR','SR'])" in html


def test_kpdh_binder_can_start_with_base_rarities_without_changing_card_ids():
    html = (BINDER / "my_card_planner.html").read_text(encoding="utf-8")
    assert '<select id="binderOrder">' in html
    assert "Energy:['SR','SSR','HR','QR','TR','UR','LR','SP','◇CR','BP','SE']" in html
    assert "'energy-2':['SR','SSR','HR','QR','TR','UR','LR','SP','CR','BP','SE']" in html
    assert "order=raw?.order==='base'?'base':'premium'" in html
    assert "state.order==='base'&&rarities" in html
    assert "$('binderOrder').value=state.order" in html


def test_kpdh_series2_evidence_note_and_v3_collection_migration_are_present():
    html = (BINDER / "my_card_planner.html").read_text(encoding="utf-8")
    assert "Series 2 evidence note:" in html
    assert "physical cards show different details after release" in html
    assert "nine Energy Series 2 CR cards awaiting physical confirmation are outlined in lime" in html
    assert "function needsPhysicalVerification(card)" in html
    assert "card.setKey==='energy-2'&&card.rarity==='CR'" in html
    assert "evidence-pending" in html
    assert "evidence-badge" in html
    assert "Awaiting physical-card confirmation" in html
    assert "function migratedCounts(raw)" in html
    assert "Number(raw?.catalogVersion)>=4" in html
    assert "index+7" in html
    assert "catalogVersion:4" in html

    builder = (BINDER / "scripts" / "build_series2_catalog.py").read_text(encoding="utf-8")
    assert '"number_starts": {"CR": 7}' in builder
    assert 'start + position:03d' in builder


def test_kpdh_docker_build_includes_series2_assets():
    dockerignore = (BINDER / ".dockerignore").read_text(encoding="utf-8")
    for expected in (
        "!cards/series2-catalog.js",
        "!cards/energy-2/*.png",
        "!cards/classic-2/kpdh02-cr-*.png",
        "!cards/classic-2/kpdh02-zr-*.png",
        "!cards/classic-2/kpdh02-r-*.png",
        "!cards/thumbs/",
    ):
        assert expected in dockerignore
    nginx = (BINDER / "nginx" / "default.conf").read_text(encoding="utf-8")
    assert "location = /cards/series2-catalog.js" in nginx
    assert 'add_header Cache-Control "no-cache" always;' in nginx
