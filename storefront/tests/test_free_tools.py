"""The free tools moved from market.poke6s.com to the store on 2026-11-20: Binder Art Maker
(/binder-art, app/binder_art.py: the page, its CSP, and the set logo list built from a logo
folder and the TCGCSV groups CSV, both temporary copies here) and Collector Hub
(/collector-hub, app/collector_hub.py)."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from storefront.app.binder_art import SetLogos
from storefront.app.main import create_app

PNG = b"\x89PNG\r\n\x1a\n"   # contents only need to differ between logos, not be real images


def make_logos(tmp_path: Path) -> tuple[Path, Path]:
    logo_dir = tmp_path / "logos"
    logo_dir.mkdir()
    (logo_dir / "100.png").write_bytes(PNG + b"old set")
    (logo_dir / "200.png").write_bytes(PNG + b"new set")
    (logo_dir / "201.png").write_bytes(PNG + b"new set")      # the new set's subset: same file
    (logo_dir / "1422.png").write_bytes(PNG + b"pop")          # POP Series family
    (logo_dir / "1423.png").write_bytes(PNG + b"pop")
    (logo_dir / "999.png").write_bytes(PNG + b"unknown")       # not in the CSV: left out
    (logo_dir / "notes.png").write_bytes(PNG)                  # not a groupId: ignored
    groups = tmp_path / "pokemon_groups.csv"
    groups.write_text(
        "groupId,name,abbreviation,isSupplemental,publishedOn,modifiedOn\n"
        "100,Base Set,BS,False,1999-01-09T00:00:00,\n"
        "200,ME02: New Set,NEW,False,2026-09-26T00:00:00,\n"
        "201,ME02: New Set Promos,NEWP,True,2026-09-27T00:00:00,\n"
        "1422,POP Series 1,P1,False,2004-09-01T00:00:00,\n"
        "1423,POP Series 2,P2,False,2005-08-01T00:00:00,\n", encoding="utf-8")
    return logo_dir, groups


@pytest.fixture()
def art_client(imported, tmp_path):
    logo_dir, groups = make_logos(tmp_path)
    settings = dataclasses.replace(imported, set_logo_dir=logo_dir, groups_csv=groups)
    with TestClient(create_app(settings), follow_redirects=False) as client:
        yield client


def test_logos_merge_shared_files_newest_first_then_families(tmp_path):
    logo_dir, groups = make_logos(tmp_path)
    logos = SetLogos(logo_dir, groups).logos()
    assert logos == [
        {"name": "ME02: New Set", "year": "2026", "url": "/binder-art/logos/200.png"},
        {"name": "Base Set", "year": "1999", "url": "/binder-art/logos/100.png"},
        {"name": "POP Series", "year": "", "url": "/binder-art/logos/1422.png"},
    ]


def test_missing_folder_or_csv_gives_an_empty_list(tmp_path):
    logo_dir, groups = make_logos(tmp_path)
    assert SetLogos(tmp_path / "nowhere", groups).logos() == []
    assert SetLogos(logo_dir, tmp_path / "nowhere.csv").logos() == []


def test_page_logos_and_csp(art_client):
    page = art_client.get("/binder-art")
    assert page.status_code == 200 and "<h1>Binder Art Maker</h1>" in page.text
    for asset in ("/static/binder-art.js", "/static/binder-art.css", "/static/vendor/jspdf.umd.min.js",
                  "/static/vendor/jszip.min.js"):
        assert asset in page.text and art_client.get(asset).status_code == 200
    # The visitor's own picture is shown through a blob: URL; only this page allows it.
    assert "img-src 'self' data: blob:" in page.headers["content-security-policy"]
    assert "script-src 'self';" in page.headers["content-security-policy"]
    assert "blob:" not in art_client.get("/shop").headers["content-security-policy"]
    # No inline script or style: the CSP would block them.
    body = page.text.split("<body>", 1)[1]
    assert "<script>" not in body and "style=" not in body and "<style" not in page.text

    logos = art_client.get("/binder-art/logos.json").json()["logos"]
    assert [x["name"] for x in logos] == ["ME02: New Set", "Base Set", "POP Series"]
    image = art_client.get(logos[0]["url"])
    assert image.status_code == 200 and image.headers["content-type"] == "image/png"
    for bad in ("/binder-art/logos/999x.png", "/binder-art/logos/..%2Fsecret.png",
                "/binder-art/logos/300.png", "/binder-art/logos/200.jpg"):
        assert art_client.get(bad).status_code == 404


def test_listed_in_the_sitemap_and_footer(art_client):
    assert "https://shop.test/binder-art</loc>" in art_client.get("/sitemap.xml").text
    assert 'href="/binder-art"' in art_client.get("/").text


def test_collector_hub(art_client):
    # Moved from the market 2026-11-20 (app/collector_hub.py); its resources still open there.
    page = art_client.get("/collector-hub")
    assert page.status_code == 200 and "<h1>Collector Hub</h1>" in page.text
    assert "https://market.poke6s.com/collector-assets/checklists-sv/index.html" in page.text
    assert "https://market.poke6s.com/placeholder-downloads/combined-printable-csv" in page.text
    assert 'href="/binder-art"' in page.text and "<script>" not in page.text.split("<body>", 1)[1]
    assert "https://shop.test/collector-hub</loc>" in art_client.get("/sitemap.xml").text
    assert 'href="/collector-hub"' in art_client.get("/").text
