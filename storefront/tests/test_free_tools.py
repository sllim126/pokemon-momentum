"""The free tools moved from market.poke6s.com to the store on 2026-11-20: Binder Art Maker
(/binder-art, app/binder_art.py: the page, its CSP, and the set logo list built from a logo
folder and the TCGCSV groups CSV, both temporary copies here), Collector Hub
(/collector-hub, app/collector_hub.py) and Placeholder Library (/placeholders,
app/placeholder_library.py, serving a temporary copy of the generated "TCG Placeholders" files)."""

from __future__ import annotations

import base64
import dataclasses
import hashlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from storefront.app import collector_hub, placeholder_library
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


CHECKLIST_SCRIPT = "localStorage.setItem('x', '1');"


def make_placeholders(tmp_path: Path) -> Path:
    """Every bucket page and download the pages link to, plus files that must stay unreachable."""
    root = tmp_path / "TCG Placeholders"
    for folder in placeholder_library.BUCKETS.values():
        (root / folder).mkdir(parents=True, exist_ok=True)
        (root / folder / "index.html").write_text(
            f"<html><style>p{{color:red}}</style><body><a href=\"/placeholders\">Back</a>"
            f"<script>{CHECKLIST_SCRIPT}</script></body></html>", encoding="utf-8")
    (root / "docs" / "set_release_dates.md").write_text("# Dates\n", encoding="utf-8")
    for rel in placeholder_library.DOWNLOADS.values():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)   # title_cards/ isn't a bucket
        (root / rel).write_text("Card Name,Card Number\nPikachu,MEP001\n", encoding="utf-8")
    (root / "output_combined" / "notes.py").write_text("secret", encoding="utf-8")   # unlisted type
    (root / "README.md").write_text("outside every bucket", encoding="utf-8")
    return root


@pytest.fixture()
def art_client(imported, tmp_path):
    logo_dir, groups = make_logos(tmp_path)
    settings = dataclasses.replace(imported, set_logo_dir=logo_dir, groups_csv=groups,
                                   placeholder_dir=make_placeholders(tmp_path))
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
    # Moved from the market 2026-11-20 (app/collector_hub.py); its files are the store's own.
    page = art_client.get("/collector-hub")
    assert page.status_code == 200 and "<h1>Collector Hub</h1>" in page.text
    assert 'href="/placeholders/files/checklists-sv/index.html"' in page.text
    assert 'href="/placeholders/download/combined-printable-csv"' in page.text
    assert "market.poke6s.com/collector-assets" not in page.text
    assert 'href="/binder-art"' in page.text and "<script>" not in page.text.split("<body>", 1)[1]
    assert "https://shop.test/collector-hub</loc>" in art_client.get("/sitemap.xml").text
    assert 'href="/collector-hub"' in art_client.get("/").text


def test_every_collector_hub_link_resolves(art_client):
    # Each tile's page and download must be among the library's files.
    for section in collector_hub.SECTIONS:
        for r in section.resources:
            bucket, rel = r.asset.split("/", 1)
            assert bucket in placeholder_library.BUCKETS, r.asset
            assert art_client.get(placeholder_library.resource_url(r.asset)).status_code == 200, r.asset
            if r.download:
                assert r.download[0] in placeholder_library.DOWNLOADS
                assert art_client.get(placeholder_library.download_url(r.download[0])).status_code == 200


def test_placeholder_library_page(art_client):
    page = art_client.get("/placeholders")
    assert page.status_code == 200 and "<h1>Placeholder Library</h1>" in page.text
    assert "Last rebuilt" in page.text and "<script>" not in page.text.split("<body>", 1)[1]
    for section in placeholder_library.SECTIONS:
        for item in section.items:
            href = (placeholder_library.resource_url(item.page) if item.page
                    else placeholder_library.download_url(item.download))
            assert f'href="{href}"' in page.text
            assert art_client.get(href).status_code == 200, href
    assert "https://shop.test/placeholders</loc>" in art_client.get("/sitemap.xml").text
    assert 'href="/placeholders"' in art_client.get("/").text


def test_generated_pages_get_a_csp_for_their_own_scripts_only(art_client):
    response = art_client.get("/placeholders/files/checklists-mega/index.html")
    assert response.status_code == 200 and response.headers["content-type"].startswith("text/html")
    csp = response.headers["content-security-policy"]
    digest = base64.b64encode(hashlib.sha256(CHECKLIST_SCRIPT.encode()).digest()).decode()
    assert f"script-src 'self' 'sha256-{digest}';" in csp
    assert "style-src 'self' 'unsafe-inline'" in csp and "frame-ancestors 'none'" in csp
    assert "unsafe-inline" not in csp.split("script-src", 1)[1].split(";", 1)[0]
    # The rest of the store keeps its strict policy.
    assert "sha256" not in art_client.get("/placeholders").headers["content-security-policy"]


def test_placeholder_files_are_whitelisted(art_client):
    md = art_client.get("/placeholders/files/docs/set_release_dates.md")
    assert md.status_code == 200 and md.headers["content-type"].startswith("text/plain")
    download = art_client.get("/placeholders/download/mega-auto-promos-csv")
    assert download.status_code == 200 and "attachment" in download.headers["content-disposition"]
    for title_cards in ("jtg-title-cards", "journey-together-title-cards"):   # hand-made, generated
        pdf = art_client.get(f"/placeholders/download/{title_cards}")
        assert pdf.status_code == 200 and pdf.headers["content-type"] == "application/pdf", title_cards
    for bad in ("/placeholders/files/nope/index.html", "/placeholders/files/print-combined/notes.py",
                "/placeholders/files/print-combined/..%2F..%2FREADME.md", "/placeholders/files/docs/missing.md",
                "/placeholders/download/not-real"):
        assert art_client.get(bad).status_code == 404, bad


def test_library_says_when_files_are_missing(imported, tmp_path):
    settings = dataclasses.replace(imported, placeholder_dir=tmp_path / "nowhere")
    with TestClient(create_app(settings)) as client:
        page = client.get("/placeholders")
        assert page.status_code == 200 and "aren't available right now" in page.text
        assert client.get("/placeholders/download/combined-json").status_code == 404
