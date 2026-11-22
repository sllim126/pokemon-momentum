"""The admin's Supplier Pricing page (moved from the market site 2026-11-22;
storefront/admin/supplier_quotes.py). Synthetic store from conftest.py: JP-AE-BB sells at $71.99."""

from __future__ import annotations

import csv
import re

import pytest
from fastapi.testclient import TestClient

from storefront.admin import supplier_quotes
from storefront.admin.main import create_admin_app

from storefront.tests.test_admin import HOME, admin, config, password_hash, sign_in  # noqa: F401
from storefront.tests.test_admin_suppliers import MARKET, fake_fx

MAPPING = "supplier_name_raw,sku,note\nAbyss Eye,JP-AE-BB,Japanese booster box\n"


@pytest.fixture()
def quotes(imported, config, tmp_path):  # noqa: F811
    market = tmp_path / "market_prices_latest.csv"
    market.write_text(MARKET, encoding="utf-8")
    (tmp_path / "supplier_name_mapping.csv").write_text(MAPPING, encoding="utf-8")
    config.supplier_quotes_file = tmp_path / "supplier_quotes.csv"
    config.supplier_mapping_file = tmp_path / "supplier_name_mapping.csv"
    config.market_targets_file = market
    with TestClient(create_admin_app(imported, config, fx_rate=fake_fx()[0]), client=HOME,
                    follow_redirects=False) as client:
        client.files = tmp_path
        yield sign_in(client)


def token(client) -> str:
    return re.search(r'name="form_token" value="([^"]+)"', client.get("/supplier-pricing").text).group(1)


def ledger(client) -> list[dict]:
    with (client.files / "supplier_quotes.csv").open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_parse_text_reads_names_and_yen():
    rows = supplier_quotes.parse_text("Prices today\n• Abyss Eye ¥12,800\nNihil Zero | 9800\nMega Dream JPY 15000\n12\n")
    assert [(r["item"], r["cost"]) for r in rows] == [("Abyss Eye", "12800"), ("Nihil Zero", "9800"),
                                                     ("Mega Dream", "15000")]


def test_needs_sign_in(imported, config):  # noqa: F811
    with TestClient(create_admin_app(imported, config, fx_rate=fake_fx()[0]), client=HOME,
                    follow_redirects=False) as client:
        assert client.get("/supplier-pricing").headers["location"] == "/login"
        assert client.post("/supplier-pricing", data={"action": "read"}).headers["location"] == "/login"


def test_page_and_tabs(quotes):
    page = quotes.get("/supplier-pricing").text
    assert 'aria-current="page">Suppliers' in page and 'aria-current="page">Profitability' not in page
    assert 'href="/supplier-pricing" aria-current="page">Enter a quote' in page
    assert "No quotes saved yet." in page


def test_expired_form_is_refused(quotes):
    assert quotes.post("/supplier-pricing", data={"form_token": "nope", "action": "read"}).status_code == 403


def test_read_matches_from_the_list_and_shows_prices(quotes):
    page = quotes.post("/supplier-pricing", data={"form_token": token(quotes), "action": "read",
                                                  "text": "Abyss Eye ¥9,000\nUnknown Box 5000"}).text
    assert 'value="JP-AE-BB"' in page and "From the list" in page
    assert "$71.99" in page and "$80.00" in page          # store price, market price
    assert "No SKU" in page
    assert not (quotes.files / "supplier_quotes.csv").exists()   # reading saves nothing


def test_read_with_nothing_usable_says_so(quotes):
    response = quotes.post("/supplier-pricing", data={"form_token": token(quotes), "action": "read", "text": "hello"})
    assert response.status_code == 400 and "No rows found" in response.text


def test_check_flags_bad_and_new_skus(quotes):
    page = quotes.post("/supplier-pricing", data={
        "form_token": token(quotes), "action": "check",
        "item": ["Abyss Eye", "Odd Box", "Mystery", ""], "cost": ["9,000", "abc", "5000", ""],
        "sku": ["", "", "jp-new-bb", ""]}).text
    assert "Fix name or price" in page and "New SKU" in page and 'value="JP-NEW-BB"' in page
    assert page.count('name="item"') == 4                 # three rows plus the spare blank one


def test_save_refuses_bad_rows(quotes):
    response = quotes.post("/supplier-pricing", data={
        "form_token": token(quotes), "action": "save", "quote_date": "2026-11-22",
        "item": ["Abyss Eye"], "cost": ["lots"], "sku": [""]})
    assert response.status_code == 400 and "need a name and a whole yen price" in response.text
    assert not (quotes.files / "supplier_quotes.csv").exists()


def test_save_appends_the_quote_and_remembers_typed_matches(quotes):
    response = quotes.post("/supplier-pricing", data={
        "form_token": token(quotes), "action": "save", "supplier": "Test Supplier", "quote_date": "2026-11-22",
        "source": "list.png", "remember": "1",
        "item": ["Abyss Eye", "Cheap Box", "Unknown", ""], "cost": ["9,000", "1000", "500", ""],
        "sku": ["", "jp-cheap-bb", "", ""]})
    assert response.status_code == 303 and response.headers["location"].startswith("/supplier-pricing?saved=")
    rows = ledger(quotes)
    assert [(r["item_name_raw"], r["sku"], r["cost_jpy"]) for r in rows] == [
        ("Abyss Eye", "JP-AE-BB", "9000"), ("Cheap Box", "JP-CHEAP-BB", "1000"), ("Unknown", "", "500")]
    assert {r["supplier_name"] for r in rows} == {"Test Supplier"} and rows[0]["source_type"] == "screenshot"
    assert len({r["quote_id"] for r in rows}) == 1
    # Only the typed match is new; Abyss Eye was already in the list.
    assert supplier_quotes.load_mapping(quotes.files / "supplier_name_mapping.csv") == {
        "abyss eye": "JP-AE-BB", "cheap box": "JP-CHEAP-BB"}
    page = quotes.get(response.headers["location"]).text
    assert "3 row(s), 2 with a SKU. 1 new name-to-SKU match(es) remembered." in page
    assert "Test Supplier" in page                       # in Recent quotes
    # Supplier Profitability now sees the saved quote.
    assert "9,000 JPY" in quotes.get("/supplier-profitability").text


def test_save_without_remember_leaves_the_list_alone(quotes):
    quotes.post("/supplier-pricing", data={"form_token": token(quotes), "action": "save", "quote_date": "2026-11-22",
                                           "item": ["Cheap Box"], "cost": ["1000"], "sku": ["JP-CHEAP-BB"]})
    assert len(ledger(quotes)) == 1
    assert "cheap box" not in supplier_quotes.load_mapping(quotes.files / "supplier_name_mapping.csv")


def test_save_needs_a_date(quotes):
    response = quotes.post("/supplier-pricing", data={"form_token": token(quotes), "action": "save", "quote_date": "",
                                                      "item": ["Cheap Box"], "cost": ["1000"], "sku": [""]})
    assert response.status_code == 400 and "Pick the quote" in response.text


# --- a whole shop page pasted (MACHO TCG's shape, cut down; owner 2026-11-22) --------------------

SHOP_PAGE = """MACHO TCG
TOKYO · WHOLESALE
MIN. ORDER ¥30,000
Grid
Price: All
PRODUCT
SEALED (SHRINK)
NO SHRINK
CARTON
PREORDER
MEGA Hadou Seeker Box
ハドウシーカー
M7
SEALED (SHRINK)
Est. ship Nov 30
¥10,000
≈ 63.03 USD
−
+
Est. ship Dec 1
¥9,600
≈ 60.51 USD
NO SHRINK
Est. ship Nov 30
¥9,800
≈ 61.77 USD
Pokemon · Booster Boxes
3 products
HOT
Abyss Eye
アビスアイ
M5JAN 4521329462127
SEALED (SHRINK)
¥8,000
≈ 50.43 USD
6 in stock
−
+
NO SHRINK
¥7,100
≈ 44.75 USD
Ask us
LOOSE PACKS
¥110
≈ 0.69 USD
899 in stock
Sold in units of 10
151
ポケモンカード151
SV2aJAN 4521329346038
SEALED (SHRINK)
¥49,500
≈ 312.02 USD
5 in stock
CARTON
¥575,000
≈ 3,624.46 USD
Ask us
On request — confirmed before you pay.
WhatsApp
Notify me
Dark Rush
ダークラッシュ
ALC-G85
LOOSE PACKS
¥900
≈ 5.67 USD
Ask us
MEGA Hadou Seeker Box
ハドウシーカー
M7
SEALED (SHRINK)
Est. ship Nov 30
¥10,000
Your order
Product subtotal
¥0
Total
¥0
"""


def test_shop_page_is_read_product_by_product():
    rows = supplier_quotes.parse_text(SHOP_PAGE)
    # Packs-only Dark Rush is left out; the pre-order listed twice is kept once; the cart's totals
    # and the column-heading row above the list aren't products.
    assert [(r["item"], r["cost"]) for r in rows] == [("MEGA Hadou Seeker Box", "10000"), ("Abyss Eye", "8000"),
                                                       ("151", "49500")]
    assert rows[0]["note"] == "M7; sealed (shrink), est. ship nov 30; also no shrink ¥9,800"
    assert rows[1]["note"] == ("M5 JAN 4521329462127; sealed (shrink), 6 in stock; "
                               "also no shrink ¥7,100, loose packs ¥110")
    assert rows[2]["note"] == "SV2a JAN 4521329346038; sealed (shrink), 5 in stock; also carton ¥575,000"
    assert supplier_quotes.shop_name(SHOP_PAGE) == "MACHO TCG (ALC Japan)"
    # A plain list still reads one line at a time, with no notes.
    assert supplier_quotes.parse_text("Mega Dream ¥12,800\nNihil Zero 9,800") == [
        {"item": "Mega Dream", "cost": "12800", "sku": "", "note": ""},
        {"item": "Nihil Zero", "cost": "9800", "sku": "", "note": ""}]


def test_shop_page_saves_only_rows_with_a_sku(quotes):
    page = quotes.post("/supplier-pricing", data={"form_token": token(quotes), "action": "read",
                                                  "supplier": supplier_quotes.DEFAULT_SUPPLIER,
                                                  "quote_date": "2026-11-22", "text": SHOP_PAGE}).text
    assert 'value="MACHO TCG (ALC Japan)"' in page
    assert 'name="only_sku" value="1" checked' in page
    assert 'name="note" value="M5 JAN 4521329462127;' in page and "also no shrink ¥7,100" in page
    response = quotes.post("/supplier-pricing", data={
        "form_token": token(quotes), "action": "save", "supplier": "MACHO TCG (ALC Japan)",
        "quote_date": "2026-11-22", "only_sku": "1", "remember": "1",
        "item": ["MEGA Hadou Seeker Box", "Abyss Eye", "151"], "cost": ["10000", "8000", "49500"],
        "sku": ["", "", ""], "note": ["M7; sealed (shrink)", "M5 JAN 4521329462127; sealed (shrink)", "SV2a"]})
    assert response.status_code == 303 and "n=1" in response.headers["location"]
    saved = ledger(quotes)
    assert [(r["item_name_raw"], r["sku"], r["cost_jpy"], r["source_type"]) for r in saved] == [
        ("Abyss Eye", "JP-AE-BB", "8000", "shop page")]
    assert saved[0]["notes"] == "saved in the store admin; M5 JAN 4521329462127; sealed (shrink)"
    # Nothing with a SKU: a message, not an empty quote.
    refused = quotes.post("/supplier-pricing", data={
        "form_token": token(quotes), "action": "save", "quote_date": "2026-11-22", "only_sku": "1",
        "item": ["151"], "cost": ["49500"], "sku": [""], "note": ["SV2a"]})
    assert refused.status_code == 400 and "No row has a SKU yet" in refused.text


def test_screenshot_reader_is_served_with_wasm_allowed(quotes):
    page = quotes.get("/supplier-pricing").text
    assert '<p class="sp-ocr" hidden>' in page and 'src="/static/supplier-ocr.js"' in page
    assert "'wasm-unsafe-eval'" not in quotes.get("/supplier-pricing").headers["content-security-policy"]
    for name in ("tesseract.min.js", "worker.min.js", "tesseract-core-simd-lstm.wasm.js", "eng.traineddata.gz"):
        response = quotes.get(f"/static/vendor/tesseract/{name}")
        assert response.status_code == 200, name
        assert "script-src 'self' 'wasm-unsafe-eval'" in response.headers["content-security-policy"]
    assert "'wasm-unsafe-eval'" not in quotes.get("/static/admin.css").headers["content-security-policy"]
