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
