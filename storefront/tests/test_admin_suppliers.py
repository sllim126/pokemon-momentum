"""The admin's Supplier Profitability page (moved from the market site 2026-11-21;
storefront/admin/supplier_profit.py). Synthetic store from conftest.py: JP-AE-BB sells at $71.99.
The exchange rate comes from a fake fetch, so no test reaches Frankfurter."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from storefront.admin import supplier_profit
from storefront.admin.main import create_admin_app

from storefront.tests.test_admin import HOME, admin, config, password_hash, sign_in  # noqa: F401

QUOTES = (
    "quote_id,quote_date,supplier_name,source_name,source_type,item_name_raw,sku,cost_jpy,image_name,notes\n"
    "20260101T000000Z,2026-01-01,Old Supplier,a.png,screenshot,Abyss Eye,JP-AE-BB,12000,a.png,\n"
    "20260301T000000Z,2026-03-01,Test Supplier,b.png,screenshot,Abyss Eye,JP-AE-BB,9000,b.png,\n"
    "20260301T000000Z,2026-03-01,Test Supplier,b.png,screenshot,Cheap Box,JP-CHEAP-BB,1000,b.png,\n"
    "20260301T000000Z,2026-03-01,Test Supplier,b.png,screenshot,Mystery Item,,5000,b.png,\n"
)
MARKET = ("sku,market_price,target_price,profit_floor_price,supplier_cost_jpy,target_source,title,market_title,pricing_mode,market_source\n"
          "JP-AE-BB,80.00,85.00,,,market,Abyss Eye Booster Box,Abyss Eye,market,jp\n"
          "JP-CHEAP-BB,90.00,95.00,,,market,Cheap Booster Box,Cheap,market,jp\n")


def fake_fx(rate: float = 150.0):
    calls = []

    def fetch() -> str:
        calls.append(1)
        return json.dumps([{"base": "USD", "quote": "JPY", "rate": rate, "date": "2026-11-20"}])
    return supplier_profit.FxRate(fetch), calls


@pytest.fixture()
def suppliers(imported, config, tmp_path):  # noqa: F811
    quotes, market = tmp_path / "supplier_quotes.csv", tmp_path / "market_prices_latest.csv"
    quotes.write_text(QUOTES, encoding="utf-8")
    market.write_text(MARKET, encoding="utf-8")
    config.supplier_quotes_file, config.market_targets_file = quotes, market
    fx, calls = fake_fx()
    with TestClient(create_admin_app(imported, config, fx_rate=fx), client=HOME, follow_redirects=False) as client:
        client.fx_calls = calls
        yield sign_in(client)


def test_needs_sign_in(imported, config):  # noqa: F811
    with TestClient(create_admin_app(imported, config, fx_rate=fake_fx()[0]), client=HOME,
                    follow_redirects=False) as client:
        assert client.get("/supplier-profitability").headers["location"] == "/login"


def test_lists_the_newest_quote_per_sku_with_its_verdict(suppliers):
    page = suppliers.get("/supplier-profitability").text
    assert 'href="/supplier-profitability" aria-current="page"' in page   # the Suppliers tab
    assert "JPY/USD 150.0 from Frankfurter, 2026-11-20" in page
    # 9000 JPY at 150 = $60, +10% duty, 13800 JPY shipping / 11 boxes, $0.75 handling, $15 / 11 boxes.
    assert "9,000 JPY" in page and "12,000 JPY" not in page
    assert "$76.48" in page
    assert "Pokemon Abyss Eye Booster Box - Japanese" in page   # the store's own title wins
    assert "Store price: $71.99" in page
    assert "1 quote row(s) have no SKU yet" in page
    # The cheap box clears every floor, so it's a Buy and listed first.
    assert page.index("JP-CHEAP-BB") < page.index("JP-AE-BB")
    assert "Quotes (2)" in page


def test_rate_is_fetched_once_and_a_typed_rate_wins(suppliers):
    suppliers.get("/supplier-profitability")
    page = suppliers.get("/supplier-profitability", params={"jpy_per_usd": "300"}).text
    assert suppliers.fx_calls == [1]
    assert "Using your rate, 300 JPY/USD" in page
    # $30 + $3 duty + 13800/11/300 shipping + $0.75 + $15/11.
    assert "$39.30" in page


def test_assumptions_and_zero_stay_as_typed(suppliers):
    page = suppliers.get("/supplier-profitability", params={
        "inbound_shipping_mode": "manual", "inbound_shipping_usd": "0", "import_duty_pct": "0",
        "handling_cost_usd": "0", "disbursement_fee_usd": "0"}).text
    assert "$60.00" in page                      # landed = the box alone
    assert 'name="import_duty_pct" value="0"' in page


def test_filter_by_verdict_and_text(suppliers):
    page = suppliers.get("/supplier-profitability", params={"decision": "Buy"}).text
    assert "JP-CHEAP-BB" in page and "JP-AE-BB</span>" not in page and "Quotes (1 of 2)" in page
    page = suppliers.get("/supplier-profitability", params={"q": "abyss"}).text
    assert "JP-AE-BB" in page and "JP-CHEAP-BB" not in page


def test_quick_calculator(suppliers):
    page = suppliers.get("/supplier-profitability", params={
        "q_item": "Test box", "q_price": "60", "q_currency": "usd", "q_site_price": "200"}).text
    assert "Test box" in page and "$60.00" in page
    assert "At $200.00" in page


def test_missing_files_say_so(imported, config, tmp_path):  # noqa: F811
    config.supplier_quotes_file = tmp_path / "none.csv"
    config.market_targets_file = tmp_path / "none2.csv"
    with TestClient(create_admin_app(imported, config, fx_rate=fake_fx()[0]), client=HOME,
                    follow_redirects=False) as client:
        page = sign_in(client).get("/supplier-profitability").text
    assert "No supplier quotes with a SKU yet." in page


def test_channel_maths():
    a = supplier_profit.read_assumptions({"income_tax_pct": "25", "target_margin_pct": "15"}, 150.0)
    channel = supplier_profit.Channel("x", "X", "store", 10.0, 0.0, 0.0)
    result = supplier_profit.channel_result(channel, 100.0, 45.0, a)
    assert result["break_even"] == 50.0                    # 45 / (1 - 0.10)
    assert result["for_margin"] == 60.0                    # 45 / (1 - 0.10 - 0.15)
    assert result["profit_after_tax"] == 33.75             # (90 - 45) * 0.75
    assert result["decision"] == "Buy"
    losing = supplier_profit.channel_result(channel, 40.0, 45.0, a)
    assert losing["profit_after_tax"] == -9.0 and losing["decision"] == "Pass"   # no tax on a loss
    assert supplier_profit.channel_result(channel, 55.0, 45.0, a)["decision"] == "Thin margin"
    assert supplier_profit.channel_result(channel, None, 45.0, a)["decision"] == "No ref price"


def test_fx_rate_keeps_the_last_good_rate(monkeypatch):
    answers = [json.dumps([{"base": "USD", "quote": "JPY", "rate": 151.5, "date": "d"}])]

    def fetch() -> str:
        if not answers:
            raise OSError("offline")
        return answers.pop()
    fx = supplier_profit.FxRate(fetch)
    assert fx.get() == {"rate": 151.5, "date": "d", "stale": False}
    clock = [supplier_profit.time.monotonic() + supplier_profit.FX_CACHE_SECONDS + 1]
    monkeypatch.setattr(supplier_profit.time, "monotonic", lambda: clock[0])
    assert fx.get() == {"rate": 151.5, "date": "d", "stale": True}
    assert supplier_profit.FxRate(lambda: "not json").get() is None


def test_lower_case_skus_are_the_same_product(tmp_path):
    # The old market page saved some SKUs in lower case; the newest quote wins either way.
    path = tmp_path / "supplier_quotes.csv"
    path.write_text("quote_id,quote_date,supplier_name,source_name,source_type,item_name_raw,sku,cost_jpy,image_name,notes\n"
                    "A,2026-04-22,Facebook Supplier,,screenshot,e Battle Partners,jp-bp-box,11200,,\n"
                    "B,2026-11-22,Macho TCG,,shop page,Battle partners,JP-BP-BOX,10500,,\n"
                    "C,2026-04-18,Facebook Supplier,,screenshot,Inferno X,JP-INFx-BOX,29800,,\n", encoding="utf-8")
    quotes, unmatched = supplier_profit.latest_quotes(path)
    assert [(q["sku"], q["cost_jpy"]) for q in quotes] == [("JP-BP-BOX", "10500"), ("JP-INFX-BOX", "29800")]
    assert unmatched == 0
