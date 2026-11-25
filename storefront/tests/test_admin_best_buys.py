"""The admin's Best buys page (storefront/admin/best_buys.py): every buy-and-sell route per box,
fast sellers first. Uses Supplier Profitability's fixtures (fake exchange rate, quote and market files)."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from storefront.admin import best_buys, supplier_profit
from storefront.admin.main import create_admin_app
from storefront.tests.test_admin import HOME, admin, config, password_hash, sign_in  # noqa: F401
from storefront.tests.test_admin_orders import token
from storefront.tests.test_admin_suppliers import MARKET, QUOTES, fake_fx

RATES = {"single": 1000, "accessory": 1000, "small-sealed": 1000, "large-sealed": 1000, "oversize": 2000}


@pytest.fixture()
def best(imported, config, tmp_path):  # noqa: F811
    quotes, market = tmp_path / "supplier_quotes.csv", tmp_path / "market_prices_latest.csv"
    quotes.write_text(QUOTES, encoding="utf-8")
    market.write_text(MARKET, encoding="utf-8")
    config.supplier_quotes_file, config.market_targets_file = quotes, market
    config.tcgplayer_sales_file = tmp_path / "tcgplayer_sales.csv"
    with TestClient(create_admin_app(imported, config, fx_rate=fake_fx()[0]), client=HOME,
                    follow_redirects=False) as client:
        client.sales_file = config.tcgplayer_sales_file
        yield sign_in(client)


def test_needs_sign_in(imported, config):  # noqa: F811
    with TestClient(create_admin_app(imported, config, fx_rate=fake_fx()[0]), client=HOME,
                    follow_redirects=False) as client:
        assert client.get("/supplier-best-buys").headers["location"] == "/login"


def test_buyer_shipping_follows_the_store_rates():
    assert best_buys.buyer_shipping(76.99, "large-sealed", RATES, 15000) == 10.0
    assert best_buys.buyer_shipping(142.99, "oversize", RATES, 15000) == 20.0
    assert best_buys.buyer_shipping(150.00, "large-sealed", RATES, 15000) == 0.0   # free over $150


@pytest.mark.parametrize("speed, profit, falling, stock, qty", [
    ("Fast", 12.0, False, 0, 4),        # the owner's standard order: 2-4 of each fast seller
    ("Fast", 12.0, True, 0, 2),         # falling market: half
    ("Fast", 12.0, False, 3, 1),        # less what's in stock
    ("Steady", 5.0, False, 0, 2),
    ("Slow", 5.0, True, 0, 1),
    ("Fast", -1.0, False, 0, 0),        # never a box that loses money on every route
    ("", 9.0, False, 0, 0),             # no sales numbers, no suggestion
])
def test_suggested_order(speed, profit, falling, stock, qty):
    assert best_buys.suggested_order(speed, profit, falling, stock)[0] == qty


def test_routes_compare_both_sources_on_every_channel():
    a = supplier_profit.read_assumptions({}, 150.0)
    buy = best_buys.read_buy_assumptions({})
    quotes = [{"sku": "JP-X", "cost_jpy": "9000", "supplier_name": "MACHO", "quote_date": "2026-11-22"}]
    market = {"JP-X": {"market_price": "100.00", "target_price": "110.00", "title": "X"}}
    store = {"JP-X": {"title": "X box", "price": 120.0, "stock": 0, "ship_class": "large-sealed", "tcgplayer_ids": ""}}
    sales = {"JP-X": {"entered_at": "2026-11-25T00:00:00Z", "sold_per_day": 5.0, "listed": 200.0, "trend_3m_pct": -5.0}}
    rows = best_buys.build(quotes, market, store, sales, lambda ids: None, a, buy, RATES, 15000)
    row = rows[0]
    assert len(row["routes"]) == 6                                 # 2 sources x 3 channels
    assert row["import_cost"] == supplier_profit.money(supplier_profit.landed_cost(60.0, a))
    assert row["tcg_cost"] == 107.45                               # $100 market + 7.45% Utah tax
    assert row["best"]["source"] == "import" and row["best"]["channel"] == "site"
    assert row["best"]["buyer_shipping"] == 10.0
    assert row["days_of_supply"] == 40 and row["speed"] == "Fast" and row["order_qty"] == 4
    assert row["elsewhere"]["channel"] in ("ebay", "tcgplayer")     # the best quick marketplace sale
    profits = [r["profit"] for r in row["routes"]]
    assert profits == sorted(profits, reverse=True)


def test_page_lists_fast_sellers_first_and_saves_sales(best):
    page = best.get("/supplier-best-buys").text
    assert 'href="/supplier-best-buys" aria-current="page"' in page and "Not entered" in page
    response = best.post("/supplier-best-buys/sales", data={"form_token": token(page), "sku": "jp-ae-bb",
                                                             "sold_per_day": "5", "listed": "325", "trend_3m_pct": "-11"})
    assert response.status_code == 303 and response.headers["location"].startswith("/supplier-best-buys?done=sales")
    assert "JP-AE-BB,5,325,-11" in Path(best.sales_file).read_text(encoding="utf-8")
    page = best.get("/supplier-best-buys").text
    assert "5/day" in page and "65 days listed" in page
    # Abyss Eye has sales numbers, so it comes before the box without them.
    assert page.index("JP-AE-BB") < page.index("JP-CHEAP-BB")


def test_bad_sales_numbers_are_refused(best):
    page = best.get("/supplier-best-buys").text
    for data, message in (({"sku": "NOPE"}, "Pick a product"), ({"sold_per_day": "lots"}, "Sold per day is a number"),
                          ({"listed": "x"}, "Listed is a whole number")):
        form = {"form_token": token(page), "sku": "JP-AE-BB", "sold_per_day": "2", **data}
        response = best.post("/supplier-best-buys/sales", data=form)
        assert response.status_code == 400 and message in response.text, data
    assert not Path(best.sales_file).exists()
    assert best.post("/supplier-best-buys/sales", data={"form_token": "wrong", "sku": "JP-AE-BB",
                                                         "sold_per_day": "2"}).status_code == 403
