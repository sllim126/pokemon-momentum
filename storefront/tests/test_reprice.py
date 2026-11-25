"""storefront/tools/reprice.py: the floors the nightly run must never go under (owner,
2026-11-25: automatic repricing is fine "so long as it doesn't dip below my profitability
amount"), and the stale-market guard."""

from __future__ import annotations

from datetime import date

from storefront.tools import reprice

COST = {"unit_cost": 20.0, "ship_label": 8.0, "packing": 1.0}


def product(sku: str, now_cents: int, tcg_id: str = "1") -> dict:
    return {"id": 1, "sku": sku, "title": sku, "price_cents": now_cents, "compare_at_cents": None,
            "tcgplayer_product_ids": tcg_id, "visible": 1, "stock": 3, "track_stock": 1}


def test_min_price_is_a_floor_even_above_break_even():
    # Market $25 -> rule $22.99; break-even for a $20 cost is ~$21.29 -> $21.99; min_price $29.99 wins.
    result = reprice.plan([product("A", 3499)], {"A": COST}, {"products": {"1": {"price": 25}}},
                          {"A": "market_minus_5_pct_99_with_floor"}, min_prices={"A": 29.99})
    item = result["priced"][0]
    assert item["new"] == 2999 and item["raised"]


def test_break_even_still_applies_without_min_price():
    result = reprice.plan([product("A", 3499)], {"A": COST}, {"products": {"1": {"price": 10}}},
                          {"A": "market"})
    item = result["priced"][0]
    assert item["new"] >= 2129 and item["new"] % 100 == 99 and item["raised"]


def test_no_cost_means_no_change():
    result = reprice.plan([product("A", 3499)], {}, {"products": {"1": {"price": 10}}}, {"A": "market"},
                          min_prices={"A": 5})
    assert not result["priced"] and result["skipped"][0]["reason"] == "no cost on file"


def test_min_prices_read_from_the_rules_file(tmp_path):
    rules = tmp_path / "rules.csv"
    rules.write_text("sku,market_source,lookup_type,lookup_value,pricing_mode,min_price,note\n"
                     "A,en,id,1,market_minus_5_pct_99_with_floor,29.99,x\nB,en,id,2,market,,y\n", encoding="utf-8")
    assert reprice.load_min_prices(rules) == {"A": 29.99}
    assert reprice.load_rules(rules) == {"A": "market_minus_5_pct_99_with_floor", "B": "market"}


def test_market_age():
    today = date(2026, 11, 25)
    assert reprice.market_age_days("2026-11-24", today) == 1
    assert reprice.market_age_days(None, today) is None
    assert reprice.market_age_days("garbage", today) is None


def test_priced_by_hand_is_left_alone():
    # Owner ticked "Keep this price" in the admin (products.price_manual, migration 011).
    row = {**product("A", 3499), "price_manual": 1}
    result = reprice.plan([row], {"A": COST}, {"products": {"1": {"price": 25}}}, {"A": "market"})
    assert not result["priced"] and result["skipped"][0]["reason"] == "priced by hand"


def test_big_changes_are_held():
    assert reprice.too_big({"now": 10000, "new": 13100}, 30)        # +31%
    assert reprice.too_big({"now": 10000, "new": 6900}, 30)         # -31%
    assert not reprice.too_big({"now": 10000, "new": 12999}, 30)
    assert not reprice.too_big({"now": 0, "new": 999}, 30)          # no price yet: nothing to compare


def test_old_database_without_the_switch(tmp_path):
    import sqlite3
    con = sqlite3.connect(tmp_path / "old.db")
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE products (id INTEGER, sku TEXT, title TEXT, price_cents INTEGER, compare_at_cents INTEGER, "
                "tcgplayer_product_ids TEXT, visible INTEGER, stock INTEGER, track_stock INTEGER)")
    con.execute("INSERT INTO products VALUES (1, 'A', 'A', 999, NULL, '1', 1, 1, 1)")
    assert reprice.load_products(con)[0]["price_manual"] == 0
