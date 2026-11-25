"""The admin's Pricing page (Phase 3, step 6): reprice.py's plan with a preview, applying only
ticked lines, skipping lines that changed since the page loaded, never showing costs. Synthetic
store from conftest.py: ENG-BB-BB is TCGplayer 630431 (market $100), JP-AE-BB is 900001 ($80)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from storefront.admin.main import create_admin_app
from storefront.app.db import connect

from storefront.tests.test_admin import HOME, admin, config, password_hash, sign_in  # noqa: F401
from storefront.tests.test_admin_orders import token

COST_SECRET = "12.34"   # a cost that must never appear on the page


@pytest.fixture()
def priced(imported, config, tmp_path):  # noqa: F811
    """The admin with a rules file and a private costs file in place."""
    (imported.data_dir.parent / "store_price_rules.csv").write_text(
        "sku,market_source,lookup_type,lookup_value,pricing_mode,min_price,note\n"
        "ENG-BB-BB,en,product_id,630431,market_minus_5_pct_99,,test\n"
        "JP-AE-BB,jp,product_id,900001,market,,test\n", encoding="utf-8")
    costs = tmp_path / "private" / "store_sku_costs.csv"
    costs.parent.mkdir()
    costs.write_text(f"sku,unit_cost,ship_label,packing\nENG-BB-BB,{COST_SECRET},10,1\nJP-AE-BB,90,10,1\n",
                     encoding="utf-8")
    config.costs_file = costs
    with TestClient(create_admin_app(imported, config), client=HOME, follow_redirects=False) as client:
        yield sign_in(client)


def price_of(settings, sku: str) -> int:
    with connect(settings.db_path) as conn:
        return conn.execute("SELECT price_cents FROM products WHERE sku = ?", (sku,)).fetchone()[0]


def set_price(settings, sku: str, cents: int) -> None:
    with connect(settings.db_path) as conn:
        conn.execute("UPDATE products SET price_cents = ?, visible = 1 WHERE sku = ?", (cents, sku))


def picks(page: str) -> list[str]:
    return [chunk.split('"')[0] for chunk in page.split('name="pick" value="')[1:]]


def test_pricing_needs_sign_in(admin, imported):  # noqa: F811
    assert admin.get("/pricing").headers["location"] == "/login"
    assert admin.post("/pricing/apply", data={"pick": "1:1:1"}).headers["location"] == "/login"


def test_missing_costs_file_says_so(admin, imported):  # noqa: F811
    page = sign_in(admin).get("/pricing").text   # the default config has no costs file
    assert "read the costs file" in page and "Apply the ticked" not in page


def test_preview_lists_changes_without_changing_anything(priced, imported):
    set_price(imported, "ENG-BB-BB", 12000)
    set_price(imported, "JP-AE-BB", 5000)
    page = priced.get("/pricing").text
    assert "$94.99" in page                      # 5% under $100, ending in .99
    assert "$93.99" in page and "raised to floor" in page   # $80 market, but cost 90 + fees
    assert COST_SECRET not in page               # costs are never shown
    assert price_of(imported, "ENG-BB-BB") == 12000 and price_of(imported, "JP-AE-BB") == 5000
    assert 'href="/pricing"' in page


def test_apply_only_ticked_lines_and_log_them(priced, imported):
    set_price(imported, "ENG-BB-BB", 12000)
    set_price(imported, "JP-AE-BB", 5000)
    page = priced.get("/pricing").text
    first = [p for p in picks(page) if p.endswith(":9499")]
    assert len(first) == 1 and len(picks(page)) == 2
    response = priced.post("/pricing/apply", data={"form_token": token(page), "pick": first})
    assert response.status_code == 303 and "n=1" in response.headers["location"]
    assert price_of(imported, "ENG-BB-BB") == 9499 and price_of(imported, "JP-AE-BB") == 5000
    assert "Applied 1 price change" in priced.get(response.headers["location"]).text
    with connect(imported.db_path) as conn:
        product_id, = conn.execute("SELECT id FROM products WHERE sku = 'ENG-BB-BB'").fetchone()
        log = conn.execute("SELECT field, old_value, new_value, source FROM product_changes WHERE product_id = ?",
                           (product_id,)).fetchall()
    assert ("price", "12000", "9499", "reprice") in [tuple(row) for row in log]
    assert "repricing" in priced.get(f"/products/{product_id}").text


def test_lines_changed_since_the_page_loaded_are_skipped(priced, imported):
    set_price(imported, "ENG-BB-BB", 12000)
    page = priced.get("/pricing").text
    line = [p for p in picks(page) if p.endswith(":9499")]
    set_price(imported, "ENG-BB-BB", 11000)   # changed on the product page meanwhile
    response = priced.post("/pricing/apply", data={"form_token": token(page), "pick": line})
    assert "n=0" in response.headers["location"] and "stale=1" in response.headers["location"]
    assert price_of(imported, "ENG-BB-BB") == 11000
    assert "left alone" in priced.get(response.headers["location"]).text


def test_made_up_lines_change_nothing(priced, imported):
    set_price(imported, "ENG-BB-BB", 12000)
    page = priced.get("/pricing").text
    with connect(imported.db_path) as conn:
        product_id, = conn.execute("SELECT id FROM products WHERE sku = 'ENG-BB-BB'").fetchone()
    response = priced.post("/pricing/apply", data={"form_token": token(page), "pick": [f"{product_id}:12000:100"]})
    assert "n=0" in response.headers["location"] and price_of(imported, "ENG-BB-BB") == 12000
    assert priced.post("/pricing/apply", data={"form_token": "nope", "pick": picks(page)}).status_code == 403
