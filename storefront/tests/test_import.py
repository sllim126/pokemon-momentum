"""Importer: classification, visibility, prices, variants, stock movements, re-runs."""

from __future__ import annotations

import sqlite3

from storefront.app.db import connect

from .conftest import run_import


def products(settings) -> dict[str, sqlite3.Row]:
    conn = connect(settings.db_path)
    try:
        return {r["sku"]: r for r in conn.execute("SELECT * FROM products")}
    finally:
        conn.close()


def test_import_counts_and_skips_singles(settings):
    report = run_import(settings)
    rows = products(settings)
    assert "602682" not in rows  # single: owner decision D1
    assert report.created == len(rows) == 9
    assert any("602682" in s for s in report.skipped)


def test_include_singles_imports_them_hidden(settings):
    run_import(settings, include_singles=True)
    single = products(settings)["602682"]
    assert single["kind"] == "single" and single["visible"] == 0
    assert single["tcgplayer_product_ids"] == "602682"


def test_classification(imported):
    rows = products(imported)
    assert rows["JP-AE-BB"]["product_type"] == "booster-box"
    assert rows["JP-AE-BB"]["language"] == "ja"
    assert rows["JP-AE-BB"]["ship_class"] == "large-sealed"
    assert rows["ENG-BB-BB"]["product_type"] == "booster-bundle"
    assert rows["ENG-BB-BB"]["language"] == "en"
    assert rows["ENG-G25-TIN"]["product_type"] == "tin"  # from the intake CSV
    # "Binders & Bulk" in an accessory title doesn't make it a bulk lot.
    assert rows["111"]["kind"] == "accessory" and rows["111"]["language"] is None
    assert rows["ENG-SS-ETB"]["product_type"] == "elite-trainer-box"


def test_set_names_from_market_reference_and_categories(imported):
    rows = products(imported)
    assert (rows["JP-AE-BB"]["set_code"], rows["JP-AE-BB"]["set_name"]) == ("m5", "Abyss Eye")
    assert rows["ENG-BB-BB"]["set_slug"] == "black-bolt"
    # A catch-all TCGplayer group is not a set.
    assert rows["ENG-G25-TIN"]["set_name"] is None


def test_prices_sale_and_cents(imported):
    rows = products(imported)
    assert rows["JP-AE-BB"]["price_cents"] == 7199 and rows["JP-AE-BB"]["compare_at_cents"] is None
    assert rows["ENG-BB-BB"]["price_cents"] == 7999 and rows["ENG-BB-BB"]["compare_at_cents"] == 9399


def test_visibility_rules(imported):
    rows = products(imported)
    assert rows["JP-AE-BB"]["visible"] == 1
    assert rows["JP-BB-BOX"]["visible"] == 0     # visible on Squarespace but out of stock
    assert rows["ENG-SS-ETB"]["visible"] == 0    # hidden on Squarespace
    assert rows["111"]["visible"] == 1           # "Unlimited" stock: untracked, so shown
    assert rows["111"]["track_stock"] == 0


def test_variants_become_products(imported):
    rows = products(imported)
    assert rows["111"]["title"].endswith("31.5\" × 15.5\"")
    assert rows["222"]["title"].startswith("TCG Desk Mat") and rows["222"]["title"].endswith("13.8\"")
    # Variant rows carry no Visible flag of their own; they follow the parent product.
    assert rows["222"]["visible"] == 1


def test_description_html_becomes_markdown(imported):
    md = products(imported)["JP-AE-BB"]["description_md"]
    assert "**30 packs**" in md and "<" not in md


def test_tcgplayer_ids_from_mapping_and_intake(imported):
    rows = products(imported)
    assert rows["ENG-BB-BB"]["tcgplayer_product_ids"] == "630431"
    assert rows["ENG-G25-TIN"]["tcgplayer_product_ids"] == "688965|688966"


def test_stock_equals_sum_of_movements(imported):
    # Re-import after changing nothing, then check the invariant from the plan's data model.
    run_import(imported)
    conn = connect(imported.db_path)
    try:
        bad = conn.execute(
            "SELECT p.sku FROM products p WHERE p.stock != "
            "(SELECT coalesce(sum(change), 0) FROM stock_movements m WHERE m.product_id = p.id)"
        ).fetchall()
        moves = conn.execute("SELECT count(*) FROM stock_movements").fetchone()[0]
    finally:
        conn.close()
    assert bad == []
    assert moves == 6  # one per tracked product with stock; the re-run added none


def test_reimport_updates_but_keeps_slug(imported):
    conn = connect(imported.db_path)
    conn.execute("UPDATE products SET slug = 'renamed', price_cents = 1 WHERE sku = 'JP-AE-BB'")
    conn.commit()
    conn.close()
    report = run_import(imported)
    row = products(imported)["JP-AE-BB"]
    assert report.created == 0 and report.updated == 9
    assert row["slug"] == "renamed" and row["price_cents"] == 7199


def test_only_new_leaves_existing_alone(imported):
    conn = connect(imported.db_path)
    conn.execute("UPDATE products SET price_cents = 1 WHERE sku = 'JP-AE-BB'")
    conn.commit()
    conn.close()
    run_import(imported, only_new=True)
    assert products(imported)["JP-AE-BB"]["price_cents"] == 1


def test_dry_run_writes_nothing(settings):
    report = run_import(settings, dry_run=True)
    assert report.created == 9
    assert products(settings) == {}
