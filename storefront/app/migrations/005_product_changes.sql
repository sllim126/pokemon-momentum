-- Phase 3 step 5 (admin: products and stock). Every change to a product's price or visibility,
-- with who made it and why, so "why is this $10 cheaper than last week?" has an answer.
-- Stock changes are not here: they stay in stock_movements, whose sum must equal
-- products.stock. Written by storefront/app/product_changes.py (admin) and
-- storefront/tools/reprice.py (source 'reprice').
CREATE TABLE product_changes (
    id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    field TEXT NOT NULL CHECK (field IN ('price', 'compare_at', 'visible', 'featured', 'max_per_order')),
    old_value TEXT,               -- cents for prices, '0'/'1' for flags, NULL for "not set"
    new_value TEXT,
    reason TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL CHECK (source IN ('admin', 'reprice')),
    at TEXT NOT NULL
);
CREATE INDEX product_changes_product ON product_changes (product_id, at);
