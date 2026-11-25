-- Oversize shipping (owner, 2026-11-25): every class ships for $10 except big boxes such as the
-- Costco bundles and premium collections, which the owner marks "oversize" in the admin's
-- Details panel ($20, STOREFRONT_SHIP_OVERSIZE_CENTS). products.ship_class is plain text, so the
-- new value needs no change there; product_changes must allow the 'ship_class' field so the
-- switch shows in the product's history. SQLite can't change a CHECK constraint in place: rebuilt
-- with its rows kept (the list is 011's, plus ship_class).
CREATE TABLE product_changes_new (
    id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    field TEXT NOT NULL CHECK (field IN (
        'price', 'compare_at', 'visible', 'featured', 'max_per_order', 'price_manual',
        'created', 'title', 'description', 'product_type', 'language', 'set_name',
        'tcgplayer_ids', 'gtin', 'photo', 'packed_size', 'ship_class')),
    old_value TEXT,
    new_value TEXT,
    reason TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL CHECK (source IN ('admin', 'reprice')),
    at TEXT NOT NULL
);
INSERT INTO product_changes_new (id, product_id, field, old_value, new_value, reason, source, at)
    SELECT id, product_id, field, old_value, new_value, reason, source, at FROM product_changes;
DROP TABLE product_changes;
ALTER TABLE product_changes_new RENAME TO product_changes;
CREATE INDEX product_changes_product ON product_changes (product_id, at);
