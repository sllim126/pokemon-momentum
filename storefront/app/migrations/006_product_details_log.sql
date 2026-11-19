-- Phase 3 step 5b (admin: add products, edit details, photos). product_changes (migration 005)
-- only allowed price and visibility fields; it now also logs a product being created, its
-- details (title, description, type, language, set, TCGplayer IDs, barcode) and photos.
-- SQLite can't change a CHECK constraint in place, so the table is rebuilt with its rows kept.
CREATE TABLE product_changes_new (
    id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    field TEXT NOT NULL CHECK (field IN (
        'price', 'compare_at', 'visible', 'featured', 'max_per_order',
        'created', 'title', 'description', 'product_type', 'language', 'set_name',
        'tcgplayer_ids', 'gtin', 'photo')),
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
