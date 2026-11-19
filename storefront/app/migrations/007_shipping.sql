-- Phase 3 step 3 (admin: shipping through Pirate Ship).
-- 1. product_changes also logs a product's packed weight and box size ('packed_size'), which the
--    Pirate Ship export fills in. The size lives in the existing products columns weight_oz,
--    length_in, width_in and height_in (first filled by the Squarespace importer).
--    SQLite can't change a CHECK constraint in place, so the table is rebuilt with its rows kept
--    (same as migration 006).
-- 2. orders.labels_exported_at: when the order last went out in a Pirate Ship file, so the
--    Shipping page can warn before a second label is bought for the same order.
CREATE TABLE product_changes_new (
    id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    field TEXT NOT NULL CHECK (field IN (
        'price', 'compare_at', 'visible', 'featured', 'max_per_order',
        'created', 'title', 'description', 'product_type', 'language', 'set_name',
        'tcgplayer_ids', 'gtin', 'photo', 'packed_size')),
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

ALTER TABLE orders ADD COLUMN labels_exported_at TEXT;   -- UTC ISO-8601
