-- Nightly repricing safeguards (owner, 2026-11-25): a product can be "priced by hand", and the
-- nightly run (storefront/tools/reprice.py, scripts/pipeline/run_store_reprice_host.sh) leaves
-- its price alone. Set from the admin's price form (ticked by default when the owner types a
-- price, so the next night can't undo it); untick it to hand the product back to the rules.
ALTER TABLE products ADD COLUMN price_manual INTEGER NOT NULL DEFAULT 0;

-- The switch is logged in the product's history, so product_changes (migration 006) must allow
-- the field (the list is 007's, plus price_manual). SQLite can't change a CHECK constraint in
-- place: rebuilt with its rows kept.
CREATE TABLE product_changes_new (
    id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    field TEXT NOT NULL CHECK (field IN (
        'price', 'compare_at', 'visible', 'featured', 'max_per_order', 'price_manual',
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
