-- Phase 1 catalog schema. Data model: docs/storefront_plan.md "Data model (SQLite)".
-- Prices are in cents; times are UTC ISO-8601 strings ("2026-10-31T12:00:00Z").
-- Orders, order items and webhook events arrive in Phase 2 as a new migration.

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    sku TEXT NOT NULL UNIQUE,
    slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    -- sealed | single | accessory | bulk
    kind TEXT NOT NULL CHECK (kind IN ('sealed', 'single', 'accessory', 'bulk')),
    -- booster-box, booster-bundle, booster-pack, elite-trainer-box, collection-box, tin,
    -- single, accessory, other. Drives the category and the shipping class.
    product_type TEXT NOT NULL DEFAULT 'other',
    language TEXT,                -- 'en', 'ja' or NULL (accessories)
    set_code TEXT,                -- TCGplayer group code where known, e.g. 'sv11b'
    set_name TEXT,                -- 'Black Bolt'
    set_slug TEXT,                -- 'black-bolt'; EN and JP printings share one set page
    card_number TEXT,
    rarity TEXT,
    finish TEXT,
    condition TEXT,               -- singles only
    description_md TEXT NOT NULL DEFAULT '',
    price_cents INTEGER NOT NULL DEFAULT 0 CHECK (price_cents >= 0),
    compare_at_cents INTEGER CHECK (compare_at_cents IS NULL OR compare_at_cents >= 0),
    stock INTEGER NOT NULL DEFAULT 0 CHECK (stock >= 0),
    -- 0 for made-to-order or supplier-backed items with no fixed count (Squarespace
    -- "Unlimited"). Their stock column stays 0 and is not checked at checkout.
    track_stock INTEGER NOT NULL DEFAULT 1 CHECK (track_stock IN (0, 1)),
    max_per_order INTEGER,
    visible INTEGER NOT NULL DEFAULT 0 CHECK (visible IN (0, 1)),
    featured INTEGER NOT NULL DEFAULT 0 CHECK (featured IN (0, 1)),
    weight_oz REAL,
    length_in REAL,
    width_in REAL,
    height_in REAL,
    -- single | small-sealed | large-sealed | accessory (plan "Shipping")
    ship_class TEXT NOT NULL DEFAULT 'large-sealed',
    gtin TEXT,
    tcgplayer_product_ids TEXT NOT NULL DEFAULT '',   -- '|'-separated TCGplayer product IDs
    tcgplayer_url TEXT,           -- optional "Also on TCGplayer" link for this product
    -- Lower-cased, accent-free text over title, set, number and tags, for keyword search.
    search_text TEXT NOT NULL DEFAULT '',
    source TEXT,                  -- where the row came from, e.g. 'squarespace:<variant id>'
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX products_visible ON products (visible, kind);
CREATE INDEX products_set ON products (set_slug);

CREATE TABLE product_images (
    id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    -- Base name of the stored image, e.g. 'black-bolt-booster-box-3f2a9c1e'. The original is
    -- in images/original/<file_name>.<ext>; resized WebP copies in images/<width>/<file_name>.webp.
    file_name TEXT NOT NULL,
    original_ext TEXT NOT NULL DEFAULT 'jpg',
    source_url TEXT,
    alt_text TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0,
    UNIQUE (product_id, file_name)
);

-- Browse categories (/c/<slug>). The list is fixed here; products get theirs from
-- product_type on import (see storefront/app/classify.py).
CREATE TABLE categories (
    id INTEGER PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    sort_order INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE product_categories (
    product_id INTEGER NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    category_id INTEGER NOT NULL REFERENCES categories (id) ON DELETE CASCADE,
    PRIMARY KEY (product_id, category_id)
);

CREATE TABLE tags (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE COLLATE NOCASE
);
CREATE TABLE product_tags (
    product_id INTEGER NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES tags (id) ON DELETE CASCADE,
    PRIMARY KEY (product_id, tag_id)
);

-- Every stock change, with a reason. products.stock always equals the sum of change for
-- that product (a test checks this). Reasons: sale, reservation, release, restock,
-- adjustment, tcgplayer_sale, import.
CREATE TABLE stock_movements (
    id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    change INTEGER NOT NULL,
    reason TEXT NOT NULL CHECK (reason IN
        ('sale', 'reservation', 'release', 'restock', 'adjustment', 'tcgplayer_sale', 'import')),
    reference TEXT,
    at TEXT NOT NULL
);
CREATE INDEX stock_movements_product ON stock_movements (product_id);

-- Old addresses of a product (Squarespace /shop/p/<slug>, or an earlier slug of ours),
-- answered with a 301 to the product's current page. Page and category redirects that
-- don't depend on the database are in storefront/redirects.csv.
CREATE TABLE product_redirects (
    old_path TEXT PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products (id) ON DELETE CASCADE
);

INSERT INTO categories (slug, name, description, sort_order) VALUES
    ('booster-boxes', 'Booster boxes', 'Sealed booster boxes, English and Japanese.', 10),
    ('elite-trainer-boxes', 'Elite Trainer Boxes', 'Elite and premium trainer boxes.', 20),
    ('booster-bundles', 'Booster bundles', 'Booster bundles of sealed packs.', 30),
    ('booster-packs', 'Booster packs', 'Single booster packs and pack art sets.', 40),
    ('collection-boxes', 'Collection boxes', 'Collection boxes, ex boxes and premium collections.', 50),
    ('tins', 'Tins', 'Pokémon TCG tins.', 60),
    ('accessories', 'Accessories', 'Playmats, display stands and binders.', 70),
    ('singles', 'Singles', 'Single cards.', 80),
    ('other', 'Other', 'Everything else.', 90);
