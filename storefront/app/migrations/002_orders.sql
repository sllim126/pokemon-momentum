-- Phase 2: orders, order lines and Stripe webhook events. Flow: docs/storefront_plan.md
-- "Checkout and orders"; code: storefront/app/orders.py. Money in cents, times UTC ISO-8601.

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    number TEXT NOT NULL UNIQUE,              -- 'P6-2026-0001'
    -- Secret part of the order status link (/order/<number>?k=<access_token>), so order
    -- numbers can't be guessed to read someone's address.
    access_token TEXT NOT NULL,
    -- pending: waiting for payment, stock reserved until reserved_until.
    -- expired: checkout abandoned or timed out, reservation released.
    -- cancelled: checkout could not start (Stripe error), reservation released.
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN
        ('pending', 'paid', 'shipped', 'refunded', 'partially_refunded', 'cancelled', 'expired',
         'disputed')),
    -- 'test' or 'live': which Stripe keys took the order. Reports leave test orders out.
    mode TEXT NOT NULL DEFAULT 'test' CHECK (mode IN ('test', 'live')),
    email TEXT,
    name TEXT,
    shipping_json TEXT,                       -- Stripe's shipping address, as JSON
    ship_class TEXT NOT NULL,                 -- the largest class in the cart
    subtotal_cents INTEGER NOT NULL DEFAULT 0,
    shipping_cents INTEGER NOT NULL DEFAULT 0,
    tax_cents INTEGER NOT NULL DEFAULT 0,
    discount_cents INTEGER NOT NULL DEFAULT 0,
    total_cents INTEGER NOT NULL DEFAULT 0,
    refunded_cents INTEGER NOT NULL DEFAULT 0,
    stripe_session_id TEXT UNIQUE,
    stripe_payment_intent TEXT,
    reserved_until TEXT,
    created_at TEXT NOT NULL,
    paid_at TEXT,
    shipped_at TEXT,
    closed_at TEXT,                           -- when it became expired or cancelled
    carrier TEXT,
    tracking_number TEXT,
    notes TEXT NOT NULL DEFAULT ''
);
CREATE INDEX orders_status ON orders (status);
CREATE INDEX orders_payment_intent ON orders (stripe_payment_intent);

-- What was sold, frozen at checkout: later price or title changes don't alter old orders.
CREATE TABLE order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products (id) ON DELETE SET NULL,
    sku TEXT NOT NULL,
    title TEXT NOT NULL,
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    -- 1 if this line took stock (track_stock products); untracked items have no movements.
    reserved INTEGER NOT NULL DEFAULT 0 CHECK (reserved IN (0, 1))
);
CREATE INDEX order_items_order ON order_items (order_id);

-- Every Stripe webhook event handled, so a repeated delivery does nothing twice.
CREATE TABLE webhook_events (
    event_id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    handled_at TEXT NOT NULL
);
