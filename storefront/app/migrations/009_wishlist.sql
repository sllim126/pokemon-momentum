-- Wishlist (one Poke6s account, storefront plan step 9b, stage 2): products a signed-in visitor
-- saved with the ♡ on a product page. Keyed by the account's email (the market's account name,
-- app/accounts.py), so it follows the account across devices. Rows for accounts deleted on the
-- market are removed by the reconcile job (wishlist.drop_deleted_accounts).
CREATE TABLE IF NOT EXISTS wishlist (
    email TEXT NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,            -- UTC ISO-8601
    PRIMARY KEY (email, product_id)
);
CREATE INDEX IF NOT EXISTS wishlist_product ON wishlist(product_id);
