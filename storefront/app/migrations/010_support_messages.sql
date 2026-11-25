-- Support messages (one Poke6s account, storefront plan step 9b, stage 3; owner 2026-11-23):
-- the "Send us a message" box on /contact (and "Get help with this order" on /account). Each
-- message pings the private Discord order channel (number only, no customer details) and shows
-- on the admin Overview until it's marked answered. Answered messages are deleted after a year
-- (support.drop_old); the privacy page says so.
CREATE TABLE IF NOT EXISTS support_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,          -- UTC ISO-8601
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    order_number TEXT,                 -- as typed (checked only for shape), may be empty
    message TEXT NOT NULL,
    signed_in INTEGER NOT NULL DEFAULT 0,   -- sent while signed in with this email
    status TEXT NOT NULL DEFAULT 'new' CHECK (status IN ('new', 'answered')),
    answered_at TEXT
);
CREATE INDEX IF NOT EXISTS support_messages_status ON support_messages(status, created_at);
