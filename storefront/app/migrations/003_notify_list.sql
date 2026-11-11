-- "Email me when the store opens" list (owner, 2026-11-11). While checkout is off, product
-- pages offer this next to the TCGplayer link. Code: storefront/app/notify_list.py. The Privacy
-- page says what is kept and why; remove an address on request with
--   DELETE FROM notify_signups WHERE email = '...';

CREATE TABLE notify_signups (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE COLLATE NOCASE,
    source TEXT,                  -- the page it was sent from, e.g. '/p/black-bolt-booster-bundle'
    created_at TEXT NOT NULL,     -- UTC ISO-8601
    notified_at TEXT              -- set once the "we're open" email has gone to this address
);
