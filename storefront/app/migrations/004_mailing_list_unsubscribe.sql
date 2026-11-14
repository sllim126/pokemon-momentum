-- Unsubscribe support for the email list (owner, 2026-11-14: one reusable marketing email that
-- meets the legal requirements; code in storefront/app/mailing.py). Every marketing email
-- carries a link with the address's own random token (/unsubscribe/<token>); opening it and
-- pressing the button, or a mail app's one-click unsubscribe, sets unsubscribed_at. Rows are
-- kept, not deleted, so the address is never emailed again by mistake; signing up again on
-- the site clears unsubscribed_at (a fresh opt-in).

ALTER TABLE notify_signups ADD COLUMN unsubscribe_token TEXT;   -- made on first send
ALTER TABLE notify_signups ADD COLUMN unsubscribed_at TEXT;     -- UTC ISO-8601
CREATE UNIQUE INDEX notify_signups_unsubscribe_token ON notify_signups (unsubscribe_token);
