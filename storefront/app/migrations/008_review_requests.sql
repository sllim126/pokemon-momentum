-- Google review request (owner, 2026-11-20): a few days after an order ships, the reconcile job
-- emails the customer once, asking for a review on the shop's Google Business Profile
-- (orders.send_review_requests). review_asked_at records that it went, so it never goes twice.
ALTER TABLE orders ADD COLUMN review_asked_at TEXT;   -- UTC ISO-8601
