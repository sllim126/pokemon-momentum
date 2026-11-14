"""The public app stays public-only (security review 2026-11-11, item 15).

Everything on port 8010 is on the internet: the Cloudflare Tunnel routes poke6s.com there. So
the admin (Phase 3) must be its own app on its own port, never routes added to this one, and
this app may only accept the few writes a shopper needs. A new POST route, or anything under
/admin, fails here on purpose: if it's really meant to be public, add it to the list below
with a reason; if it's an admin feature, it belongs in the admin app (docs/storefront_plan.md, "How
it fits together").
"""

from storefront.app.main import create_app

# Every write the public app accepts, and why it has to be public.
PUBLIC_WRITES = {
    "/notify",           # "email me when the store opens" sign-up
    "/cart/quote",       # prices the shopper's cart (reads only)
    "/checkout",         # starts a Stripe checkout
    "/stripe/webhook",   # Stripe's own calls, signature-checked
}


def test_public_app_accepts_only_known_writes(imported):
    app = create_app(imported)
    writes = {route.path for route in app.routes
              if (getattr(route, "methods", None) or set()) - {"GET", "HEAD"}}
    assert writes == PUBLIC_WRITES


def test_public_app_has_no_admin_pages(imported, client):
    app = create_app(imported)
    assert not [r.path for r in app.routes if r.path.lower().startswith("/admin")]
    assert client.get("/admin").status_code == 404
