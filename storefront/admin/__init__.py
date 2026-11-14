"""The store admin (Phase 3): its own app on its own LAN-only port, never part of the public site.

See storefront/README.md, "Admin", and docs/storefront_plan.md, "How it fits together".
Kept free of imports so `python3 -m storefront.admin.passwd` runs on the host's plain Python.
"""
