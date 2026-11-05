"""Orders from the command line, until the LAN-only admin (Phase 3) takes this over.

Run inside the storefront container:

    docker-compose exec storefront python -m storefront.app.orders_cli list
    docker-compose exec storefront python -m storefront.app.orders_cli show P6T-2026-0001
    docker-compose exec storefront python -m storefront.app.orders_cli ship P6T-2026-0001 \\
        --carrier USPS --tracking 9400111899223817461234
    docker-compose exec storefront python -m storefront.app.orders_cli reconcile

`ship` marks a paid order shipped and emails the customer the tracking number (or writes the
email to data/storefront/outbox/ when SMTP isn't set up). `reconcile` runs the missed-webhook
catch-up once, the same job the app runs every 15 minutes.
"""

from __future__ import annotations

import argparse
import sys

from . import orders, stripe_api
from .db import connect, migrate
from .notify import local_time, money
from .settings import load_settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="orders_cli", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    listing = sub.add_parser("list", help="recent orders")
    listing.add_argument("--all", action="store_true", help="include expired and cancelled checkouts")
    sub.add_parser("show", help="one order").add_argument("number")
    ship = sub.add_parser("ship", help="mark shipped and email tracking")
    ship.add_argument("number")
    ship.add_argument("--carrier", default="USPS")
    ship.add_argument("--tracking", required=True)
    sub.add_parser("reconcile", help="catch up on missed Stripe webhooks once")
    args = parser.parse_args(argv)

    settings = load_settings()
    migrate(settings.db_path)
    conn = connect(settings.db_path, writer=True)
    try:
        if args.command == "list":
            where = "" if args.all else "WHERE status NOT IN ('expired', 'cancelled')"
            for row in conn.execute(f"SELECT * FROM orders {where} ORDER BY id DESC LIMIT 50"):
                print(f"{row['number']:<16} {row['status']:<18} {money(row['total_cents']):>10}  "
                      f"{local_time(row['paid_at'] or row['created_at']):<26} {row['email'] or ''}")
        elif args.command == "show":
            order = orders.get_order(conn, args.number)
            if order is None:
                print(f"No order {args.number}", file=sys.stderr)
                return 1
            print(f"{order['number']}  {order['status']}  ({order['mode']} mode)")
            print(f"Customer: {order['name'] or ''} <{order['email'] or ''}>")
            for item in order["items"]:
                print(f"  {item['quantity']} x {item['sku']}  {item['title']}  {money(item['unit_price_cents'])}")
            print(f"Total {money(order['total_cents'])} (shipping {money(order['shipping_cents'])}, "
                  f"tax {money(order['tax_cents'])}, discount {money(order['discount_cents'])})")
            if order["notes"]:
                print(f"Notes: {order['notes']}")
            print(f"Status page: {orders.status_url(settings.base_url, order)}")
        elif args.command == "ship":
            order = orders.mark_shipped(conn, settings, args.number, args.carrier, args.tracking, settings.base_url)
            print(f"{order['number']} marked shipped; tracking email sent to {order['email']}.")
        elif args.command == "reconcile":
            if not settings.checkout_enabled:
                print("Checkout is off (no Stripe key), nothing to reconcile.")
                return 0
            print(orders.reconcile(conn, settings, stripe_api.StripeClient(settings.stripe_secret_key),
                                   settings.base_url))
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
