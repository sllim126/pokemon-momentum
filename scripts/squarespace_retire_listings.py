"""Retire Squarespace listings for cards that are gone: hide the product and set stock to 0.

This is reversible (the listing, photos, and URL stay), unlike deleting the product.
Product/variant IDs come from the latest Squarespace export. Without --apply it only
prints what it would do.

    python3 scripts/squarespace_retire_listings.py --sku 610540-holofoil --sku 84840
    python3 scripts/squarespace_retire_listings.py --sku ... --apply
"""

import argparse
import csv
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
BASE_URL = "https://api.squarespace.com"
LOG_PATH = ROOT / "output" / "squarespace_retired_listings.jsonl"


def load_env() -> None:
    env_file = ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        if sep and key.strip() and key.strip() not in os.environ:
            os.environ[key.strip()] = value.strip().strip("'\"")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sku", action="append", required=True, help="SKU to retire (repeatable).")
    parser.add_argument("--export-csv", help="Squarespace product export (default: SQUARESPACE_EXPORT_CSV).")
    parser.add_argument("--apply", action="store_true", help="Actually change the store.")
    return parser.parse_args()


def main() -> int:
    load_env()
    args = parse_args()
    export_csv = args.export_csv or os.getenv("SQUARESPACE_EXPORT_CSV", "")
    api_key = os.getenv("SQUARESPACE_API_KEY", "")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": os.getenv("SQUARESPACE_USER_AGENT", "pokemon-momentum/retire-listings"),
        "Content-Type": "application/json",
    }

    with open(export_csv, newline="", encoding="utf-8-sig") as f:
        export = {row["SKU"].strip(): row for row in csv.DictReader(f) if row.get("SKU")}

    targets = []
    for sku in dict.fromkeys(s.strip() for s in args.sku):
        row = export.get(sku)
        if row is None or not row.get("Product ID [Non Editable]"):
            print(f"ERROR: {sku} not found in {export_csv}; nothing changed.")
            return 1
        targets.append((sku, row))

    for sku, row in targets:
        print(f"{'RETIRE' if args.apply else 'WOULD RETIRE'}: {sku} | {row['Title'][:70]} | "
              f"visible={row['Visible']} stock={row['Stock']} -> visible=No stock=0")
    if not args.apply:
        print("\nPreview only. Re-run with --apply to change the store.")
        return 0
    if not api_key:
        print("ERROR: SQUARESPACE_API_KEY is not set.")
        return 2

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    failures = 0
    with LOG_PATH.open("a", encoding="utf-8") as log:
        for sku, row in targets:
            product_id = row["Product ID [Non Editable]"]
            variant_id = row["Variant ID [Non Editable]"]
            hide = requests.post(
                f"{BASE_URL}/v2/commerce/products/{product_id}",
                headers=headers, json={"isVisible": False}, timeout=30,
            )
            stock = requests.post(
                f"{BASE_URL}/1.0/commerce/inventory/adjustments",
                headers={**headers, "Idempotency-Key": str(uuid.uuid4())},
                json={"setFiniteOperations": [{"variantId": variant_id, "quantity": 0}]},
                timeout=30,
            )
            ok = hide.ok and stock.ok
            failures += 0 if ok else 1
            log.write(json.dumps({
                "at": datetime.now(timezone.utc).isoformat(),
                "sku": sku, "product_id": product_id, "variant_id": variant_id,
                "hide_status": hide.status_code, "stock_status": stock.status_code,
                "hide_response": hide.text[:300], "stock_response": stock.text[:300], "ok": ok,
            }) + "\n")
            print(f"  {sku}: hide={hide.status_code} stock={stock.status_code} {'OK' if ok else 'FAILED'}")
    print(f"\nDone: {len(targets) - failures} retired, {failures} failed. Log: {LOG_PATH}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
