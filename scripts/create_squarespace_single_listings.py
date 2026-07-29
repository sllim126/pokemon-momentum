#!/usr/bin/env python3
"""
Create hidden Squarespace singles listings from reviewed draft CSV rows.

Important constraints:
- draft rows must be reviewed locally before API writes
- SKU stays aligned to the canonical product identifier used by price sync
- Squarespace remains the publishing target, not the metadata source of truth
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "output"
DEFAULT_DRAFT_CSV = OUTPUT_DIR / "squarespace_single_listing_drafts.csv"
DEFAULT_CREATED_CSV = OUTPUT_DIR / "squarespace_created_single_listings.csv"
DEFAULT_LOG_JSONL = OUTPUT_DIR / "squarespace_single_listing_create_log.jsonl"
DEFAULT_EXPORT_CSV = REPO_ROOT / "products_Apr-09_04-31-18PM.csv"

CREATED_FIELDS = [
    "created_at",
    "sku",
    "product_id",
    "variant_id",
    "canonical_product_id",
    "subtype",
    "title",
    "target_price",
    "quantity",
    "language",
    "condition",
    "url_slug",
    "squarespace_url",
    "store_page_id",
    "visibility",
    "tags",
    "categories",
    "image_url",
    "notes",
]


def load_local_dotenv(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create hidden Squarespace singles listings from approved draft rows."
    )
    parser.add_argument("--draft-csv", default=str(DEFAULT_DRAFT_CSV))
    parser.add_argument("--created-csv", default=str(DEFAULT_CREATED_CSV))
    parser.add_argument("--log-jsonl", default=str(DEFAULT_LOG_JSONL))
    parser.add_argument("--squarespace-export", default=str(DEFAULT_EXPORT_CSV))
    parser.add_argument("--store-page-id", default=os.getenv("SQUARESPACE_STORE_PAGE_ID"))
    parser.add_argument("--base-url", default="https://api.squarespace.com")
    parser.add_argument(
        "--api-version",
        default=os.getenv("SQUARESPACE_PRODUCTS_API_VERSION", "v2"),
    )
    parser.add_argument(
        "--user-agent",
        default=os.getenv("SQUARESPACE_USER_AGENT", "pokemon-momentum/single-listings"),
    )
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=float(os.getenv("SQUARESPACE_SLEEP_SECONDS", "0.25")),
    )
    parser.add_argument("--sku", action="append", default=[])
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually call the Squarespace API. Without this flag the script stays in preview mode.",
    )
    return parser.parse_args()


def read_csv_rows(path: Path) -> List[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def append_created_rows(path: Path, rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CREATED_FIELDS)
        if not existing:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_decimal(value: str | None) -> Optional[Decimal]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def load_existing_skus(path: Path) -> set[str]:
    skus: set[str] = set()
    if not path.exists():
        return skus
    for row in read_csv_rows(path):
        sku = str(row.get("SKU") or row.get("sku") or "").strip()
        if sku:
            skus.add(sku)
    return skus


def split_csv_list(value: str | None) -> List[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def build_product_payload(
    row: dict[str, str],
    store_page_id: str,
    currency: str = "USD",
) -> dict:
    price = parse_decimal(row.get("target_price"))
    if price is None:
        raise ValueError(f"Missing target_price for SKU {row.get('sku')}")

    stock_quantity = row.get("stock_quantity") or row.get("quantity") or "0"
    try:
        quantity = int(stock_quantity)
    except ValueError as exc:
        raise ValueError(f"Invalid quantity for SKU {row.get('sku')}: {stock_quantity}") from exc

    payload = {
        "type": "PHYSICAL",
        "name": row.get("final_title") or row.get("title_override") or row.get("sku"),
        "description": row.get("description_html") or row.get("short_description") or "",
        "isVisible": False,
        "storePageId": store_page_id,
        "tags": split_csv_list(row.get("tags")),
        "urlSlug": row.get("url_slug") or "",
        "variants": [
            {
                "sku": row.get("sku"),
                "pricing": {
                    "basePrice": {
                        "currency": currency,
                        "value": float(price),
                    }
                },
                "stock": {
                    "quantity": quantity,
                    "unlimited": False,
                },
            }
        ],
    }
    return payload


def create_product(
    *,
    base_url: str,
    api_version: str,
    api_key: str,
    user_agent: str,
    payload: dict,
    timeout: int,
) -> requests.Response:
    return requests.post(
        f"{base_url}/{api_version}/commerce/products",
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": user_agent,
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=timeout,
    )


def get_product(
    *,
    base_url: str,
    api_version: str,
    api_key: str,
    user_agent: str,
    product_id: str,
    timeout: int,
) -> requests.Response:
    return requests.get(
        f"{base_url}/{api_version}/commerce/products/{product_id}",
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": user_agent,
        },
        timeout=timeout,
    )


def extract_variant_id(product_payload: dict, sku: str) -> str:
    for product in product_payload.get("products", []):
        for variant in product.get("variants", []) or []:
            if str(variant.get("sku") or "").strip() == sku:
                return str(variant.get("id") or "").strip()
    return ""


def eligible_rows(rows: List[dict[str, str]], sku_filters: set[str], limit: int) -> List[dict[str, str]]:
    filtered: List[dict[str, str]] = []
    for row in rows:
        sku = str(row.get("sku") or "").strip()
        if sku_filters and sku not in sku_filters:
            continue
        if str(row.get("draft_status") or "").strip().lower() != "ready":
            continue
        if str(row.get("review_status") or "").strip().lower() != "approved":
            continue
        filtered.append(row)
        if limit and len(filtered) >= limit:
            break
    return filtered


def main() -> int:
    load_local_dotenv(REPO_ROOT / ".env")
    args = parse_args()

    draft_csv = Path(args.draft_csv)
    if not draft_csv.exists():
        raise SystemExit(f"Draft CSV not found: {draft_csv}")

    rows = read_csv_rows(draft_csv)
    selected_rows = eligible_rows(rows, set(args.sku), args.limit)
    print(f"Approved ready rows selected: {len(selected_rows)}")

    if not selected_rows:
        print("No approved rows to create.")
        return 0

    existing_skus = load_existing_skus(Path(args.squarespace_export))
    existing_skus.update(load_existing_skus(Path(args.created_csv)))

    store_page_id = args.store_page_id or os.getenv("SQUARESPACE_STORE_PAGE_ID", "")
    if not store_page_id:
        raise SystemExit("Missing --store-page-id or SQUARESPACE_STORE_PAGE_ID.")

    api_key = os.getenv("SQUARESPACE_API_KEY", "")
    if not args.execute:
        for row in selected_rows:
            payload = build_product_payload(row, store_page_id=store_page_id)
            print(
                json.dumps(
                    {
                        "sku": row.get("sku"),
                        "title": row.get("final_title"),
                        "payload": payload,
                        "categories_manual_followup": split_csv_list(row.get("categories")),
                        "image_url_manual_followup": row.get("image_url"),
                    }
                )
            )
        print("Preview only. Re-run with --execute to create products.")
        return 0

    if not api_key:
        raise SystemExit("Missing SQUARESPACE_API_KEY env var.")

    Path(args.log_jsonl).parent.mkdir(parents=True, exist_ok=True)
    created_records: List[dict[str, str]] = []
    with Path(args.log_jsonl).open("a", encoding="utf-8") as log_handle:
        for row in selected_rows:
            sku = str(row.get("sku") or "").strip()
            if sku in existing_skus:
                record = {"sku": sku, "ok": False, "error": "sku already exists in export/created log"}
                log_handle.write(json.dumps(record) + "\n")
                print(f"SKIP {sku}: already exists")
                continue

            payload = build_product_payload(row, store_page_id=store_page_id)
            try:
                create_response = create_product(
                    base_url=args.base_url,
                    api_version=args.api_version,
                    api_key=api_key,
                    user_agent=args.user_agent,
                    payload=payload,
                    timeout=args.timeout,
                )
                ok = create_response.status_code // 100 == 2
                response_text = create_response.text[:1000]
                if not ok:
                    log_handle.write(
                        json.dumps(
                            {
                                "sku": sku,
                                "ok": False,
                                "status_code": create_response.status_code,
                                "response": response_text,
                            }
                        )
                        + "\n"
                    )
                    print(f"FAILED {sku}: {create_response.status_code}")
                    time.sleep(args.sleep_seconds)
                    continue

                product_data = create_response.json()
                product_id = str(product_data.get("id") or "").strip()
                product_url = str(product_data.get("url") or "").strip()
                variant_id = ""
                if product_id:
                    detail_response = get_product(
                        base_url=args.base_url,
                        api_version=args.api_version,
                        api_key=api_key,
                        user_agent=args.user_agent,
                        product_id=product_id,
                        timeout=args.timeout,
                    )
                    if detail_response.status_code // 100 == 2:
                        variant_id = extract_variant_id(detail_response.json(), sku)

                created_record = {
                    "created_at": str(product_data.get("createdOn") or ""),
                    "sku": sku,
                    "product_id": product_id,
                    "variant_id": variant_id,
                    "canonical_product_id": str(row.get("product_id") or ""),
                    "subtype": str(row.get("subtype") or ""),
                    "title": str(row.get("final_title") or ""),
                    "target_price": str(row.get("target_price") or ""),
                    "quantity": str(row.get("stock_quantity") or row.get("quantity") or ""),
                    "language": str(row.get("language") or ""),
                    "condition": str(row.get("condition") or ""),
                    "url_slug": str(row.get("url_slug") or ""),
                    "squarespace_url": product_url,
                    "store_page_id": store_page_id,
                    "visibility": "hidden",
                    "tags": str(row.get("tags") or ""),
                    "categories": str(row.get("categories") or ""),
                    "image_url": str(row.get("image_url") or ""),
                    "notes": str(row.get("notes") or ""),
                }
                created_records.append(created_record)
                existing_skus.add(sku)
                log_handle.write(
                    json.dumps(
                        {
                            "sku": sku,
                            "ok": True,
                            "product_id": product_id,
                            "variant_id": variant_id,
                            "response": response_text,
                        }
                    )
                    + "\n"
                )
                print(f"CREATED {sku}: product_id={product_id} variant_id={variant_id or '<missing>'}")
            except requests.RequestException as exc:
                log_handle.write(json.dumps({"sku": sku, "ok": False, "error": str(exc)}) + "\n")
                print(f"ERROR {sku}: {exc}")

            time.sleep(args.sleep_seconds)

    if created_records:
        append_created_rows(Path(args.created_csv), created_records)
    print(f"Created records written: {len(created_records)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
