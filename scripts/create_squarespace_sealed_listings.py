#!/usr/bin/env python3
"""
Create hidden Squarespace sealed listings from reviewed draft CSV rows.

This script keeps local CSVs as the source of truth for metadata and pricing
rules, then writes successful creations back into the local Squarespace mapping
and pricing-rule files so the existing daily price sync can take over later.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Iterable, List

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.create_squarespace_single_listings import (
    append_csv_rows,
    associate_variant_image,
    create_product,
    ensure_image_cached,
    extract_variant_id,
    get_product,
    load_local_dotenv,
    load_existing_skus,
    parse_decimal,
    read_csv_rows,
    resolve_existing_image_cache_path,
    upload_product_image,
    wait_for_image_ready,
)
from scripts.card_image_cache import DEFAULT_CARD_IMAGE_DIR

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
OUTPUT_DIR = REPO_ROOT / "output"
DEFAULT_DRAFT_CSV = OUTPUT_DIR / "squarespace_sealed_listing_drafts.csv"
DEFAULT_CREATED_CSV = OUTPUT_DIR / "squarespace_created_sealed_listings.csv"
DEFAULT_LOG_JSONL = OUTPUT_DIR / "squarespace_sealed_listing_create_log.jsonl"
DEFAULT_EXPORT_CSV = REPO_ROOT / "products_Apr-09_04-31-18PM.csv"
DEFAULT_FOLLOWUP_CSV = OUTPUT_DIR / "squarespace_sealed_listing_followup.csv"
DEFAULT_MAPPING_CSV = DATA_DIR / "squarespace_tcgplayer_mapping.csv"
DEFAULT_RULES_CSV = DATA_DIR / "store_price_rules.csv"
DEFAULT_IMAGE_UPLOAD_LOG_CSV_SEALED = OUTPUT_DIR / "squarespace_sealed_listing_image_uploads.csv"

CREATED_FIELDS = [
    "created_at",
    "sku",
    "product_id",
    "variant_id",
    "tcgplayer_product_id",
    "title",
    "target_price",
    "quantity",
    "language",
    "product_type",
    "url_slug",
    "squarespace_url",
    "store_page_id",
    "visibility",
    "tags",
    "categories",
    "image_url",
    "notes",
]

FOLLOWUP_FIELDS = [
    "created_at",
    "sku",
    "title",
    "squarespace_url",
    "product_id",
    "variant_id",
    "tcgplayer_product_id",
    "target_price",
    "quantity",
    "language",
    "product_type",
    "visibility",
    "image_review_status",
    "tax_code_status",
    "categories_status",
    "fulfillment_status",
    "categories_suggested",
    "tags_suggested",
    "manual_notes",
    "reviewed_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create hidden Squarespace sealed listings from approved draft rows."
    )
    parser.add_argument("--draft-csv", default=str(DEFAULT_DRAFT_CSV))
    parser.add_argument("--created-csv", default=str(DEFAULT_CREATED_CSV))
    parser.add_argument("--log-jsonl", default=str(DEFAULT_LOG_JSONL))
    parser.add_argument("--squarespace-export", default=str(DEFAULT_EXPORT_CSV))
    parser.add_argument("--followup-csv", default=str(DEFAULT_FOLLOWUP_CSV))
    parser.add_argument("--mapping-csv", default=str(DEFAULT_MAPPING_CSV))
    parser.add_argument("--rules-csv", default=str(DEFAULT_RULES_CSV))
    parser.add_argument("--image-upload-log-csv", default=str(DEFAULT_IMAGE_UPLOAD_LOG_CSV_SEALED))
    parser.add_argument("--store-page-id", default=os.getenv("SQUARESPACE_STORE_PAGE_ID"))
    parser.add_argument("--base-url", default="https://api.squarespace.com")
    parser.add_argument(
        "--api-version",
        default=os.getenv("SQUARESPACE_PRODUCTS_API_VERSION", "v2"),
    )
    parser.add_argument(
        "--user-agent",
        default=os.getenv("SQUARESPACE_USER_AGENT", "pokemon-momentum/sealed-listings"),
    )
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--image-cache-dir", default=str(DEFAULT_CARD_IMAGE_DIR))
    parser.add_argument(
        "--skip-image-cache",
        action="store_true",
        help="Skip downloading sealed product images into the local cache.",
    )
    parser.add_argument(
        "--force-image-download",
        action="store_true",
        help="Re-download cached sealed product images even if a local copy already exists.",
    )
    parser.add_argument(
        "--square-pad-images",
        action="store_true",
        help="Pad cached sealed product images to a 1:1 square canvas before Squarespace upload.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=float(os.getenv("SQUARESPACE_SLEEP_SECONDS", "0.25")),
    )
    parser.add_argument("--sku", action="append", default=[])
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument(
        "--upload-images-existing",
        action="store_true",
        help="Upload cached local images to existing Squarespace sealed products from the created CSV ledger.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually call the Squarespace API. Without this flag the script stays in preview mode.",
    )
    return parser.parse_args()


def split_csv_list(value: str | None) -> List[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def append_created_rows(path: Path, rows: Iterable[dict[str, str]]) -> None:
    append_csv_rows(path, CREATED_FIELDS, rows)


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


def build_product_payload(row: dict[str, str], store_page_id: str, currency: str = "USD") -> dict:
    price = parse_decimal(row.get("target_price"))
    if price is None:
        raise ValueError(f"Missing target_price for SKU {row.get('sku')}")
    stock_quantity = row.get("stock_quantity") or row.get("quantity") or "0"
    try:
        quantity = int(stock_quantity)
    except ValueError as exc:
        raise ValueError(f"Invalid quantity for SKU {row.get('sku')}: {stock_quantity}") from exc

    return {
        "type": "PHYSICAL",
        "name": row.get("final_title") or row.get("product_name") or row.get("sku"),
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
                        "value": str(price),
                    }
                },
                "stock": {
                    "quantity": quantity,
                    "unlimited": False,
                },
            }
        ],
    }


def build_followup_rows(rows: Iterable[dict[str, str]]) -> List[dict[str, str]]:
    followup_rows: List[dict[str, str]] = []
    for row in rows:
        followup_rows.append(
            {
                "created_at": str(row.get("created_at") or ""),
                "sku": str(row.get("sku") or ""),
                "title": str(row.get("title") or ""),
                "squarespace_url": str(row.get("squarespace_url") or ""),
                "product_id": str(row.get("product_id") or ""),
                "variant_id": str(row.get("variant_id") or ""),
                "tcgplayer_product_id": str(row.get("tcgplayer_product_id") or ""),
                "target_price": str(row.get("target_price") or ""),
                "quantity": str(row.get("quantity") or ""),
                "language": str(row.get("language") or ""),
                "product_type": str(row.get("product_type") or ""),
                "visibility": str(row.get("visibility") or ""),
                "image_review_status": "pending",
                "tax_code_status": "pending",
                "categories_status": "pending",
                "fulfillment_status": "pending",
                "categories_suggested": str(row.get("categories") or ""),
                "tags_suggested": str(row.get("tags") or ""),
                "manual_notes": "",
                "reviewed_at": "",
            }
        )
    return followup_rows


def upsert_csv_rows(path: Path, fieldnames: List[str], key_field: str, rows: Iterable[dict[str, str]]) -> None:
    existing_rows: List[dict[str, str]] = []
    by_key: dict[str, dict[str, str]] = {}
    if path.exists():
        existing_rows = read_csv_rows(path)
        for row in existing_rows:
            key = str(row.get(key_field) or "").strip()
            if key:
                by_key[key] = row
    for row in rows:
        key = str(row.get(key_field) or "").strip()
        if not key:
            continue
        by_key[key] = {field: str(row.get(field) or "") for field in fieldnames}

    ordered_keys = []
    seen_keys: set[str] = set()
    for row in existing_rows:
        key = str(row.get(key_field) or "").strip()
        if key and key not in seen_keys:
            ordered_keys.append(key)
            seen_keys.add(key)
    for row in rows:
        key = str(row.get(key_field) or "").strip()
        if key and key not in seen_keys:
            ordered_keys.append(key)
            seen_keys.add(key)

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for key in ordered_keys:
            writer.writerow(by_key[key])


def build_mapping_rows(rows: Iterable[dict[str, str]]) -> List[dict[str, str]]:
    mapping_rows: List[dict[str, str]] = []
    for row in rows:
        mapping_rows.append(
            {
                "sku": str(row.get("sku") or ""),
                "tcgplayer_product_id": str(row.get("tcgplayer_product_id") or ""),
                "pricing_mode": "market_minus_5_pct_99",
                "min_price": "",
                "note": "Created by sealed Squarespace listing workflow",
            }
        )
    return mapping_rows


def build_rule_rows(rows: Iterable[dict[str, str]]) -> List[dict[str, str]]:
    rule_rows: List[dict[str, str]] = []
    for row in rows:
        rule_rows.append(
            {
                "sku": str(row.get("sku") or ""),
                "market_source": "en",
                "lookup_type": "product_id",
                "lookup_value": str(row.get("tcgplayer_product_id") or ""),
                "pricing_mode": "market_minus_5_pct_99",
                "min_price": "",
                "note": "Created by sealed Squarespace listing workflow",
            }
        )
    return rule_rows


def main() -> int:
    load_local_dotenv(REPO_ROOT / ".env")
    args = parse_args()
    image_cache_dir = Path(args.image_cache_dir)
    draft_csv = Path(args.draft_csv)
    api_key = os.getenv("SQUARESPACE_API_KEY", "")

    if args.upload_images_existing:
        if not api_key:
            raise SystemExit("Missing SQUARESPACE_API_KEY env var.")

        created_rows = read_csv_rows(Path(args.created_csv))
        selected_created_rows: List[dict[str, str]] = []
        for row in created_rows:
            sku = str(row.get("sku") or "").strip()
            if set(args.sku) and sku not in set(args.sku):
                continue
            if not str(row.get("product_id") or "").strip():
                continue
            if not str(row.get("variant_id") or "").strip():
                continue
            selected_created_rows.append(row)
            if args.limit and len(selected_created_rows) >= args.limit:
                break

        print(f"Existing created rows selected for image upload: {len(selected_created_rows)}")
        if not selected_created_rows:
            print("No existing created rows selected.")
            return 0

        if not args.execute:
            for row in selected_created_rows:
                image_cache = {"image_cache_path": "", "image_public_url": "", "image_cached": ""}
                if not args.skip_image_cache:
                    image_cache = ensure_image_cached(
                        row=row,
                        image_cache_dir=image_cache_dir,
                        timeout=args.timeout,
                        force=args.force_image_download,
                        square_pad=args.square_pad_images,
                    )
                existing_image_path = resolve_existing_image_cache_path(row, image_cache_dir)
                print(
                    json.dumps(
                        {
                            "sku": row.get("sku"),
                            "product_id": row.get("product_id"),
                            "variant_id": row.get("variant_id"),
                            "image_url": row.get("image_url"),
                            "image_cache_path": str(existing_image_path or image_cache.get("image_cache_path", "")),
                        }
                    )
                )
            print("Preview only. Re-run with --execute to upload images.")
            return 0

        upload_log_rows: List[dict[str, str]] = []
        for row in selected_created_rows:
            sku = str(row.get("sku") or "").strip()
            product_id = str(row.get("product_id") or "").strip()
            variant_id = str(row.get("variant_id") or "").strip()
            image_path = None
            try:
                if not args.skip_image_cache:
                    ensure_image_cached(
                        row=row,
                        image_cache_dir=image_cache_dir,
                        timeout=args.timeout,
                        force=args.force_image_download,
                        square_pad=args.square_pad_images,
                    )
                image_path = resolve_existing_image_cache_path(row, image_cache_dir)
                if image_path is None or not image_path.exists():
                    print(f"SKIP {sku}: cached image not found at {image_path}")
                    upload_log_rows.append(
                        {
                            "uploaded_at": "",
                            "sku": sku,
                            "product_id": product_id,
                            "variant_id": variant_id,
                            "image_id": "",
                            "image_url": str(row.get("image_url") or ""),
                            "image_source": "local-cache",
                            "image_cache_path": str(image_path or ""),
                            "status": "missing_cache_file",
                        }
                    )
                    continue

                upload_response = upload_product_image(
                    base_url=args.base_url,
                    api_version=args.api_version,
                    api_key=api_key,
                    user_agent=args.user_agent,
                    product_id=product_id,
                    image_path=image_path,
                    timeout=args.timeout,
                )
                if upload_response.status_code // 100 != 2:
                    print(f"FAILED {sku}: upload returned {upload_response.status_code}")
                    upload_log_rows.append(
                        {
                            "uploaded_at": "",
                            "sku": sku,
                            "product_id": product_id,
                            "variant_id": variant_id,
                            "image_id": "",
                            "image_url": str(row.get("image_url") or ""),
                            "image_source": "local-cache",
                            "image_cache_path": str(image_path),
                            "status": f"upload_http_{upload_response.status_code}",
                        }
                    )
                    time.sleep(args.sleep_seconds)
                    continue

                image_id = str(upload_response.json().get("imageId") or "").strip()
                status = wait_for_image_ready(
                    base_url=args.base_url,
                    api_version=args.api_version,
                    api_key=api_key,
                    user_agent=args.user_agent,
                    product_id=product_id,
                    image_id=image_id,
                    timeout=args.timeout,
                    sleep_seconds=args.sleep_seconds,
                )
                if status != "ready":
                    print(f"FAILED {sku}: image processing status={status}")
                    upload_log_rows.append(
                        {
                            "uploaded_at": "",
                            "sku": sku,
                            "product_id": product_id,
                            "variant_id": variant_id,
                            "image_id": image_id,
                            "image_url": str(row.get("image_url") or ""),
                            "image_source": "local-cache",
                            "image_cache_path": str(image_path),
                            "status": status,
                        }
                    )
                    time.sleep(args.sleep_seconds)
                    continue

                associate_response = associate_variant_image(
                    base_url=args.base_url,
                    api_version=args.api_version,
                    api_key=api_key,
                    user_agent=args.user_agent,
                    product_id=product_id,
                    variant_id=variant_id,
                    image_id=image_id,
                    timeout=args.timeout,
                )
                final_status = "associated" if associate_response.status_code // 100 == 2 else f"associate_http_{associate_response.status_code}"
                if associate_response.status_code // 100 != 2:
                    print(f"ASSOCIATE-DETAIL {sku}: {associate_response.text[:500]}")
                print(f"IMAGE-UPLOADED {sku}: image_id={image_id} status={final_status}")
                upload_log_rows.append(
                    {
                        "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "sku": sku,
                        "product_id": product_id,
                        "variant_id": variant_id,
                        "image_id": image_id,
                        "image_url": str(row.get("image_url") or ""),
                        "image_source": "local-cache",
                        "image_cache_path": str(image_path),
                        "status": final_status,
                    }
                )
            except requests.RequestException as exc:
                print(f"ERROR {sku}: {exc}")
                upload_log_rows.append(
                    {
                        "uploaded_at": "",
                        "sku": sku,
                        "product_id": product_id,
                        "variant_id": variant_id,
                        "image_id": "",
                        "image_url": str(row.get("image_url") or ""),
                        "image_source": "local-cache",
                        "image_cache_path": str(image_path or ""),
                        "status": str(exc),
                    }
                )
            time.sleep(args.sleep_seconds)

        if upload_log_rows:
            append_csv_rows(Path(args.image_upload_log_csv), [
                "uploaded_at",
                "sku",
                "product_id",
                "variant_id",
                "image_id",
                "image_url",
                "image_source",
                "image_cache_path",
                "status",
            ], upload_log_rows)
        print(f"Image upload records written: {len(upload_log_rows)}")
        return 0

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
                log_handle.write(json.dumps({"sku": sku, "ok": False, "error": "sku already exists in export/created log"}) + "\n")
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
                        ) + "\n"
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
                    "tcgplayer_product_id": str(row.get("tcgplayer_product_id") or ""),
                    "title": str(row.get("final_title") or ""),
                    "target_price": str(row.get("target_price") or ""),
                    "quantity": str(row.get("stock_quantity") or row.get("quantity") or ""),
                    "language": str(row.get("language") or ""),
                    "product_type": str(row.get("product_type") or ""),
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
                    ) + "\n"
                )
                print(f"CREATED {sku}: product_id={product_id} variant_id={variant_id or '<missing>'}")
            except requests.RequestException as exc:
                log_handle.write(json.dumps({"sku": sku, "ok": False, "error": str(exc)}) + "\n")
                print(f"ERROR {sku}: {exc}")

            time.sleep(args.sleep_seconds)

    if created_records:
        append_created_rows(Path(args.created_csv), created_records)
        append_csv_rows(Path(args.followup_csv), FOLLOWUP_FIELDS, build_followup_rows(created_records))
        upsert_csv_rows(
            Path(args.mapping_csv),
            ["sku", "tcgplayer_product_id", "pricing_mode", "min_price", "note"],
            "sku",
            build_mapping_rows(created_records),
        )
        upsert_csv_rows(
            Path(args.rules_csv),
            ["sku", "market_source", "lookup_type", "lookup_value", "pricing_mode", "min_price", "note"],
            "sku",
            build_rule_rows(created_records),
        )
    print(f"Created records written: {len(created_records)}")
    if created_records:
        print(f"Follow-up records written: {len(created_records)}")
        print(f"Mapping rows upserted: {len(created_records)}")
        print(f"Rule rows upserted: {len(created_records)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
