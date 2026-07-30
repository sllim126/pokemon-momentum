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
import mimetypes
import os
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.card_image_cache import (
    DEFAULT_CARD_IMAGE_DIR,
    build_card_image_path,
    build_card_image_public_url,
    cache_card_image,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "output"
DEFAULT_DRAFT_CSV = OUTPUT_DIR / "squarespace_single_listing_drafts.csv"
DEFAULT_CREATED_CSV = OUTPUT_DIR / "squarespace_created_single_listings.csv"
DEFAULT_LOG_JSONL = OUTPUT_DIR / "squarespace_single_listing_create_log.jsonl"
DEFAULT_EXPORT_CSV = REPO_ROOT / "products_Apr-09_04-31-18PM.csv"
DEFAULT_IMAGE_UPLOAD_LOG_CSV = OUTPUT_DIR / "squarespace_single_listing_image_uploads.csv"

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

IMAGE_UPLOAD_LOG_FIELDS = [
    "uploaded_at",
    "sku",
    "product_id",
    "variant_id",
    "image_id",
    "image_url",
    "image_source",
    "image_cache_path",
    "status",
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
    parser.add_argument("--image-upload-log-csv", default=str(DEFAULT_IMAGE_UPLOAD_LOG_CSV))
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
    parser.add_argument("--image-cache-dir", default=str(DEFAULT_CARD_IMAGE_DIR))
    parser.add_argument(
        "--skip-image-cache",
        action="store_true",
        help="Skip downloading card images into the local /images/cards cache.",
    )
    parser.add_argument(
        "--force-image-download",
        action="store_true",
        help="Re-download cached card images even if a local copy already exists.",
    )
    parser.add_argument(
        "--square-pad-images",
        action="store_true",
        help="Pad cached card images to a 1:1 square canvas before Squarespace upload.",
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
        help="Upload cached local images to existing Squarespace products from the created CSV ledger.",
    )
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


def append_csv_rows(path: Path, fieldnames: Iterable[str], rows: Iterable[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
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


def resolve_image_cache_metadata(
    row: dict[str, str],
    image_cache_dir: Path,
) -> dict[str, str]:
    image_url = str(row.get("image_url") or "").strip()
    if not image_url:
        return {"image_cache_path": "", "image_public_url": ""}

    image_path = build_card_image_path(
        image_root=image_cache_dir,
        sku=str(row.get("sku") or ""),
        language=str(row.get("language") or ""),
        image_url=image_url,
    )
    return {
        "image_cache_path": str(image_path),
        "image_public_url": build_card_image_public_url(image_path=image_path, image_root=image_cache_dir),
    }


def resolve_existing_image_cache_path(row: dict[str, str], image_cache_dir: Path) -> Optional[Path]:
    metadata = resolve_image_cache_metadata(row, image_cache_dir)
    default_path = Path(metadata.get("image_cache_path") or "")
    if default_path.exists():
        return default_path

    language = str(row.get("language") or "").strip().lower() or "english"
    sku = str(row.get("sku") or "").strip()
    if not sku:
        return None
    language_dir = image_cache_dir / language
    candidates = sorted(language_dir.glob(f"{sku}.*"))
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def ensure_image_cached(
    *,
    row: dict[str, str],
    image_cache_dir: Path,
    timeout: int,
    force: bool,
    square_pad: bool,
) -> dict[str, str]:
    metadata = resolve_image_cache_metadata(row, image_cache_dir)
    image_url = str(row.get("image_url") or "").strip()
    cache_path = metadata["image_cache_path"]
    if not image_url or not cache_path:
        return metadata

    resolved_path, downloaded = cache_card_image(
        image_url=image_url,
        image_path=Path(cache_path),
        timeout=timeout,
        force=force,
        square_pad=square_pad,
    )
    metadata["image_cache_path"] = str(resolved_path)
    metadata["image_public_url"] = build_card_image_public_url(
        image_path=resolved_path,
        image_root=image_cache_dir,
    )
    metadata["image_cached"] = "downloaded" if downloaded else "existing"
    return metadata


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


def mime_type_for_path(path: Path) -> str:
    guessed, _ = mimetypes.guess_type(str(path))
    return guessed or "application/octet-stream"


def upload_product_image(
    *,
    base_url: str,
    api_version: str,
    api_key: str,
    user_agent: str,
    product_id: str,
    image_path: Path,
    timeout: int,
) -> requests.Response:
    url = f"{base_url}/{api_version}/commerce/products/{product_id}/images"
    auth_headers = {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": user_agent,
    }
    mime_type = mime_type_for_path(image_path)

    with image_path.open("rb") as handle:
        raw_response = requests.post(
            url,
            headers={**auth_headers, "Content-Type": mime_type},
            data=handle.read(),
            timeout=timeout,
        )
    if raw_response.status_code != 415:
        return raw_response

    # Squarespace's image endpoint is picky about upload shape; keep the retry
    # sequence here so the rest of the workflow can treat image upload as one
    # operation without caring which wire format actually succeeds.
    with image_path.open("rb") as handle:
        multipart_file_response = requests.post(
            url,
            headers=auth_headers,
            files={"file": (image_path.name, handle, mime_type)},
            timeout=timeout,
        )
    if multipart_file_response.status_code != 415:
        return multipart_file_response

    with image_path.open("rb") as handle:
        return requests.post(
            url,
            headers=auth_headers,
            files={"image": (image_path.name, handle, mime_type)},
            timeout=timeout,
        )


def get_product_image_status(
    *,
    base_url: str,
    api_version: str,
    api_key: str,
    user_agent: str,
    product_id: str,
    image_id: str,
    timeout: int,
) -> requests.Response:
    return requests.get(
        f"{base_url}/{api_version}/commerce/products/{product_id}/images/{image_id}/status",
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": user_agent,
        },
        timeout=timeout,
    )


def associate_variant_image(
    *,
    base_url: str,
    api_version: str,
    api_key: str,
    user_agent: str,
    product_id: str,
    variant_id: str,
    image_id: str,
    timeout: int,
) -> requests.Response:
    wrapped_response = requests.post(
        f"{base_url}/{api_version}/commerce/products/{product_id}/variants/{variant_id}/image",
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": user_agent,
            "Content-Type": "application/json",
        },
        json={"imageId": {"present": True, "value": image_id}},
        timeout=timeout,
    )
    if wrapped_response.status_code // 100 == 2:
        return wrapped_response
    if wrapped_response.status_code != 400:
        return wrapped_response
    # The variant-image endpoint accepted a simpler payload shape during live
    # testing, so fall back before treating the association as a hard failure.
    return requests.post(
        f"{base_url}/{api_version}/commerce/products/{product_id}/variants/{variant_id}/image",
        headers={
            "Authorization": f"Bearer {api_key}",
            "User-Agent": user_agent,
            "Content-Type": "application/json",
        },
        json={"imageId": image_id},
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


def wait_for_image_ready(
    *,
    base_url: str,
    api_version: str,
    api_key: str,
    user_agent: str,
    product_id: str,
    image_id: str,
    timeout: int,
    sleep_seconds: float,
    max_attempts: int = 12,
) -> str:
    last_status = ""
    for _ in range(max_attempts):
        # Upload returns immediately while Squarespace processes the image in
        # the background. Poll until the image is ready before association.
        response = get_product_image_status(
            base_url=base_url,
            api_version=api_version,
            api_key=api_key,
            user_agent=user_agent,
            product_id=product_id,
            image_id=image_id,
            timeout=timeout,
        )
        if response.status_code // 100 != 2:
            return f"http_{response.status_code}"
        payload = response.json()
        last_status = str(payload.get("status") or "").strip().lower()
        if last_status in {"ready", "error"}:
            return last_status
        time.sleep(sleep_seconds)
    return last_status or "timeout"


def main() -> int:
    load_local_dotenv(REPO_ROOT / ".env")
    args = parse_args()
    sku_filters = set(args.sku)
    image_cache_dir = Path(args.image_cache_dir)

    if args.upload_images_existing:
        api_key = os.getenv("SQUARESPACE_API_KEY", "")
        if not api_key:
            raise SystemExit("Missing SQUARESPACE_API_KEY env var.")

        created_rows = read_csv_rows(Path(args.created_csv))
        selected_created_rows: List[dict[str, str]] = []
        for row in created_rows:
            sku = str(row.get("sku") or "").strip()
            if sku_filters and sku not in sku_filters:
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
                existing_image_path = resolve_existing_image_cache_path(row, image_cache_dir)
                print(
                    json.dumps(
                        {
                            "sku": row.get("sku"),
                            "product_id": row.get("product_id"),
                            "variant_id": row.get("variant_id"),
                            "image_url": row.get("image_url"),
                            "image_cache_path": str(existing_image_path or ""),
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

            try:
                # Existing products only need the cached local file path plus the
                # stored Squarespace product/variant ids from the creation ledger.
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
                        "image_cache_path": str(image_path),
                        "status": str(exc),
                    }
                )
            time.sleep(args.sleep_seconds)

        if upload_log_rows:
            append_csv_rows(Path(args.image_upload_log_csv), IMAGE_UPLOAD_LOG_FIELDS, upload_log_rows)
        print(f"Image upload records written: {len(upload_log_rows)}")
        return 0

    draft_csv = Path(args.draft_csv)
    if not draft_csv.exists():
        raise SystemExit(f"Draft CSV not found: {draft_csv}")

    rows = read_csv_rows(draft_csv)
    selected_rows = eligible_rows(rows, sku_filters, args.limit)
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
            image_cache = resolve_image_cache_metadata(row, image_cache_dir)
            print(
                json.dumps(
                    {
                        "sku": row.get("sku"),
                        "title": row.get("final_title"),
                        "payload": payload,
                        "categories_manual_followup": split_csv_list(row.get("categories")),
                        "image_url_manual_followup": row.get("image_url"),
                        "image_cache_path": image_cache["image_cache_path"],
                        "image_public_url": image_cache["image_public_url"],
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
                image_cache = {"image_cache_path": "", "image_public_url": "", "image_cached": ""}
                if not args.skip_image_cache:
                    image_cache = ensure_image_cached(
                        row=row,
                        image_cache_dir=image_cache_dir,
                        timeout=args.timeout,
                        force=args.force_image_download,
                        square_pad=args.square_pad_images,
                    )
                    if image_cache.get("image_cache_path"):
                        print(
                            f"IMAGE {sku}: {image_cache['image_cached']} {image_cache['image_cache_path']}"
                        )

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
                            "image_cache_path": image_cache.get("image_cache_path", ""),
                            "image_public_url": image_cache.get("image_public_url", ""),
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
