#!/usr/bin/env python3
"""Download local card-image cache files for Squarespace singles drafts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.card_image_cache import DEFAULT_CARD_IMAGE_DIR
from scripts.create_squarespace_single_listings import (
    DEFAULT_DRAFT_CSV,
    eligible_rows,
    ensure_image_cached,
    load_local_dotenv,
    read_csv_rows,
    resolve_image_cache_metadata,
)
from scripts.scrydex_images import build_scrydex_card_image_url


REPO_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cache local card images for approved Squarespace singles drafts."
    )
    parser.add_argument("--draft-csv", default=str(DEFAULT_DRAFT_CSV))
    parser.add_argument("--image-cache-dir", default=str(DEFAULT_CARD_IMAGE_DIR))
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--sku", action="append", default=[])
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--include-unapproved", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--source",
        choices=["dataset", "scrydex"],
        default="dataset",
        help="Choose whether to cache from the existing dataset image_url or a derived Scrydex URL.",
    )
    parser.add_argument(
        "--square-pad",
        action="store_true",
        help="Pad cached card images to a 1:1 square canvas after download.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually download images. Without this flag the script stays in preview mode.",
    )
    return parser.parse_args()


def selected_rows(rows: List[dict[str, str]], sku_filters: set[str], limit: int, include_unapproved: bool) -> List[dict[str, str]]:
    if include_unapproved:
        filtered: List[dict[str, str]] = []
        for row in rows:
            sku = str(row.get("sku") or "").strip()
            if sku_filters and sku not in sku_filters:
                continue
            image_url = str(row.get("image_url") or "").strip()
            if not image_url:
                continue
            filtered.append(row)
            if limit and len(filtered) >= limit:
                break
        return filtered
    return eligible_rows(rows, sku_filters, limit)


def image_source_url(row: dict[str, str], source: str) -> str:
    if source == "scrydex":
        # Scrydex URLs are derived from set id + printed number so we can keep
        # the draft CSV as the source of truth and avoid hand-maintaining links.
        return (
            build_scrydex_card_image_url(
                set_name=row.get("set_name"),
                card_number=row.get("card_number"),
            )
            or ""
        )
    return str(row.get("image_url") or "").strip()


def main() -> int:
    load_local_dotenv(REPO_ROOT / ".env")
    args = parse_args()

    draft_csv = Path(args.draft_csv)
    if not draft_csv.exists():
        raise SystemExit(f"Draft CSV not found: {draft_csv}")

    rows = read_csv_rows(draft_csv)
    selected = selected_rows(rows, set(args.sku), args.limit, args.include_unapproved)
    image_cache_dir = Path(args.image_cache_dir)

    print(f"Rows selected for image cache: {len(selected)}")
    if not selected:
        print("No rows selected.")
        return 0

    for row in selected:
        source_url = image_source_url(row, args.source)
        source_row = dict(row)
        source_row["image_url"] = source_url
        if args.execute:
            metadata = ensure_image_cached(
                row=source_row,
                image_cache_dir=image_cache_dir,
                timeout=args.timeout,
                force=args.force,
                square_pad=args.square_pad,
            )
        else:
            metadata = resolve_image_cache_metadata(source_row, image_cache_dir)
            metadata["image_cached"] = "preview"

        print(
            json.dumps(
                {
                    "sku": row.get("sku"),
                    "title": row.get("final_title"),
                    "image_url": source_url,
                    "image_source": args.source,
                    "image_cache_path": metadata.get("image_cache_path", ""),
                    "image_public_url": metadata.get("image_public_url", ""),
                    "image_cached": metadata.get("image_cached", ""),
                }
            )
        )

    if not args.execute:
        print("Preview only. Re-run with --execute to download images.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
