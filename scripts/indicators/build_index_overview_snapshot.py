import argparse
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.dashboards.api import (
    _build_index_overview_payload,
    index_overview_keys_for_category,
    save_index_overview_snapshot,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build persisted index overview payload snapshots for one category."
    )
    parser.add_argument(
        "--category-id",
        type=int,
        default=3,
        help="Category ID to snapshot. Default: 3 (Pokemon).",
    )
    parser.add_argument(
        "--index",
        action="append",
        default=[],
        help="Optional index key to build. Can be passed multiple times.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    requested_indexes = [str(value or "").strip().lower() for value in args.index if str(value or "").strip()]
    index_keys = requested_indexes or index_overview_keys_for_category(args.category_id)
    if not index_keys:
        print(f"No eligible index overview snapshots found for category_id={args.category_id}.")
        return 1

    built_count = 0
    for index_key in index_keys:
        payload = _build_index_overview_payload(category_id=args.category_id, index_key=index_key)
        path = save_index_overview_snapshot(args.category_id, index_key, payload)
        built_count += 1
        print(f"Built {index_key} -> {path}")

    print(f"Built {built_count} index overview snapshot(s) for category_id={args.category_id}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
