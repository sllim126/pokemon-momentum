"""Validate persisted market-index snapshots and write an operator audit report."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.dashboards.api import INDEX_DEFINITIONS, category_config, index_overview_snapshot_path


DEFAULT_REPORT = REPO_ROOT / "output" / "index_overview_validation.md"


def validate_snapshot(payload: dict, definition: dict) -> list[str]:
    errors: list[str] = []
    index_key = str(payload.get("index_key") or "unknown")
    expected_count = int(definition.get("constituent_limit") or 100)
    holdings = payload.get("holdings") or []
    included_sets = payload.get("included_sets") or []
    series = payload.get("series") or []

    if int(payload.get("constituent_limit") or 0) != expected_count:
        errors.append(f"payload limit does not match definition ({expected_count})")
    if len(holdings) != expected_count:
        errors.append(f"latest holdings count is {len(holdings)}, expected {expected_count}")

    ranks = [int(item.get("rank") or 0) for item in holdings]
    if ranks != list(range(1, len(holdings) + 1)):
        errors.append("holding ranks are not contiguous from 1")

    keys = [
        (int(item.get("productId") or 0), str(item.get("subTypeName") or ""))
        for item in holdings
    ]
    if len(keys) != len(set(keys)):
        errors.append("duplicate product/variant holdings found")

    included_group_names = {
        int(item.get("groupId") or 0): str(item.get("name") or "")
        for item in included_sets
    }
    excluded_group_ids = {
        int(group_id) for group_id in definition.get("exclude_group_ids", [])
    }
    for item in holdings:
        group_id = int(item.get("groupId") or 0)
        group_name = str(item.get("groupName") or "")
        product_name = str(item.get("productName") or "")
        if group_id not in included_group_names:
            errors.append(f"holding {item.get('productId')} uses group {group_id} outside the index universe")
        elif included_group_names[group_id] != group_name:
            errors.append(
                f"holding {item.get('productId')} group name {group_name!r} does not match "
                f"universe name {included_group_names[group_id]!r}"
            )
        if group_id in excluded_group_ids:
            errors.append(f"excluded group {group_id} appears in holdings")
        if not product_name or product_name.lower().startswith("productid "):
            errors.append(f"holding {item.get('productId')} has placeholder product metadata")

    if not series:
        errors.append("index series is empty")
    else:
        latest_series_count = int(series[-1].get("constituent_count") or 0)
        if latest_series_count != len(holdings):
            errors.append(
                f"latest series count {latest_series_count} does not match holdings count {len(holdings)}"
            )
        if str(series[-1].get("date")) != str(payload.get("latest_date")):
            errors.append("latest series date does not match payload latest_date")

    summary_count = int((payload.get("summary") or {}).get("holdings_count") or 0)
    if summary_count != len(holdings):
        errors.append(f"summary holdings count {summary_count} does not match payload")

    return list(dict.fromkeys(f"{index_key}: {message}" for message in errors))


def load_health_latest(category_id: int) -> str | None:
    category = category_config(category_id)
    path = Path("data/extracted") / f"{category.slug}_health_snapshot.csv"
    if not path.exists():
        return None
    import csv

    with path.open(newline="", encoding="utf-8-sig") as handle:
        row = next(csv.DictReader(handle), None)
    return str((row or {}).get("latest") or (row or {}).get("latest_date") or "") or None


def build_report() -> tuple[str, list[str]]:
    rows: list[dict] = []
    errors: list[str] = []
    for index_key, definition in INDEX_DEFINITIONS.items():
        category_id = int(definition.get("category_id") or 3)
        path = index_overview_snapshot_path(category_id, index_key)
        if not path.exists():
            errors.append(f"{index_key}: snapshot file is missing")
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        snapshot_errors = validate_snapshot(payload, definition)
        errors.extend(snapshot_errors)
        latest_market_date = load_health_latest(category_id)
        if latest_market_date and str(payload.get("latest_date")) != latest_market_date:
            errors.append(
                f"{index_key}: snapshot date {payload.get('latest_date')} differs from market date {latest_market_date}"
            )
        rows.append(
            {
                "key": index_key,
                "name": payload.get("index_name"),
                "category": category_id,
                "expected": int(definition.get("constituent_limit") or 100),
                "actual": len(payload.get("holdings") or []),
                "sets": len(payload.get("included_sets") or []),
                "date": payload.get("latest_date"),
                "status": "PASS" if not snapshot_errors else "FAIL",
            }
        )

    lines = [
        "# Index Overview Validation",
        "",
        "| Index | Category | Expected | Actual | Sets | Latest | Structural status |",
        "| --- | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| {row['name']} (`{row['key']}`) | {row['category']} | {row['expected']} | "
            f"{row['actual']} | {row['sets']} | {row['date']} | {row['status']} |"
        )
    lines.extend(["", "## Findings", ""])
    if errors:
        lines.extend(f"- {message}" for message in errors)
    else:
        lines.append("- All configured index snapshots passed structural, membership, count, and freshness checks.")
    lines.append("")
    return "\n".join(lines), errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report, errors = build_report()
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report, encoding="utf-8")
    print(report)
    print(f"Wrote: {args.report}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
