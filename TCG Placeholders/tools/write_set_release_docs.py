#!/usr/bin/env python3
"""Regenerate the Markdown set-date reference from the CSV source."""

from __future__ import annotations

import csv
from pathlib import Path


NOTE_LABELS = {
    "Special expansion": "*",
    "Special product/promo set": "special",
}


def main() -> int:
    rows = list(csv.DictReader(Path("data/set_release_dates.csv").open(encoding="utf-8")))
    lines = [
        "# Set Release Dates Reference",
        "",
        "Notes from collection planning.",
        "",
        "- `*` special expansion",
        "- `special` special product, promo set, or non-main expansion release",
        "",
    ]
    for era in ["Sword & Shield", "Scarlet & Violet", "Mega Evolution"]:
        lines.extend(
            [
                f"## {era}",
                "",
                "| Set | Code | Release Date | Notes |",
                "| --- | --- | --- | --- |",
            ]
        )
        for row in rows:
            if row["Era"] != era:
                continue
            notes = NOTE_LABELS.get(row["Notes"], row["Notes"])
            lines.append(
                f"| {row['Set']} | {row['Code']} | {row['Release Date']} | {notes} |"
            )
        lines.append("")

    Path("docs/set_release_dates.md").write_text("\n".join(lines), encoding="utf-8")
    print("Wrote docs/set_release_dates.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
