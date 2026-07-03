#!/usr/bin/env python3
"""Convert extracted Play Pokemon Prize Pack card-list text to project CSV."""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


FIELDNAMES = [
    "Release Set",
    "Release Date",
    "Product",
    "Product Type",
    "Card Name",
    "Card Number",
    "Variant",
    "Region",
    "Notes",
]


SET_CODE_FIXES = {
    # Extracted from the official PDF as SHF because of the embedded font.
    "SHF": "SFA",
}


def parse_rows(path: Path, release_set: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if (
            not line
            or "Card List" in line
            or "pokemon.com" in line.lower()
            or line.startswith("©")
            or line.startswith("Use ")
            or "=" in line
        ):
            continue

        boxes = line.count("■")
        clean = line.replace("■", "").strip()
        match = re.match(r"(.+?)\s+([A-Z]{3})\s+(\d{3})$", clean)
        if not match:
            continue

        name, set_code, number = match.groups()
        set_code = SET_CODE_FIXES.get(set_code, set_code)
        notes = (
            "Official card list; two checkbox marks extracted"
            if boxes >= 2
            else "Official card list; one checkbox mark extracted"
        )
        rows.append(
            {
                "Release Set": release_set,
                "Release Date": "",
                "Product": release_set,
                "Product Type": "Prize Pack",
                "Card Name": name.replace("’", "'"),
                "Card Number": f"{set_code}{number}",
                "Variant": "Play Pokemon Stamp",
                "Region": "US/UK",
                "Notes": notes,
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--release-set", required=True)
    args = parser.parse_args()

    rows = parse_rows(args.source, args.release_set)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
