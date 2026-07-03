#!/usr/bin/env python3
"""Build validation exports and printable placeholder sheets from CSV or Excel data.

This intentionally uses only the Python standard library so the workflow can
run before a project environment or package manager is set up.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sys
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from xml.etree import ElementTree as ET


NS = {
    "m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}

DEFAULT_COLUMNS = [
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

REQUIRED_COLUMNS = ["Card Name", "Product", "Variant"]


@dataclass
class WorkbookRows:
    source_name: str
    headers: List[str]
    rows: List[Dict[str, str]]


def column_index(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha())
    value = 0
    for ch in letters:
        value = value * 26 + ord(ch.upper()) - ord("A") + 1
    return value - 1


def read_shared_strings(archive: zipfile.ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    strings = []
    for item in root.findall("m:si", NS):
        strings.append("".join(t.text or "" for t in item.findall(".//m:t", NS)))
    return strings


def read_workbook_sheet_paths(archive: zipfile.ZipFile) -> List[Tuple[str, str]]:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    relmap = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
    sheets = []
    for sheet in workbook.find("m:sheets", NS):
        rel_id = sheet.attrib["{%s}id" % NS["r"]]
        target = relmap[rel_id].lstrip("/")
        sheets.append((sheet.attrib["name"], "xl/" + target))
    return sheets


def cell_value(cell: ET.Element, shared_strings: Sequence[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(t.text or "" for t in cell.findall(".//m:t", NS)).strip()

    value_node = cell.find("m:v", NS)
    if value_node is None or value_node.text is None:
        return ""

    raw = value_node.text.strip()
    if cell_type == "s":
        return shared_strings[int(raw)].strip()
    return raw


def excel_serial_to_iso(raw: str) -> str:
    if not re.fullmatch(r"\d+(\.0+)?", raw):
        return raw
    serial = int(float(raw))
    # Excel's Windows date system includes a fake 1900-02-29.
    base = date(1899, 12, 30)
    return (base + timedelta(days=serial)).isoformat()


def date_to_iso(raw: str) -> str:
    raw = raw.strip()
    if not raw:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return raw
    match = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if match:
        month, day, year = (int(part) for part in match.groups())
        return date(year, month, day).isoformat()
    return excel_serial_to_iso(raw)


def read_first_sheet(path: Path, header_row: int = 2) -> WorkbookRows:
    with zipfile.ZipFile(path) as archive:
        shared_strings = read_shared_strings(archive)
        sheets = read_workbook_sheet_paths(archive)
        if not sheets:
            raise ValueError("Workbook does not contain any sheets.")

        sheet_name, sheet_path = sheets[0]
        root = ET.fromstring(archive.read(sheet_path))
        rows_by_number: Dict[int, Dict[int, str]] = {}
        for row in root.findall(".//m:sheetData/m:row", NS):
            row_number = int(row.attrib["r"])
            cells: Dict[int, str] = {}
            for cell in row.findall("m:c", NS):
                ref = cell.attrib.get("r", "")
                cells[column_index(ref)] = cell_value(cell, shared_strings)
            rows_by_number[row_number] = cells

    header_cells = rows_by_number.get(header_row, {})
    if not header_cells:
        raise ValueError(f"Could not find headers on row {header_row}.")

    min_col = min(header_cells)
    max_col = max(header_cells)
    headers = [header_cells.get(col, "").strip() for col in range(min_col, max_col + 1)]
    headers = [header for header in headers if header]

    data_rows: List[Dict[str, str]] = []
    for row_number in sorted(rows_by_number):
        if row_number <= header_row:
            continue
        row_cells = rows_by_number[row_number]
        record: Dict[str, str] = {"Source Row": str(row_number)}
        has_value = False
        for offset, header in enumerate(headers):
            raw = row_cells.get(min_col + offset, "").strip()
            if header == "Release Date":
                raw = excel_serial_to_iso(raw)
            if raw:
                has_value = True
            record[header] = raw
        if has_value:
            data_rows.append(record)

    return WorkbookRows(source_name=f"{path.name}:{sheet_name}", headers=headers, rows=data_rows)


def read_csv_source(path: Path) -> WorkbookRows:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        headers = list(reader.fieldnames or [])
        if not headers:
            raise ValueError("CSV does not contain a header row.")

        data_rows: List[Dict[str, str]] = []
        for line_number, row in enumerate(reader, start=2):
            record = {"Source Row": str(line_number)}
            has_value = False
            for header in headers:
                raw = (row.get(header) or "").strip()
                if header == "Release Date":
                    raw = date_to_iso(raw)
                if raw:
                    has_value = True
                record[header] = raw
            if has_value:
                data_rows.append(record)

    return WorkbookRows(source_name=path.name, headers=headers, rows=data_rows)


def read_source(path: Path, header_row: int = 2) -> WorkbookRows:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return read_csv_source(path)
    if suffix == ".xlsx":
        return read_first_sheet(path, header_row=header_row)
    raise ValueError(f"Unsupported input type: {path.suffix}")


def combine_sources(sources: Sequence[WorkbookRows]) -> WorkbookRows:
    if not sources:
        raise ValueError("No sources to combine.")

    headers = list(sources[0].headers)
    combined_rows: List[Dict[str, str]] = []
    source_names: List[str] = []

    for source in sources:
        source_names.append(source.source_name)
        for header in source.headers:
            if header not in headers:
                headers.append(header)
        for row in source.rows:
            combined = {header: row.get(header, "") for header in headers}
            combined["Source Row"] = f"{source.source_name}:{row.get('Source Row', '')}"
            combined_rows.append(combined)

    return WorkbookRows(
        source_name=" + ".join(source_names),
        headers=headers,
        rows=combined_rows,
    )


def validate(data: WorkbookRows) -> List[str]:
    issues: List[str] = []
    missing_columns = [col for col in DEFAULT_COLUMNS if col not in data.headers]
    if missing_columns:
        issues.append("Missing expected columns: " + ", ".join(missing_columns))

    seen_keys = {}
    for row in data.rows:
        source = row.get("Source Row", "?")
        if all(row.get(col) == col for col in DEFAULT_COLUMNS):
            issues.append(f"Row {source}: repeated header row found in data.")
            continue

        for col in REQUIRED_COLUMNS:
            if not row.get(col):
                issues.append(f"Row {source}: missing required field {col!r}.")

        if row.get("Variant", "").lower() == "jumbo":
            issues.append(f"Row {source}: jumbo cards are out of scope.")

        key = (
            row.get("Release Set", ""),
            row.get("Product", ""),
            row.get("Card Name", ""),
            row.get("Card Number", ""),
            row.get("Variant", ""),
            row.get("Region", ""),
        )
        if all(key):
            if key in seen_keys:
                issues.append(
                    f"Rows {seen_keys[key]} and {source}: duplicate exact card/product key."
                )
            else:
                seen_keys[key] = source
    return issues


def write_csv(path: Path, headers: Sequence[str], rows: Iterable[Dict[str, str]]) -> None:
    fieldnames = ["Source Row"] + list(headers)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def card_identity(row: Dict[str, str]) -> Tuple[str, str, str]:
    return (row.get("Card Name", ""), row.get("Card Number", ""), row.get("Variant", ""))


def is_standard_variant(row: Dict[str, object]) -> bool:
    return str(row.get("Variant", "")).strip().lower() == "standard"


def purchase_label(row: Dict[str, str]) -> str:
    release_set = row.get("Release Set", "")
    product = row.get("Product", "")
    if release_set and product:
        return f"{release_set} - {product}"
    return product or release_set


COLLECTION_CATEGORIES = [
    ("A", "Illustration Rare"),
    ("B", "Ultra Rare"),
    ("C", "Special Illustration Rare"),
    ("D", "Hyper Rare"),
    ("E", "Stamped Cards"),
    ("F", "Staff Cards"),
    ("G", "Promo Cards"),
    ("H", "MCAP Cards"),
    ("I", "Prize Pack Cards"),
    ("J", "League and Championship Cards"),
    ("K", "Japanese Promos"),
    ("Z", "Other Cards"),
]

CATEGORY_LABELS = {code: label for code, label in COLLECTION_CATEGORIES}
CATEGORY_ORDER = {code: index for index, (code, _) in enumerate(COLLECTION_CATEGORIES)}
CATEGORY_CODES_BY_LABEL = {
    label.lower(): code for code, label in COLLECTION_CATEGORIES
}


def joined_value(row: Dict[str, object], field: str) -> str:
    value = row.get(field, "")
    if isinstance(value, list):
        return " ".join(str(item) for item in value)
    return str(value)


def collection_category_code(row: Dict[str, object]) -> str:
    override = str(
        row.get("Override Category", "") or row.get("Category Override", "")
    ).strip()
    if override:
        if override.upper() in CATEGORY_LABELS:
            return override.upper()
        label_code = CATEGORY_CODES_BY_LABEL.get(override.lower())
        if label_code:
            return label_code

    variant = str(row.get("Variant", "")).strip().lower()
    card_number = str(row.get("Card Number", "")).strip().upper()
    sources = joined_value(row, "Sources").lower()
    notes = joined_value(row, "Notes").lower()
    combined = " ".join([variant, card_number.lower(), sources, notes])

    if "special illustration rare" in variant:
        return "C"
    if "illustration rare" in variant:
        return "A"
    if "ultra rare" in variant:
        return "B"
    if "hyper rare" in variant:
        return "D"

    if "play pokemon stamp" in variant or "prize pack" in combined:
        return "I"
    if "staff" in variant:
        return "F"

    league_terms = [
        "regional",
        "championship",
        "tcg gym",
        "master ball league",
        "ultra ball league",
    ]
    if any(term in combined for term in league_terms):
        return "J"

    japanese_terms = ["japanese", "battle partners", "sv-p"]
    if any(term in combined for term in japanese_terms) or "/SV-P" in card_number:
        return "K"

    if "build & battle" in sources and variant == "black star promo":
        return "E"
    if "stamp" in variant and "pokemon center" not in variant:
        return "E"

    if card_number.startswith("SVP"):
        return "G"

    mcap_terms = [
        "cosmos holo",
        "line holo",
        "mirage holo",
        "cracked ice",
        "set holo",
        "snowflake",
        "non holo",
        "non-holo",
        "premium collection",
        "tournament collection",
        "holiday calendar",
        "checklane",
        "blister",
        "box",
        "tin",
    ]
    if any(term in combined for term in mcap_terms):
        return "H"

    return "Z"


def collection_category_label(row: Dict[str, object]) -> str:
    return CATEGORY_LABELS[collection_category_code(row)]


def numeric_card_number(row: Dict[str, object]) -> Tuple[str, int, str]:
    card_number = str(row.get("Card Number", ""))
    match = re.match(r"([A-Z]{2,5})(\d+)", card_number)
    if match:
        return (match.group(1), int(match.group(2)), card_number)
    return ("ZZZ", 99999, card_number)


def collection_sort_key(row: Dict[str, object]) -> Tuple[int, str, int, str, str, str]:
    category_code = collection_category_code(row)
    number_prefix, number, card_number = numeric_card_number(row)
    return (
        CATEGORY_ORDER.get(category_code, CATEGORY_ORDER["Z"]),
        number_prefix,
        number,
        card_number,
        str(row.get("Card Name", "")),
        str(row.get("Variant", "")),
    )


def card_number_sort_key(row: Dict[str, object]) -> Tuple[str, int, str, int, str, str]:
    number_prefix, number, card_number = numeric_card_number(row)
    return (
        number_prefix,
        number,
        card_number,
        CATEGORY_ORDER.get(collection_category_code(row), CATEGORY_ORDER["Z"]),
        str(row.get("Card Name", "")),
        str(row.get("Variant", "")),
    )


def unique_print_rows(
    rows: Sequence[Dict[str, str]], sort_mode: str = "collection"
) -> List[Dict[str, object]]:
    grouped: Dict[Tuple[str, str, str], Dict[str, object]] = {}
    for row in rows:
        if is_standard_variant(row):
            continue

        key = card_identity(row)
        if key not in grouped:
            grouped[key] = {
                "Card Name": row.get("Card Name", ""),
                "Card Number": row.get("Card Number", ""),
                "Variant": row.get("Variant", ""),
                "Region": row.get("Region", ""),
                "Override Set Code": row.get("Override Set Code", "")
                or row.get("Set Code Override", ""),
                "Override Category": row.get("Override Category", "")
                or row.get("Category Override", ""),
                "Sources": [],
                "Source Rows": [],
                "Notes": [],
            }

        entry = grouped[key]
        sources = entry["Sources"]
        label = purchase_label(row)
        if label and label not in sources:
            sources.append(label)

        source_rows = entry["Source Rows"]
        source_row = row.get("Source Row", "")
        if source_row and source_row not in source_rows:
            source_rows.append(source_row)

        notes = entry["Notes"]
        note = row.get("Notes", "")
        if note and note not in notes:
            notes.append(note)

    for entry in grouped.values():
        entry["Category"] = collection_category_label(entry)

    sort_key = card_number_sort_key if sort_mode == "card_number" else collection_sort_key
    return sorted(grouped.values(), key=sort_key)


def write_unique_print_csv(path: Path, rows: Sequence[Dict[str, object]]) -> None:
    fieldnames = [
        "Category",
        "Card Name",
        "Card Number",
        "Variant",
        "Region",
        "Purchase Options",
        "Source Rows",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "Category": row.get("Category", ""),
                    "Card Name": row.get("Card Name", ""),
                    "Card Number": row.get("Card Number", ""),
                    "Variant": row.get("Variant", ""),
                    "Region": row.get("Region", ""),
                    "Purchase Options": " | ".join(row.get("Sources", [])),
                    "Source Rows": " | ".join(row.get("Source Rows", [])),
                }
            )


def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_") or "blank"


def card_set_code(card_number: str) -> str:
    card_number = card_number.strip()
    compact_match = re.match(r"([A-Z]{2,5})(?=\d)", card_number)
    if compact_match:
        return compact_match.group(1)

    named_match = re.match(r"(.+?)\s+\d+$", card_number)
    if named_match:
        return re.sub(r"[^A-Z0-9]+", "_", named_match.group(1).upper()).strip("_")

    return ""


def grouped_rows(
    rows: Sequence[Dict[str, str]], field: str
) -> List[Tuple[str, List[Dict[str, str]]]]:
    groups: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        value = row.get(field, "").strip()
        if not value:
            continue
        groups.setdefault(value, []).append(row)
    return sorted(groups.items(), key=lambda item: item[0].lower())


def grouped_rows_by_card_code(
    rows: Sequence[Dict[str, str]]
) -> List[Tuple[str, List[Dict[str, str]]]]:
    groups: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        code = card_set_code(row.get("Card Number", ""))
        if not code:
            continue
        groups.setdefault(code, []).append(row)
    return sorted(groups.items(), key=lambda item: item[0])


def card_lines(row: Dict[str, object]) -> List[Tuple[str, str]]:
    lines: List[Tuple[str, str]] = [("name", str(row.get("Card Name", "")))]

    number = str(row.get("Card Number", ""))
    if number:
        lines.append(("number", number))

    variant = str(row.get("Variant", ""))
    if variant and variant.lower() != "black star promo":
        lines.append(("variant", variant))

    region = str(row.get("Region", ""))
    if region and region not in {"US", "US/UK", "English"}:
        lines.append(("region", f"({region})"))

    return [(kind, line) for kind, line in lines if line]


def line_classes(kind: str, line: str) -> str:
    classes = [f"line-{kind}"]
    if kind in {"name", "variant"}:
        length = len(line)
        if length > 40:
            classes.append("line-tightest")
        elif length > 30:
            classes.append("line-tighter")
        elif length > 22:
            classes.append("line-tight")
    return " ".join(classes)


def render_print_html(
    rows: Sequence[Dict[str, str]], title: str, sort_mode: str = "collection"
) -> str:
    print_rows = unique_print_rows(rows, sort_mode=sort_mode)
    cards = []
    for row in print_rows:
        lines = "\n".join(
            f'<div class="line {line_classes(kind, line)}">{html.escape(line)}</div>'
            for kind, line in card_lines(row)
        )
        tooltip = " | ".join(
            part
            for part in [
                f"Rows {', '.join(row.get('Source Rows', []))}",
                row.get("Card Number", ""),
                " / ".join(row.get("Notes", [])),
            ]
            if part
        )
        cards.append(f'<section class="card" title="{html.escape(tooltip)}">{lines}</section>')

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    @page {{
      size: letter;
      margin: 0.5in;
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      background: #f3f4f6;
      color: #111;
      font-family: "Agency FB", "Arial Narrow", "Roboto Condensed", "Impact", sans-serif;
    }}
    .sheet {{
      width: 8.5in;
      min-height: 11in;
      margin: 0 auto;
      padding: 0.5in;
      background: white;
      display: grid;
      grid-template-columns: repeat(3, 2.5in);
      grid-auto-rows: 3.5in;
      align-content: start;
    }}
    .card {{
      width: 2.5in;
      height: 3.5in;
      border: 1px dotted #d9dee7;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: 0.16in;
      padding: 0.24in 0.14in;
      text-align: center;
      break-inside: avoid;
      page-break-inside: avoid;
      overflow: hidden;
    }}
    .line {{
      width: 100%;
      overflow-wrap: normal;
      text-wrap: balance;
      line-height: 1;
      font-weight: 900;
      letter-spacing: 0;
    }}
    .line-name {{
      font-size: 25pt;
      max-width: 2.18in;
    }}
    .line-number {{
      font-size: 11pt;
      letter-spacing: 0;
    }}
    .line-variant {{
      font-size: 22pt;
      max-width: 2.2in;
    }}
    .line-region {{
      font-size: 13pt;
    }}
    .line-tight {{
      font-size: 21pt;
    }}
    .line-tighter {{
      font-size: 18pt;
    }}
    .line-tightest {{
      font-size: 15pt;
      line-height: 1.04;
    }}
    @media print {{
      body {{
        background: white;
      }}
      .sheet {{
        margin: 0;
      }}
    }}
  </style>
</head>
<body>
  <main class="sheet">
    {"".join(cards)}
  </main>
</body>
</html>
"""


def render_index_html(
    source_name: str,
    row_count: int,
    printable_count: int,
    release_pages: Sequence[Tuple[str, str, int]],
    card_code_pages: Sequence[Tuple[str, str, int]],
) -> str:
    def render_links(items: Sequence[Tuple[str, str, int]]) -> str:
        links = []
        for label, href, count in items:
            links.append(
                '<a class="link" href="{}"><span>{}</span><strong>{}</strong></a>'.format(
                    html.escape(href),
                    html.escape(label),
                    count,
                )
            )
        return "\n".join(links)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(source_name)} Print Index</title>
  <style>
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      background: #f6f7f9;
      color: #171717;
      font-family: Arial, sans-serif;
    }}
    main {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 28px;
      letter-spacing: 0;
    }}
    .meta {{
      margin: 0 0 24px;
      color: #555;
      font-size: 14px;
    }}
    .actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 28px;
    }}
    .button {{
      display: inline-flex;
      align-items: center;
      min-height: 40px;
      padding: 0 14px;
      border: 1px solid #cfd5df;
      border-radius: 6px;
      background: white;
      color: #111;
      text-decoration: none;
      font-weight: 700;
      font-size: 14px;
    }}
    section {{
      margin-top: 30px;
    }}
    h2 {{
      margin: 0 0 12px;
      font-size: 18px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
      gap: 8px;
    }}
    .link {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      min-height: 44px;
      padding: 10px 12px;
      border: 1px solid #d9dee7;
      border-radius: 6px;
      background: white;
      color: #111;
      text-decoration: none;
      font-size: 14px;
      font-weight: 700;
    }}
    .link strong {{
      color: #555;
      font-size: 12px;
      white-space: nowrap;
    }}
  </style>
</head>
<body>
  <main>
    <h1>{html.escape(source_name)} Print Index</h1>
    <p class="meta">{row_count} source rows, {printable_count} unique printable placeholders.</p>
    <div class="actions">
      <a class="button" href="placeholders_print.html">Print Everything</a>
      <a class="button" href="validation_report.md">Validation Report</a>
      <a class="button" href="placeholders.printable_unique.csv">Printable CSV</a>
    </div>
    <section>
      <h2>By Release Block</h2>
      <div class="grid">
        {render_links(release_pages)}
      </div>
    </section>
    <section>
      <h2>By Card Set Code</h2>
      <div class="grid">
        {render_links(card_code_pages)}
      </div>
    </section>
  </main>
</body>
</html>
"""


def write_grouped_print_pages(
    output_dir: Path, rows: Sequence[Dict[str, str]], source_stem: str
) -> Tuple[List[Tuple[str, str, int]], List[Tuple[str, str, int]]]:
    release_dir = output_dir / "by_release_block"
    code_dir = output_dir / "by_card_code"
    release_dir.mkdir(parents=True, exist_ok=True)
    code_dir.mkdir(parents=True, exist_ok=True)
    for directory in (release_dir, code_dir):
        for page in directory.glob("*.html"):
            page.unlink()

    release_pages: List[Tuple[str, str, int]] = []
    for label, group in grouped_rows(rows, "Release Set"):
        filename = f"{slugify(label)}.html"
        title = f"{source_stem}: {label}"
        (release_dir / filename).write_text(render_print_html(group, title), encoding="utf-8")
        release_pages.append(
            (label, f"by_release_block/{filename}", len(unique_print_rows(group)))
        )

    code_pages: List[Tuple[str, str, int]] = []
    for label, group in grouped_rows_by_card_code(rows):
        filename = f"{slugify(label)}.html"
        title = f"{source_stem}: {label}"
        (code_dir / filename).write_text(
            render_print_html(group, title, sort_mode="card_number"),
            encoding="utf-8",
        )
        code_pages.append((label, f"by_card_code/{filename}", len(unique_print_rows(group))))

    return release_pages, code_pages


def write_report(path: Path, data: WorkbookRows, issues: Sequence[str]) -> None:
    lines = [
        "# Placeholder Build Report",
        "",
        f"Source: {data.source_name}",
        f"Rows exported: {len(data.rows)}",
        f"Columns: {', '.join(data.headers)}",
        "",
        "## Validation",
        "",
    ]
    if issues:
        lines.extend(f"- {issue}" for issue in issues)
    else:
        lines.append("No validation issues found.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=None,
        help="Input .csv or .xlsx file. Defaults to placeholders.csv when present.",
    )
    parser.add_argument(
        "--extra-input",
        action="append",
        default=[],
        help="Additional .csv or .xlsx source to merge into the same output.",
    )
    parser.add_argument("--output-dir", default="output", help="Directory for generated files.")
    parser.add_argument("--header-row", type=int, default=2, help="Header row in the first sheet.")
    args = parser.parse_args(argv)

    source = Path(args.input) if args.input else Path("placeholders.csv")
    if not source.exists() and args.input is None:
        source = Path("Placeholders.xlsx")
    output_dir = Path(args.output_dir)
    if not source.exists():
        print(f"Input workbook not found: {source}", file=sys.stderr)
        return 2

    sources = [read_source(source, header_row=args.header_row)]
    for extra in args.extra_input:
        sources.append(read_source(Path(extra), header_row=args.header_row))
    data = combine_sources(sources) if len(sources) > 1 else sources[0]
    issues = validate(data)
    output_dir.mkdir(parents=True, exist_ok=True)

    write_csv(output_dir / "placeholders.normalized.csv", data.headers, data.rows)
    printable_rows = unique_print_rows(data.rows)
    write_unique_print_csv(output_dir / "placeholders.printable_unique.csv", printable_rows)
    (output_dir / "placeholders.json").write_text(
        json.dumps(data.rows, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    (output_dir / "placeholders_print.html").write_text(
        render_print_html(data.rows, source.stem),
        encoding="utf-8",
    )
    release_pages, card_code_pages = write_grouped_print_pages(
        output_dir, data.rows, source.stem
    )
    (output_dir / "index.html").write_text(
        render_index_html(
            source.stem,
            len(data.rows),
            len(printable_rows),
            release_pages,
            card_code_pages,
        ),
        encoding="utf-8",
    )
    write_report(output_dir / "validation_report.md", data, issues)

    print(f"Read {len(data.rows)} rows from {source}")
    print(f"Prepared {len(printable_rows)} unique printable card placeholders")
    print(f"Wrote exports to {output_dir}")
    if issues:
        print(f"Validation found {len(issues)} issue(s); see validation_report.md")
    else:
        print("Validation passed with no issues.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
