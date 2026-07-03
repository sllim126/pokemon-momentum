#!/usr/bin/env python3
"""Build hostable master-set checklist pages from placeholder CSV sources."""

from __future__ import annotations

import argparse
import csv
import html
import json
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from build_placeholders import (
    COLLECTION_CATEGORIES,
    WorkbookRows,
    card_set_code,
    combine_sources,
    collection_category_label,
    collection_sort_key,
    is_standard_variant,
    read_source,
    slugify,
    unique_print_rows,
)


OVERRIDE_COLUMNS = [
    "Card Name",
    "Card Number",
    "Variant",
    "Region",
    "Override Set Code",
    "Override Category",
    "Exclude",
    "Review Notes",
]


def read_set_catalog(path: Path, era: str) -> Dict[str, Dict[str, str]]:
    catalog: Dict[str, Dict[str, str]] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("Era", "") != era:
                continue
            code = (row.get("Code") or "").strip()
            if code:
                catalog[code] = {
                    "Set": row.get("Set", ""),
                    "Release Date": row.get("Release Date", ""),
                    "Notes": row.get("Notes", ""),
                }

            if row.get("Set") == "Scarlet & Violet Black Star Promos":
                catalog["SVP"] = {
                    "Set": row.get("Set", ""),
                    "Release Date": row.get("Release Date", ""),
                    "Notes": row.get("Notes", ""),
                }
    return catalog


def source_rows(paths: Sequence[Path], header_row: int) -> WorkbookRows:
    sources = [read_source(path, header_row=header_row) for path in paths if path.exists()]
    return combine_sources(sources) if len(sources) > 1 else sources[0]


def override_key(row: Dict[str, object]) -> Tuple[str, str, str, str]:
    return (
        str(row.get("Card Name", "")).strip().lower(),
        str(row.get("Card Number", "")).strip().upper(),
        str(row.get("Variant", "")).strip().lower(),
        str(row.get("Region", "")).strip().lower(),
    )


def read_overrides(path: Path) -> Dict[Tuple[str, str, str, str], Dict[str, str]]:
    if not path.exists():
        return {}

    overrides: Dict[Tuple[str, str, str, str], Dict[str, str]] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not any((row.get(column) or "").strip() for column in OVERRIDE_COLUMNS):
                continue
            overrides[override_key(row)] = {
                "Override Set Code": (row.get("Override Set Code") or "").strip().upper(),
                "Override Category": (row.get("Override Category") or "").strip(),
                "Exclude": (row.get("Exclude") or "").strip(),
                "Review Notes": (row.get("Review Notes") or "").strip(),
            }
    return overrides


def apply_overrides(
    rows: Sequence[Dict[str, str]],
    overrides: Dict[Tuple[str, str, str, str], Dict[str, str]],
) -> List[Dict[str, str]]:
    updated_rows: List[Dict[str, str]] = []
    for row in rows:
        updated = dict(row)
        override = overrides.get(override_key(updated))
        if override:
            for field in ["Override Set Code", "Override Category", "Exclude"]:
                if override.get(field):
                    updated[field] = override[field]
            if override.get("Review Notes"):
                notes = updated.get("Notes", "")
                updated["Notes"] = (
                    f"{notes} | Review: {override['Review Notes']}"
                    if notes
                    else f"Review: {override['Review Notes']}"
                )
        updated_rows.append(updated)
    return updated_rows


def write_review_queue(
    path: Path, rows: Sequence[Dict[str, str]], catalog: Dict[str, Dict[str, str]]
) -> List[Dict[str, object]]:
    fieldnames = [
        "Generated Set Code",
        "Printed Set Code",
        "Category",
        *OVERRIDE_COLUMNS,
        "Release Set",
        "Product",
        "Source Rows",
    ]
    queue_rows = []
    for row in unique_print_rows(rows):
        queue_rows.append(
            {
                "Generated Set Code": checklist_set_code(row, catalog),
                "Printed Set Code": card_set_code(str(row.get("Card Number", ""))),
                "Category": collection_category_label(row),
                "Card Name": row.get("Card Name", ""),
                "Card Number": row.get("Card Number", ""),
                "Variant": row.get("Variant", ""),
                "Region": row.get("Region", ""),
                "Override Set Code": row.get("Override Set Code", ""),
                "Override Category": row.get("Override Category", ""),
                "Exclude": "",
                "Review Notes": "",
                "Release Set": "",
                "Product": " | ".join(row.get("Sources", [])),
                "Source Rows": " | ".join(row.get("Source Rows", [])),
            }
        )

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(queue_rows)
    return queue_rows


def catalog_set_codes(catalog: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    return {info.get("Set", ""): code for code, info in catalog.items() if info.get("Set")}


def checklist_set_code(row: Dict[str, str], catalog: Dict[str, Dict[str, str]]) -> str:
    override = row.get("Override Set Code", "").strip().upper()
    if override:
        return override

    release_set = row.get("Release Set", "").strip()
    release_set_codes = catalog_set_codes(catalog)
    release_code = release_set_codes.get(release_set, "")
    card_code = card_set_code(row.get("Card Number", ""))
    if (
        release_code
        and "prize pack" not in release_set.lower()
        and (card_code == release_code or card_code == "SVP" or not card_code)
    ):
        return release_code
    return card_code


def is_in_scope(row: Dict[str, str], catalog: Dict[str, Dict[str, str]]) -> bool:
    variant = row.get("Variant", "").strip().lower()
    if row.get("Exclude", "").strip().lower() in {"1", "true", "yes", "y", "x"}:
        return False
    if variant == "jumbo" or is_standard_variant(row):
        return False
    return checklist_set_code(row, catalog) in catalog


def rows_by_code(
    rows: Sequence[Dict[str, str]], catalog: Dict[str, Dict[str, str]]
) -> Dict[str, List[Dict[str, str]]]:
    allowed = set(catalog)
    grouped: Dict[str, List[Dict[str, str]]] = {code: [] for code in allowed}
    for row in rows:
        code = checklist_set_code(row, catalog)
        if code in allowed and is_in_scope(row, catalog):
            grouped.setdefault(code, []).append(row)
    return {code: group for code, group in grouped.items() if group}


def checklist_rows(rows: Sequence[Dict[str, str]]) -> List[Dict[str, object]]:
    entries = unique_print_rows(rows)
    for entry in entries:
        entry["Key"] = checklist_key(entry)
    return entries


def checklist_key(row: Dict[str, object]) -> str:
    pieces = [
        str(row.get("Card Number", "")),
        str(row.get("Card Name", "")),
        str(row.get("Variant", "")),
        str(row.get("Region", "")),
    ]
    return slugify(" ".join(pieces))


def sorted_checklist_rows(rows: Sequence[Dict[str, str]]) -> List[Dict[str, object]]:
    entries = checklist_rows(rows)
    return sorted(entries, key=collection_sort_key)


def write_checklist_csv(
    path: Path, rows: Sequence[Dict[str, object]], include_set_fields: bool = False
) -> None:
    fieldnames = [
        "Owned",
        *([] if not include_set_fields else ["Set Code", "Set"]),
        "Category",
        "Card Number",
        "Card Name",
        "Variant",
        "Region",
        "Purchase Options",
        "Notes",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            export_row = {
                "Owned": "",
                "Category": row.get("Category", ""),
                "Card Number": row.get("Card Number", ""),
                "Card Name": row.get("Card Name", ""),
                "Variant": row.get("Variant", ""),
                "Region": row.get("Region", ""),
                "Purchase Options": " | ".join(row.get("Sources", [])),
                "Notes": " | ".join(row.get("Notes", [])),
            }
            if include_set_fields:
                export_row["Set Code"] = row.get("Set Code", "")
                export_row["Set"] = row.get("Set", "")
            writer.writerow(export_row)


def render_sources(row: Dict[str, object]) -> str:
    sources = row.get("Sources", [])
    if not sources:
        return ""
    return "; ".join(str(source) for source in sources)


def render_set_page(
    code: str,
    set_info: Dict[str, str],
    rows: Sequence[Dict[str, object]],
    csv_href: str,
) -> str:
    rows_html = []
    current_category = ""
    for row in rows:
        category = str(row.get("Category", "Other Cards"))
        if category != current_category:
            current_category = category
            rows_html.append(
                '<tr class="category-row"><th colspan="5">{}</th></tr>'.format(
                    html.escape(category)
                )
            )

        key = str(row.get("Key", ""))
        label = " ".join(
            part
            for part in [
                str(row.get("Card Number", "")),
                str(row.get("Card Name", "")),
                str(row.get("Variant", "")),
            ]
            if part
        )
        rows_html.append(
            """<tr>
        <td class="owned"><input type="checkbox" data-key="{key}" aria-label="{label}"></td>
        <td class="number">{number}</td>
        <td class="name">{name}</td>
        <td class="variant">{variant}</td>
        <td class="sources">{sources}</td>
      </tr>""".format(
                key=html.escape(key),
                label=html.escape(label),
                number=html.escape(str(row.get("Card Number", ""))),
                name=html.escape(str(row.get("Card Name", ""))),
                variant=html.escape(str(row.get("Variant", ""))),
                sources=html.escape(render_sources(row)),
            )
        )

    title = f"{code} - {set_info.get('Set', code)}"
    release_date = set_info.get("Release Date", "")
    payload = json.dumps({"code": code}, ensure_ascii=True)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)} Checklist</title>
  <style>
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      background: #f7f8fa;
      color: #151515;
      font-family: Arial, sans-serif;
      font-size: 14px;
    }}
    main {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 24px 18px 44px;
    }}
    header {{
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
    }}
    h1 {{
      margin: 0 0 5px;
      font-size: 26px;
      letter-spacing: 0;
    }}
    .meta {{
      margin: 0;
      color: #555;
    }}
    .actions {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: flex-end;
    }}
    a.button,
    button {{
      min-height: 36px;
      padding: 0 12px;
      border: 1px solid #cfd5df;
      border-radius: 6px;
      background: white;
      color: #111;
      font: inherit;
      font-weight: 700;
      text-decoration: none;
      cursor: pointer;
    }}
    .progress {{
      height: 12px;
      margin-bottom: 16px;
      border: 1px solid #cfd5df;
      border-radius: 999px;
      overflow: hidden;
      background: white;
    }}
    .bar {{
      height: 100%;
      width: 0;
      background: #33745b;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: white;
      border: 1px solid #d9dee7;
    }}
    th,
    td {{
      padding: 9px 10px;
      border-bottom: 1px solid #e4e8ef;
      vertical-align: top;
      text-align: left;
    }}
    th {{
      background: #eef1f5;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0;
    }}
    .category-row th {{
      padding: 11px 10px;
      background: #dfe5ed;
      color: #111;
      font-size: 13px;
    }}
    .owned {{
      width: 44px;
      text-align: center;
    }}
    .number {{
      width: 100px;
      font-weight: 700;
      white-space: nowrap;
    }}
    .name {{
      width: 230px;
      font-weight: 700;
    }}
    .variant {{
      width: 210px;
    }}
    .sources {{
      color: #444;
      font-size: 13px;
    }}
    input[type="checkbox"] {{
      width: 18px;
      height: 18px;
    }}
    tr.is-owned td {{
      background: #f2f7f4;
    }}
    @media print {{
      body {{
        background: white;
        font-size: 11px;
      }}
      main {{
        max-width: none;
        padding: 0;
      }}
      .actions,
      .progress {{
        display: none;
      }}
      table {{
        border-color: #999;
      }}
      th,
      td {{
        padding: 5px 6px;
        border-color: #bbb;
      }}
      input[type="checkbox"] {{
        appearance: none;
        width: 12px;
        height: 12px;
        border: 1px solid #111;
      }}
    }}
  </style>
</head>
<body>
  <main data-page='{html.escape(payload)}'>
    <header>
      <div>
        <h1>{html.escape(title)}</h1>
        <p class="meta">{len(rows)} checklist rows{html.escape(f" - {release_date}" if release_date else "")}</p>
      </div>
      <div class="actions">
        <a class="button" href="../index.html">Index</a>
        <a class="button" href="{html.escape(csv_href)}">CSV</a>
        <button type="button" id="print-button">Print</button>
        <button type="button" id="clear-button">Clear</button>
      </div>
    </header>
    <div class="progress" aria-hidden="true"><div class="bar" id="bar"></div></div>
    <table>
      <thead>
        <tr>
          <th class="owned">Have</th>
          <th class="number">Number</th>
          <th class="name">Card</th>
          <th class="variant">Variant</th>
          <th class="sources">Sources</th>
        </tr>
      </thead>
      <tbody>
      {"".join(rows_html)}
      </tbody>
    </table>
  </main>
  <script>
    const page = JSON.parse(document.querySelector("main").dataset.page);
    const boxes = [...document.querySelectorAll("input[type=checkbox]")];
    const keyPrefix = `tcg-checklist:${{page.code}}:`;
    const bar = document.getElementById("bar");

    function storageKey(box) {{
      return keyPrefix + box.dataset.key;
    }}

    function updateProgress() {{
      const owned = boxes.filter((box) => box.checked).length;
      const pct = boxes.length ? (owned / boxes.length) * 100 : 0;
      bar.style.width = pct + "%";
    }}

    boxes.forEach((box) => {{
      box.checked = localStorage.getItem(storageKey(box)) === "1";
      box.closest("tr").classList.toggle("is-owned", box.checked);
      box.addEventListener("change", () => {{
        if (box.checked) {{
          localStorage.setItem(storageKey(box), "1");
        }} else {{
          localStorage.removeItem(storageKey(box));
        }}
        box.closest("tr").classList.toggle("is-owned", box.checked);
        updateProgress();
      }});
    }});

    document.getElementById("print-button").addEventListener("click", () => window.print());
    document.getElementById("clear-button").addEventListener("click", () => {{
      boxes.forEach((box) => {{
        box.checked = false;
        box.closest("tr").classList.remove("is-owned");
        localStorage.removeItem(storageKey(box));
      }});
      updateProgress();
    }});
    updateProgress();
  </script>
</body>
</html>
"""


def render_index(
    pages: Sequence[Tuple[str, str, str, int, str]], era: str
) -> str:
    page_links = []
    for code, set_name, release_date, count, href in pages:
        page_links.append(
            """<a class="set-link" href="{href}">
        <strong>{code}</strong>
        <span>{name}</span>
        <em>{count} rows{date}</em>
      </a>""".format(
                href=html.escape(href),
                code=html.escape(code),
                name=html.escape(set_name),
                count=count,
                date=html.escape(f" - {release_date}" if release_date else ""),
            )
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(era)} Checklist Index</title>
  <style>
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      background: #f7f8fa;
      color: #151515;
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
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
      gap: 10px;
    }}
    .set-link {{
      display: grid;
      gap: 4px;
      min-height: 86px;
      padding: 13px 14px;
      border: 1px solid #d9dee7;
      border-radius: 6px;
      background: white;
      color: #111;
      text-decoration: none;
    }}
    .set-link strong {{
      font-size: 18px;
    }}
    .set-link span {{
      font-weight: 700;
    }}
    .set-link em {{
      color: #555;
      font-size: 12px;
      font-style: normal;
    }}
  </style>
</head>
<body>
  <main>
    <h1>{html.escape(era)} Master Set Checklists</h1>
    <p class="meta">Generated from the current placeholder, promo, and prize pack source files.</p>
    <p><a class="set-link" href="review.html"><strong>Review</strong><span>Corrections and missing cards</span><em>Export override CSVs</em></a></p>
    <div class="grid">
      {"".join(page_links)}
    </div>
  </main>
</body>
</html>
"""


def read_csv_records(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def render_review_app(
    queue_rows: Sequence[Dict[str, object]],
    overrides: Sequence[Dict[str, str]],
    additions: Sequence[Dict[str, str]],
    pages: Sequence[Tuple[str, str, str, int, str]],
) -> str:
    set_options = [
        {"code": code, "name": name, "href": href}
        for code, name, _release_date, _count, href in pages
    ]
    categories = [label for _code, label in COLLECTION_CATEGORIES]
    payload = json.dumps(
        {
            "queue": list(queue_rows),
            "overrides": list(overrides),
            "additions": list(additions),
            "sets": set_options,
            "categories": categories,
        },
        ensure_ascii=False,
    )
    safe_payload = (
        payload.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Checklist Review</title>
  <style>
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      background: #f5f6f8;
      color: #151515;
      font-family: Arial, sans-serif;
      font-size: 14px;
    }}
    main {{
      max-width: 1320px;
      margin: 0 auto;
      padding: 22px 18px 42px;
    }}
    header {{
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 16px;
    }}
    h1 {{
      margin: 0 0 5px;
      font-size: 26px;
      letter-spacing: 0;
    }}
    h2 {{
      margin: 0 0 10px;
      font-size: 17px;
      letter-spacing: 0;
    }}
    .meta {{
      margin: 0;
      color: #555;
    }}
    .top-actions,
    .filters,
    .form-grid,
    .button-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: end;
    }}
    .layout {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) 390px;
      gap: 14px;
      align-items: start;
    }}
    section,
    aside {{
      background: white;
      border: 1px solid #d9dee7;
      border-radius: 6px;
      padding: 14px;
    }}
    label {{
      display: grid;
      gap: 4px;
      color: #444;
      font-size: 12px;
      font-weight: 700;
    }}
    input,
    select,
    textarea,
    button,
    a.button {{
      min-height: 34px;
      border: 1px solid #cbd2dc;
      border-radius: 5px;
      background: white;
      color: #111;
      font: inherit;
    }}
    input,
    select,
    textarea {{
      padding: 6px 8px;
      min-width: 150px;
    }}
    textarea {{
      min-height: 64px;
      resize: vertical;
    }}
    button,
    a.button {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 0 11px;
      font-weight: 700;
      text-decoration: none;
      cursor: pointer;
    }}
    button.primary {{
      background: #285f4d;
      border-color: #285f4d;
      color: white;
    }}
    button.danger {{
      color: #8a1f1f;
    }}
    .filters {{
      margin-bottom: 10px;
    }}
    .table-wrap {{
      max-height: 68vh;
      overflow: auto;
      border: 1px solid #e1e5ec;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: white;
    }}
    th,
    td {{
      padding: 7px 8px;
      border-bottom: 1px solid #e4e8ef;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      position: sticky;
      top: 0;
      background: #eef1f5;
      z-index: 1;
      font-size: 12px;
      text-transform: uppercase;
    }}
    tr.is-selected td {{
      background: #edf6f1;
    }}
    td.small {{
      width: 76px;
      white-space: nowrap;
      font-weight: 700;
    }}
    td.actions {{
      width: 72px;
    }}
    .stack {{
      display: grid;
      gap: 14px;
    }}
    .form-grid {{
      align-items: stretch;
    }}
    .form-grid label {{
      flex: 1 1 160px;
    }}
    .form-grid .wide {{
      flex-basis: 100%;
    }}
    .count {{
      margin: 8px 0 0;
      color: #555;
      font-size: 12px;
    }}
    .list {{
      display: grid;
      gap: 5px;
      max-height: 180px;
      overflow: auto;
      margin-top: 8px;
      padding-right: 3px;
      color: #333;
      font-size: 12px;
    }}
    .list-item {{
      padding: 7px;
      border: 1px solid #e1e5ec;
      border-radius: 5px;
      background: #fafbfc;
    }}
    @media (max-width: 980px) {{
      .layout {{
        grid-template-columns: 1fr;
      }}
      .table-wrap {{
        max-height: none;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>Checklist Review</h1>
        <p class="meta">Filter cards, create corrections, add missing rows, then download CSVs for the generator.</p>
      </div>
      <div class="top-actions">
        <a class="button" href="index.html">Checklist Index</a>
        <a class="button" href="review_queue.csv">Review Queue CSV</a>
      </div>
    </header>
    <div class="layout">
      <section>
        <div class="filters">
          <label>Search
            <input id="search" type="search" placeholder="Name, number, source">
          </label>
          <label>Generated Set
            <select id="set-filter"></select>
          </label>
          <label>Category
            <select id="category-filter"></select>
          </label>
          <label>Moved
            <select id="moved-filter">
              <option value="">All</option>
              <option value="moved">Generated differs from printed</option>
              <option value="same">Generated matches printed</option>
            </select>
          </label>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Set</th>
                <th>Printed</th>
                <th>Category</th>
                <th>Card</th>
                <th>Variant</th>
                <th>Source</th>
                <th></th>
              </tr>
            </thead>
            <tbody id="queue-body"></tbody>
          </table>
        </div>
        <p class="count" id="queue-count"></p>
      </section>
      <aside class="stack">
        <section>
          <h2>Correction</h2>
          <div class="form-grid">
            <label>Card Name<input id="o-name"></label>
            <label>Card Number<input id="o-number"></label>
            <label>Variant<input id="o-variant"></label>
            <label>Region<input id="o-region"></label>
            <label>Override Set Code<input id="o-set" placeholder="JTG, PRE, SVP"></label>
            <label>Override Category<select id="o-category"></select></label>
            <label>Exclude<select id="o-exclude"><option value=""></option><option value="yes">yes</option></select></label>
            <label class="wide">Review Notes<textarea id="o-notes"></textarea></label>
          </div>
          <div class="button-row">
            <button class="primary" type="button" id="save-override">Save Correction</button>
            <button type="button" id="clear-override">Clear Form</button>
            <button class="danger" type="button" id="remove-override">Remove Saved</button>
          </div>
          <div class="button-row">
            <button type="button" id="download-overrides">Download checklist_overrides.csv</button>
          </div>
          <div class="list" id="override-list"></div>
        </section>
        <section>
          <h2>Missing Card</h2>
          <div class="form-grid">
            <label>Release Set<input id="a-release-set" placeholder="Journey Together"></label>
            <label>Release Date<input id="a-release-date" placeholder="2025-03-28"></label>
            <label>Product<input id="a-product"></label>
            <label>Product Type<input id="a-product-type"></label>
            <label>Card Name<input id="a-name"></label>
            <label>Card Number<input id="a-number"></label>
            <label>Variant<input id="a-variant"></label>
            <label>Region<input id="a-region" value="US/UK"></label>
            <label>Override Set Code<input id="a-set"></label>
            <label>Override Category<select id="a-category"></select></label>
            <label class="wide">Notes<textarea id="a-notes"></textarea></label>
          </div>
          <div class="button-row">
            <button class="primary" type="button" id="save-addition">Add Missing Row</button>
            <button type="button" id="clear-addition">Clear Form</button>
          </div>
          <div class="button-row">
            <button type="button" id="download-additions">Download checklist_additions.csv</button>
          </div>
          <div class="list" id="addition-list"></div>
        </section>
      </aside>
    </div>
  </main>
  <script id="review-data" type="application/json">{safe_payload}</script>
  <script>
    const data = JSON.parse(document.getElementById("review-data").textContent);
    const queue = data.queue;
    let overrides = data.overrides.filter((row) => Object.values(row).some(Boolean));
    let additions = data.additions.filter((row) => Object.values(row).some(Boolean));
    let selectedIndex = -1;

    const overrideFields = ["Card Name", "Card Number", "Variant", "Region", "Override Set Code", "Override Category", "Exclude", "Review Notes"];
    const additionFields = ["Release Set", "Release Date", "Product", "Product Type", "Card Name", "Card Number", "Variant", "Region", "Notes", "Override Set Code", "Override Category"];

    function el(id) {{
      return document.getElementById(id);
    }}

    function csvEscape(value) {{
      const text = value == null ? "" : String(value);
      return /[",\\n\\r]/.test(text) ? '"' + text.replaceAll('"', '""') + '"' : text;
    }}

    function toCsv(rows, fields) {{
      return [fields.join(","), ...rows.map((row) => fields.map((field) => csvEscape(row[field] || "")).join(","))].join("\\r\\n") + "\\r\\n";
    }}

    function download(filename, text) {{
      const blob = new Blob([text], {{ type: "text/csv;charset=utf-8" }});
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    }}

    function rowKey(row) {{
      return [row["Card Name"], row["Card Number"], row["Variant"], row.Region].map((part) => String(part || "").trim().toLowerCase()).join("||");
    }}

    function setupSelect(select, options, includeAll = true) {{
      select.innerHTML = "";
      if (includeAll) {{
        select.append(new Option("All", ""));
      }} else {{
        select.append(new Option("", ""));
      }}
      options.forEach((option) => select.append(new Option(option, option)));
    }}

    function setup() {{
      setupSelect(el("set-filter"), [...new Set(queue.map((row) => row["Generated Set Code"]).filter(Boolean))].sort());
      setupSelect(el("category-filter"), data.categories);
      setupSelect(el("o-category"), data.categories, false);
      setupSelect(el("a-category"), data.categories, false);
      ["search", "set-filter", "category-filter", "moved-filter"].forEach((id) => el(id).addEventListener("input", renderQueue));
      el("save-override").addEventListener("click", saveOverride);
      el("clear-override").addEventListener("click", clearOverrideForm);
      el("remove-override").addEventListener("click", removeOverride);
      el("download-overrides").addEventListener("click", () => download("checklist_overrides.csv", toCsv(overrides, overrideFields)));
      el("save-addition").addEventListener("click", saveAddition);
      el("clear-addition").addEventListener("click", clearAdditionForm);
      el("download-additions").addEventListener("click", () => download("checklist_additions.csv", toCsv(additions, additionFields)));
      renderQueue();
      renderLists();
    }}

    function filteredQueue() {{
      const search = el("search").value.trim().toLowerCase();
      const setCode = el("set-filter").value;
      const category = el("category-filter").value;
      const moved = el("moved-filter").value;
      return queue.filter((row) => {{
        const haystack = Object.values(row).join(" ").toLowerCase();
        if (search && !haystack.includes(search)) return false;
        if (setCode && row["Generated Set Code"] !== setCode) return false;
        if (category && row.Category !== category) return false;
        if (moved === "moved" && row["Generated Set Code"] === row["Printed Set Code"]) return false;
        if (moved === "same" && row["Generated Set Code"] !== row["Printed Set Code"]) return false;
        return true;
      }});
    }}

    function renderQueue() {{
      const rows = filteredQueue();
      const body = el("queue-body");
      body.innerHTML = "";
      rows.slice(0, 500).forEach((row) => {{
        const originalIndex = queue.indexOf(row);
        const tr = document.createElement("tr");
        if (originalIndex === selectedIndex) tr.classList.add("is-selected");
        tr.innerHTML = `
          <td class="small">${{row["Generated Set Code"] || ""}}</td>
          <td class="small">${{row["Printed Set Code"] || ""}}</td>
          <td>${{row.Category || ""}}</td>
          <td><strong>${{row["Card Name"] || ""}}</strong><br>${{row["Card Number"] || ""}}</td>
          <td>${{row.Variant || ""}}<br><span class="count">${{row.Region || ""}}</span></td>
          <td>${{row.Product || ""}}</td>
          <td class="actions"><button type="button">Edit</button></td>
        `;
        tr.querySelector("button").addEventListener("click", () => selectRow(originalIndex));
        body.appendChild(tr);
      }});
      el("queue-count").textContent = `${{rows.length}} matching rows${{rows.length > 500 ? " (showing first 500)" : ""}}.`;
    }}

    function selectRow(index) {{
      selectedIndex = index;
      const row = queue[index];
      el("o-name").value = row["Card Name"] || "";
      el("o-number").value = row["Card Number"] || "";
      el("o-variant").value = row.Variant || "";
      el("o-region").value = row.Region || "";
      el("o-set").value = row["Override Set Code"] || "";
      el("o-category").value = row["Override Category"] || "";
      el("o-exclude").value = row.Exclude || "";
      el("o-notes").value = row["Review Notes"] || "";
      const saved = overrides.find((override) => rowKey(override) === rowKey(row));
      if (saved) {{
        el("o-set").value = saved["Override Set Code"] || "";
        el("o-category").value = saved["Override Category"] || "";
        el("o-exclude").value = saved.Exclude || "";
        el("o-notes").value = saved["Review Notes"] || "";
      }}
      renderQueue();
    }}

    function overrideFromForm() {{
      return {{
        "Card Name": el("o-name").value.trim(),
        "Card Number": el("o-number").value.trim(),
        "Variant": el("o-variant").value.trim(),
        "Region": el("o-region").value.trim(),
        "Override Set Code": el("o-set").value.trim().toUpperCase(),
        "Override Category": el("o-category").value,
        "Exclude": el("o-exclude").value,
        "Review Notes": el("o-notes").value.trim(),
      }};
    }}

    function saveOverride() {{
      const row = overrideFromForm();
      if (!row["Card Name"] || !row["Card Number"] || !row.Variant || !row.Region) return;
      overrides = overrides.filter((override) => rowKey(override) !== rowKey(row));
      overrides.push(row);
      renderLists();
    }}

    function removeOverride() {{
      const row = overrideFromForm();
      overrides = overrides.filter((override) => rowKey(override) !== rowKey(row));
      renderLists();
    }}

    function clearOverrideForm() {{
      ["o-name", "o-number", "o-variant", "o-region", "o-set", "o-category", "o-exclude", "o-notes"].forEach((id) => el(id).value = "");
      selectedIndex = -1;
      renderQueue();
    }}

    function additionFromForm() {{
      return {{
        "Release Set": el("a-release-set").value.trim(),
        "Release Date": el("a-release-date").value.trim(),
        "Product": el("a-product").value.trim(),
        "Product Type": el("a-product-type").value.trim(),
        "Card Name": el("a-name").value.trim(),
        "Card Number": el("a-number").value.trim(),
        "Variant": el("a-variant").value.trim(),
        "Region": el("a-region").value.trim(),
        "Notes": el("a-notes").value.trim(),
        "Override Set Code": el("a-set").value.trim().toUpperCase(),
        "Override Category": el("a-category").value,
      }};
    }}

    function saveAddition() {{
      const row = additionFromForm();
      if (!row["Card Name"] || !row.Variant) return;
      additions.push(row);
      clearAdditionForm();
      renderLists();
    }}

    function clearAdditionForm() {{
      ["a-release-set", "a-release-date", "a-product", "a-product-type", "a-name", "a-number", "a-variant", "a-set", "a-category", "a-notes"].forEach((id) => el(id).value = "");
      el("a-region").value = "US/UK";
    }}

    function renderLists() {{
      el("override-list").innerHTML = overrides.map((row) => `<div class="list-item"><strong>${{row["Card Name"] || ""}}</strong> ${{row["Card Number"] || ""}}<br>${{row.Variant || ""}} -> ${{row["Override Set Code"] || "same set"}} / ${{row["Override Category"] || "auto category"}}${{row.Exclude ? " / exclude" : ""}}</div>`).join("") || '<div class="count">No pending corrections.</div>';
      el("addition-list").innerHTML = additions.map((row) => `<div class="list-item"><strong>${{row["Card Name"] || ""}}</strong> ${{row["Card Number"] || ""}}<br>${{row.Variant || ""}} -> ${{row["Override Set Code"] || row["Release Set"] || "auto set"}}</div>`).join("") || '<div class="count">No pending additions.</div>';
    }}

    setup();
  </script>
</body>
</html>
"""


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        action="append",
        default=[],
        help="Input CSV/XLSX source. Repeat for combined checklist data.",
    )
    parser.add_argument("--output-dir", default="checklists_sv")
    parser.add_argument("--era", default="Scarlet & Violet")
    parser.add_argument("--set-catalog", default="data/set_release_dates.csv")
    parser.add_argument("--overrides", default="data/checklist_overrides.csv")
    parser.add_argument("--additions", default="data/checklist_additions.csv")
    parser.add_argument("--header-row", type=int, default=2)
    args = parser.parse_args(argv)

    inputs = [Path(path) for path in args.input] or [
        Path("placeholders.csv"),
        Path("mega_placeholders.csv"),
        Path("prize_pack_series7.csv"),
        Path("prize_pack_series8.csv"),
        Path(args.additions),
    ]
    data = source_rows(inputs, args.header_row)
    override_records = read_csv_records(Path(args.overrides))
    addition_records = read_csv_records(Path(args.additions))
    overrides = read_overrides(Path(args.overrides))
    data.rows = apply_overrides(data.rows, overrides)
    catalog = read_set_catalog(Path(args.set_catalog), args.era)
    grouped = rows_by_code(data.rows, catalog)

    output_dir = Path(args.output_dir)
    by_set_dir = output_dir / "by_set"
    csv_dir = output_dir / "csv"
    by_set_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    for directory in (by_set_dir, csv_dir):
        for old_file in directory.glob("*.*"):
            old_file.unlink()

    queue_rows = write_review_queue(output_dir / "review_queue.csv", data.rows, catalog)

    pages: List[Tuple[str, str, str, int, str]] = []
    all_rows: List[Dict[str, object]] = []
    for code in sorted(grouped):
        rows = sorted_checklist_rows(grouped[code])
        set_info = catalog.get(code, {"Set": code, "Release Date": "", "Notes": ""})
        html_name = f"{slugify(code)}.html"
        csv_name = f"{slugify(code)}.csv"
        write_checklist_csv(csv_dir / csv_name, rows)
        (by_set_dir / html_name).write_text(
            render_set_page(code, set_info, rows, f"../csv/{csv_name}"),
            encoding="utf-8",
        )
        pages.append(
            (
                code,
                set_info.get("Set", code),
                set_info.get("Release Date", ""),
                len(rows),
                f"by_set/{html_name}",
            )
        )
        for row in rows:
            row_copy = dict(row)
            row_copy["Set Code"] = code
            row_copy["Set"] = set_info.get("Set", code)
            all_rows.append(row_copy)

    all_csv_name = f"{slugify(args.era)}_checklist_all.csv"
    write_checklist_csv(
        output_dir / all_csv_name,
        all_rows,
        include_set_fields=True,
    )
    (output_dir / "index.html").write_text(
        render_index(pages, args.era),
        encoding="utf-8",
    )
    (output_dir / "review.html").write_text(
        render_review_app(queue_rows, override_records, addition_records, pages),
        encoding="utf-8",
    )

    print(f"Read {len(data.rows)} rows from {data.source_name}.")
    print(f"Wrote {sum(page[3] for page in pages)} checklist rows across {len(pages)} set pages.")
    print(f"Open {output_dir / 'index.html'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
