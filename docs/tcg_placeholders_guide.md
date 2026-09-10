# TCG Placeholders Workflow Guide

This guide explains why the placeholder workflow exists, what each major file is
supposed to do, what outputs it produces, and how the main app exposes the
results.

## Why This Exists

The main Pokemon Momentum dashboard is a market research tool. The TCG
placeholder workflow is different: it is a collector utility for creating
printable binder placeholders and hostable checklist pages.

This workflow exists so the project can:

- generate printable placeholder cards for missing binder slots
- publish static checklist pages without requiring a database-backed feature
- keep collector reference tools available inside the same website shell
- preserve a workflow that can still run even before the full app environment is
  available

In short, this is here because it solves a real collector problem now and can be
published immediately as static assets.

## High-Level Architecture

The placeholder subsystem has four layers:

1. Hand-edited source data
2. Builder scripts
3. Generated static outputs
4. Website exposure through FastAPI

### 1. Hand-edited source data

Primary editable files:

- `TCG Placeholders/placeholders.csv`
  - Scarlet & Violet source of truth
- `TCG Placeholders/mega_placeholders.csv`
  - Mega-era source used for carryover and Mega checklist builds
- `TCG Placeholders/prize_pack_series7.csv`
  - Prize Pack Series 7 source rows
- `TCG Placeholders/prize_pack_series8.csv`
  - Prize Pack Series 8 source rows

Historical reference:

- `TCG Placeholders/Placeholders.xlsx`
  - legacy workbook input, useful as reference or fallback import source

Correction files:

- `TCG Placeholders/data/checklist_overrides.csv`
  - moves, recategorizes, or excludes already-generated checklist rows
- `TCG Placeholders/data/checklist_additions.csv`
  - adds rows not present in the source data yet
- `TCG Placeholders/data/checklist_mega_overrides.csv`
  - Mega checklist overrides
- `TCG Placeholders/data/checklist_mega_additions.csv`
  - Mega checklist additions

Reference catalog:

- `TCG Placeholders/data/set_release_dates.csv`
  - maps set codes to set names, release dates, and notes

## 2. Builder Scripts

### `TCG Placeholders/tools/build_placeholders.py`

Purpose:

- read CSV or XLSX source rows
- normalize dates and row structure
- validate basic data quality
- collapse duplicates for printable output
- emit static print-ready assets

Expected outputs:

- normalized CSV for review
- JSON export for future tooling
- printable HTML sheets
- grouped print dashboards by release block and card set code
- validation report

Why it is here:

- the placeholder builder is the foundation for printable binder cards
- it intentionally uses only the Python standard library so it can run in a
  bare environment

### `TCG Placeholders/tools/build_checklists.py`

Purpose:

- turn placeholder rows into hostable checklist pages
- group cards into the set page where collectors expect to track them
- apply overrides and manual additions without editing generated files
- emit set-level CSV downloads and review queues

Expected outputs:

- per-set HTML checklist pages
- per-set CSV downloads
- one combined era CSV
- a review queue for auditing and corrections
- a browser-based static review page

Why it is here:

- it converts placeholder data into a more collector-friendly “track what I own”
  format
- it keeps the published site static, portable, and easy to host

## 3. Generated Output Folders

### Print outputs

- `TCG Placeholders/output/`
  - Scarlet & Violet print hub
- `TCG Placeholders/output_mega/`
  - Mega print hub
- `TCG Placeholders/output_prize_pack_series7/`
  - Prize Pack Series 7 print hub
- `TCG Placeholders/output_prize_pack_series8/`
  - Prize Pack Series 8 print hub
- `TCG Placeholders/output_combined/`
  - combined print hub across multiple sources

Typical contents:

- `index.html`
- `placeholders.normalized.csv`
- `placeholders.printable_unique.csv`
- `placeholders.json`
- `placeholders_print.html`
- `validation_report.md`
- grouped HTML pages under `by_release_block/` and `by_card_code/`

### Checklist outputs

- `TCG Placeholders/checklists_sv/`
  - Scarlet & Violet checklist site
- `TCG Placeholders/checklists_mega/`
  - Mega checklist site

Typical contents:

- `index.html`
- `by_set/*.html`
- `csv/*.csv`
- one era-wide combined CSV
- `review.html`
- `review_queue.csv`

## 4. How The Website Exposes It

The main app does not rebuild placeholder data on request. Instead, it serves
the already-generated files.

Relevant app files:

- `scripts/dashboards/api.py`
- `scripts/dashboards/collector_hub.html`
- `scripts/dashboards/placeholder_library.html`

### Backend exposure

`api.py` exposes this workflow in two ways:

- collector-style asset browsing:
  - `/collector-hub`
  - `/collector-manifest`
  - `/collector-assets/{bucket}/{asset_path}`
- dedicated placeholder download page:
  - `/placeholders`
  - `/placeholder-downloads-manifest`
  - `/placeholder-downloads/{asset_id}`

Expected behavior:

- only whitelisted folders/files are published
- routes should return static HTML, CSV, JSON, or Markdown files
- file-serving helpers should block path traversal and unknown asset IDs

Why it is wired this way:

- it keeps publishing simple
- it avoids database work for static collector tools
- it makes the imported side project usable immediately in the main site

## Expected Operator Flow

When source data changes:

1. Edit the relevant CSV source.
2. Run the placeholder builder for the desired output set.
3. Review the validation report.
4. Run the checklist builder if checklist pages are affected.
5. Review the review queue and compare reports.
6. Publish or commit the updated generated outputs.

## Expected Developer Questions

### “Where should a human edit data?”

Edit the source CSVs and the override/addition CSVs. Do not hand-edit generated
HTML, JSON, or generated CSV outputs.

### “What is safe to regenerate?”

All `output*` and `checklists_*` folders are build artifacts from the current
workflow. They can be regenerated from source plus override/addition files.

### “Why are there both print hubs and checklist hubs?”

They solve adjacent but different use cases:

- print hubs answer “what placeholder cards do I need to print?”
- checklist hubs answer “which cards do I already own or still need?”

### “Why is this in the same repo as the market app?”

Because it shares the same audience, can be published under the same site, and
already has enough value to justify being available before a larger collector
system is built.

## Current Risks / Maintenance Notes

- imported folder hygiene still needs cleanup:
  - nested `.git`
  - nested `.agents`
  - `tools/__pycache__`
- generated outputs are large and should stay intentionally curated if committed
- checklist quality still depends on review queue follow-up and manual overrides
- release-block and set-code grouping rules should be treated as product
  decisions, not just implementation details

## Practical Rule Of Thumb

If someone needs to “decode” this subsystem quickly:

- source CSVs explain the input
- build scripts explain the transformation rules
- generated folders show the expected output shape
- `api.py` explains how the public website publishes it

That is the intended mental model for maintaining this part of the project.
