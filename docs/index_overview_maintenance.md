# Index Overview Maintenance Guide

This document explains how the desktop Index Overview pages are wired and how to safely extend them.

## Architecture

The index experience has two layers:

1. Static page routes:
- `/index-overview` (hub page)
- `/index-overview-<key>` (all detail routes serve one shared template)

2. Shared data API:
- `/index-overview-data?index=<key>`

Every page pulls data from `/index-overview-data`, then renders:
- stat cards
- Plotly chart (index level / aggregate value)
- included set grid
- top-N holdings based on each index definition's `constituent_limit`

## Source Files

- Backend/API: `scripts/dashboards/api.py`
- Definitions and membership exclusions: `scripts/dashboards/index_config.py`
- Era-classification rules: `scripts/dashboards/query_support.py`
- Hub page: `scripts/dashboards/index_overview_hub.html`
- Shared detail page: `scripts/dashboards/index_overview_detail.html`
- Snapshot validator: `scripts/validate_index_overviews.py`
- Contract tests: `tests/test_secondary_pages.py`

## Current Index Keys

Configured in `INDEX_DEFINITIONS` inside `index_config.py`.

- `pokemon100`: all active English groups
- `sv100`: explicit Scarlet & Violet group list
- `mega100`: generation bucket `MEG`
- `swsh100`: generation bucket `SWSH`
- `sm100`: generation bucket `SM`
- `xy100`: generation bucket `XY`
- `bw100`: generation bucket `BW`
- `dp100`: generation bucket `DP/HGSS`
- `ex100`: generation bucket `EX`
- `wotc100`: explicit WOTC groups (Base Set through Gym Challenge)
- `neo100`: explicit Neo groups (Genesis through Destiny)
- `ecard100`: explicit e-Card groups (Expedition through Skyridge)
- `jp_pokemon100`: all active Japanese groups, top 151
- `jp_sv100`: Japanese generation bucket `SV`

## How Membership Is Computed

For each index key:

1. Resolve included sets using one of:
- explicit `group_ids`
- `generation` via `build_generation_case(...)`
- `all_active_groups` (Pokemon Top 151)

Then remove every configured `exclude_group_ids` entry. These exclusions are the audited
boundary for preconstructed, reprint, catch-all, or otherwise misassigned groups. Prefer an
exact group override or exclusion over making the date heuristic broader.

2. For each date:
- rank cards by `marketPrice` descending
- keep the top rows for the index definition's `constituent_limit`
- compute aggregate market value for that day

3. Build index series:
- normalize first day to base level (typically 1000)
- track day-to-day constituent turnover
- if turnover exceeds 10%, trigger reconstitution and adjust divisor

4. Build latest-day holdings:
- top cards for that index limit (rank, set, image, number, rarity, subtype, price)

## Release Marker Toggle Rules

`release_markers_enabled` is returned by backend and used by frontend to show or hide the "Set Releases" toggle.

Current policy:
- Enabled: `sv100`, `mega100`
- Disabled: all older eras and `pokemon100`

## Adding a New Index

1. Add route in `api.py`:
- `/index-overview-<newkey>`

2. Add `INDEX_DEFINITIONS["<newkey>"]` in `index_config.py`:
- choose `group_ids` for strict set ranges, or `generation` for broad eras
- set `release_markers_enabled` appropriately

3. Add the route path, index key, and category ID to `indexRouteConfig` in
`index_overview_detail.html`. Titles, counts, methodology text, and data come from the API.

4. Update hub card:
- add/adjust link and status in `index_overview_hub.html`

5. Update the shared-template contract in `tests/test_secondary_pages.py`.

6. Validate:
- `python3 -m py_compile scripts/dashboards/api.py`
- rebuild English and/or Japanese index snapshots
- `python scripts/validate_index_overviews.py`
- `python tests/run_with_coverage.py`

## Known UX Behavior

- Chart defaults to `All` range and `Index Level`.
- Holdings are capped to each index definition's constituent limit; the all-English Pokemon index uses 151 while era-specific indexes generally use 100.
- Card title dedupe handles number formatting differences like `48/108` vs `048/108`.
