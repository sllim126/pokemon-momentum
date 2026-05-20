# Index Overview Maintenance Guide

This document explains how the desktop Index Overview pages are wired and how to safely extend them.

## Architecture

The index experience has two layers:

1. Static page routes:
- `/index-overview` (hub page)
- `/index-overview-<key>` (one page per index family, for example `/index-overview-sv100`)

2. Shared data API:
- `/index-overview-data?index=<key>`

Every page pulls data from `/index-overview-data`, then renders:
- stat cards
- Plotly chart (index level / aggregate value)
- included set grid
- top-N holdings based on each index definition's `constituent_limit`

## Source Files

- Backend/API: `scripts/dashboards/api.py`
- Hub page: `scripts/dashboards/index_overview_hub.html`
- Index pages:
  - `scripts/dashboards/index_overview.html` (SV100)
  - `scripts/dashboards/index_overview_mega100.html`
  - `scripts/dashboards/index_overview_pokemon100.html`
  - `scripts/dashboards/index_overview_swsh100.html`
  - `scripts/dashboards/index_overview_sm100.html`
  - `scripts/dashboards/index_overview_xy100.html`
  - `scripts/dashboards/index_overview_bw100.html`
  - `scripts/dashboards/index_overview_dp100.html`
  - `scripts/dashboards/index_overview_ex100.html`
  - `scripts/dashboards/index_overview_wotc100.html`
  - `scripts/dashboards/index_overview_neo100.html`
  - `scripts/dashboards/index_overview_ecard100.html`
- Contract tests: `tests/test_secondary_pages.py`

## Current Index Keys

Configured in `INDEX_DEFINITIONS` inside `api.py`.

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

## How Membership Is Computed

For each index key:

1. Resolve included sets using one of:
- explicit `group_ids`
- `generation` via `build_generation_case(...)`
- `all_active_groups` (Pokemon Top 151)

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

2. Add `INDEX_DEFINITIONS["<newkey>"]`:
- choose `group_ids` for strict set ranges, or `generation` for broad eras
- set `release_markers_enabled` appropriately

3. Create page file:
- copy an existing live page (for example `index_overview_xy100.html`)
- update page title labels
- update fetch endpoint to `/index-overview-data?index=<newkey>`

4. Update hub card:
- add/adjust link and status in `index_overview_hub.html`

5. Update tests:
- include page file in `tests/test_secondary_pages.py`
- add route/endpoint assertions

6. Validate:
- `python3 -m py_compile scripts/dashboards/api.py`
- `python3 -m unittest tests/test_secondary_pages.py`

## Known UX Behavior

- Chart defaults to `All` range and `Index Level`.
- Holdings are capped to each index definition's constituent limit; the all-English Pokemon index uses 151 while era-specific indexes generally use 100.
- Card title dedupe handles number formatting differences like `48/108` vs `048/108`.
