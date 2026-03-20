# pokemon-momentum

Pokemon market research dashboards, set-analysis tools, and a daily TCGCSV-backed data pipeline.

This project is no longer a `top200` prototype. The current product is a small dashboard suite for:

- finding movers and early trend candidates
- comparing cards and sealed products
- reading set-level breadth and momentum
- exploring set value concentration and basket cost
- saving lightweight tracking tags such as `Favorite`, `Watchlist`, `Research`, and `Buy List`


## Main Pages

- `/dashboard`
  - main research dashboard
- `/dashboard-dev`
  - same dashboard with extra inline operator help
- `/embed`
  - lighter embedded/public-facing dashboard variant
- `/set-explorer`
  - lighter set-cost and concentration browser
- `/account-settings`
  - lightweight tracking account settings


## Current Features

- English and Japanese category support
  - `3` = Pokemon
  - `85` = Pokemon Japanese
- product-level screens
  - `Top Movers`
  - `Breakouts`
  - `Good Buys`
  - `SMA30 Holds`
  - `Early Uptrends`
  - `Uptrends`
  - `Signals`
- set-level screens
  - `Group Signals`
  - set-level charting via `/group_series`
  - `Set Explorer` via `/set_baskets`
- lightweight tracking accounts
  - username + PIN
  - synced tags across sessions/devices
- uploaded set-logo support
  - synced into `/images/logos/<groupId>.<ext>`
  - used by `Group Signals` and `Set Explorer`


## Storage Model

The project uses:

- DuckDB
  - working analytics database
  - metadata tables
  - snapshot tables
  - API query engine
- Parquet
  - partitioned historical price store
  - preferred history source when available for that category

The API automatically chooses parquet when the requested category exists there and falls back to DuckDB otherwise.


## Run With Docker

Build the image:

```bash
docker compose build
```

Start the app:

```bash
docker compose up -d
```

Open a shell in the running container:

```bash
docker compose exec pokemon-momentum bash
```

The container serves FastAPI from:

- `scripts/dashboards/api.py`

Default dashboard port:

- `http://localhost:8001`


## Daily Pipeline

The current daily flow is:

1. Download and extract new TCGCSV archives
2. Load historical price rows into DuckDB
3. Refresh group metadata
4. Refresh product metadata
5. Rebuild joined/named exports
6. Build product signal snapshot
7. Build group signal snapshot
8. Export partitioned parquet history

Run it manually:

```bash
python scripts/pipeline/run_daily_update.py
```

Useful examples:

```bash
python scripts/pipeline/run_daily_update.py --category-id 3
python scripts/pipeline/run_daily_update.py --category-id 85
python scripts/pipeline/run_daily_update.py --skip-download
python scripts/pipeline/run_daily_update.py --dry-run
```

Validate the current pipeline state:

```bash
python scripts/pipeline/validate_pipeline.py
python scripts/pipeline/validate_pipeline.py --category-id 85
```


## Systemd / Host Automation

The repo includes host-side systemd units for:

- daily updates
- weekly full metadata refreshes

Relevant files:

- `deploy/systemd/pokemon-momentum-daily-update.service`
- `deploy/systemd/pokemon-momentum-daily-update.timer`
- `deploy/systemd/pokemon-momentum-weekly-refresh.service`
- `deploy/systemd/pokemon-momentum-weekly-refresh.timer`

See the runbook for installation and verification details.


## Set Logo Workflow

Uploaded source logos live in:

- `/opt/pokemon-momentum/images/set logos`

Sync them into runtime filenames:

```bash
python3 scripts/utilities/sync_set_logos.py
```

That script copies supported files into:

- `/opt/pokemon-momentum/images/logos/<groupId>.<ext>`

The dashboards then use those synced files automatically.


## Documentation

Use these files as the main docs:

- [dashboard_how_to.txt](/opt/pokemon-momentum/docs/dashboard_how_to.txt)
  - how to use the dashboard
- [data_flow.txt](/opt/pokemon-momentum/docs/data_flow.txt)
  - how the system fits together
- [operator_runbook.txt](/opt/pokemon-momentum/docs/operator_runbook.txt)
  - day-to-day operation and recovery
- [todo.txt](/opt/pokemon-momentum/docs/todo.txt)
  - active roadmap


## Notes

- Generated CSVs, snapshots, parquet partitions, and dashboard outputs are rebuildable artifacts.
- The old `top200` stack is legacy scaffolding and is no longer the main product path.
