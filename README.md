# Pokemon Momentum

Pokemon Momentum is a market research and screening project for Pokemon TCG data.

It combines:
- a daily TCGCSV-backed pipeline
- DuckDB and Parquet storage
- a main research dashboard
- a lighter set explorer
- lightweight tracking tags for sourcing and review

The goal is simple: make it easier to spot what is moving, what may be starting to move, and which sets or products deserve attention.


## Main Pages

- `/dashboard`
  - the main research dashboard
- `/dashboard-dev`
  - the same dashboard with extra operator help text
- `/embed`
  - a lighter embedded/public-style view
- `/set-explorer`
  - a simpler set-browsing page for cost and concentration questions
- `/account-settings`
  - lightweight tracking account management


## What The Project Does

- tracks historical Pokemon price data
- serves screener views such as:
  - `Top Movers`
  - `Breakouts`
  - `Good Buys`
  - `SMA30 Holds`
  - `Early Uptrends`
  - `Uptrends`
  - `Signals`
  - `Group Signals`
- supports set-level history through `/group_series`
- supports set-cost and concentration browsing through `/set_baskets`
- supports lightweight synced tags such as:
  - `Favorite`
  - `Watchlist`
  - `Research`
  - `Buy List`


## Storage

The project currently uses:

- DuckDB
  - working analytics database
  - metadata and snapshot tables
- Parquet
  - partitioned historical fact store
- CSV
  - metadata/intermediate fallback layer in some parts of the pipeline

In practice:
- historical price reads prefer Parquet when available
- the app falls back to DuckDB when needed


## Run With Docker

Build:

```bash
docker compose build
```

Start:

```bash
docker compose up -d
```

App URL:

```text
http://localhost:8001
```


## Daily Pipeline

Run the pipeline manually:

```bash
python scripts/pipeline/run_daily_update.py
```

Validate outputs:

```bash
python scripts/pipeline/validate_pipeline.py
```

The current daily flow is:
1. download and extract new TCGCSV archives
2. load price history into DuckDB
3. refresh group metadata
4. refresh product metadata
5. rebuild joined/named exports
6. build product signal snapshot
7. build group signal snapshot
8. export parquet history


## Testing

The repo now includes a lightweight test harness for:

- backend API route coverage
- frontend dashboard contract checks
- shared dashboard/query helper logic

Run the suite inside the app container:

```bash
docker-compose exec -T pokemon-momentum python tests/run_with_coverage.py
```

Current expectations:

- all tests pass
- target-module coverage stays at or above `90%`

The coverage gate currently measures the shared dashboard logic modules:

- `scripts/common/category_config.py`
- `scripts/common/product_classification.py`
- `scripts/dashboards/query_support.py`

This keeps the threshold honest without pretending the entire large dashboard API file
is already unit-test-shaped.


## Set Logos

Uploaded source logos can live in:

- `/opt/pokemon-momentum/images/set logos`

Sync them into the runtime logo folder with:

```bash
python3 scripts/utilities/sync_set_logos.py
```

The dashboards then read:

- `/opt/pokemon-momentum/images/logos/<groupId>.<ext>`


## Documentation

Detailed docs live here:

- [docs/dashboard_how_to.txt](/opt/pokemon-momentum/docs/dashboard_how_to.txt)
- [docs/data_flow.txt](/opt/pokemon-momentum/docs/data_flow.txt)
- [docs/operator_runbook.txt](/opt/pokemon-momentum/docs/operator_runbook.txt)
- [docs/todo.txt](/opt/pokemon-momentum/docs/todo.txt)


## Notes

- This project has moved beyond the old `top200` prototype workflow.
- Generated outputs are rebuildable artifacts.
