# Pokemon Momentum

Pokemon Momentum is a market research and screening project for Pokemon TCG data.

It combines:
- a daily TCGCSV-backed pipeline
- DuckDB and Parquet storage
- separate desktop and mobile research dashboards
- set, sealed-product, and budget research tools
- a family of English and Japanese market indexes
- collector placeholder/checklist resources
- Google-based synced tracking auth
- Squarespace pricing and listing operations

The goal is simple: make it easier to spot what is moving, what may be starting to move, and which sets or products deserve attention.


## Public Pages

- `/`
  - user-agent-aware entry point: desktop terminal on larger screens and the mobile decision dashboard on phones/tablets
- `/dashboard`
  - alias of `/`, with the same responsive routing
- `/mobile`
  - direct access to the mobile decision dashboard
- `/dashboard-lab`
  - desktop command-center design/workflow reference
- `/collector-hub`
  - entry point for collector checklists, print tools, review queues, and downloads
- `/placeholders`
  - curated placeholder/checklist download library
- `/dashboard-dev`
  - desktop dashboard alias retained for development compatibility
- `/set-explorer`
  - set-cost, concentration, depth, generation, and rarity/variant browsing
- `/budget-builder`
  - budget-based card recommendation and diversification tool
- `/sealed-deals`
  - a sealed-product screening page focused on pack-value and product-level deal math
- `/account-settings`
  - lightweight synced tracking account management
- `/index-overview`
  - hub for the English/Japanese and era-specific market indexes
- `/index-overview-{index}`
  - individual index pages; see `docs/project_status.md` for the current route list


## Protected Operator Pages

These routes require an authenticated admin tracking account:

- `/eod-dashboard`
- `/embed`
- `/bug-reports`
- `/pricing-upload`
- `/single-listings-upload`
- `/supplier-pricing`
- `/supplier-profitability`


## What The Project Does

- tracks historical Pokemon price data
- serves screener views such as:
  - `Top Movers`
  - `Breakouts`
  - `Good Buys`
  - `Consistent Movers`
  - `Early Movers`
  - `Confirmed Movers`
  - `Set Strength`
- supports set-level history through `/group_series`
- supports set-cost and concentration browsing through `/set_baskets`
- supports sealed deal screening through `/sealed_deals`
- supports budget-based recommendations through `/budget_builder`
- publishes prebuilt market-index snapshots through `/index-overview-data`
- publishes collector/checklist resources through controlled manifest and asset routes
- supports lightweight synced tags such as:
  - `Favorite`
  - `Watchlist`
  - `Research`
  - `Buy List`
- supports Google sign-in for synced tracking accounts
- routes dashboard TCGplayer outbound links through the configured affiliate link
- resolves in-stock Squarespace listings for dashboard storefront links
- supports operator workflows for pricing, supplier economics, and new singles listings


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

Google sign-in:

- set `POKEMON_MOMENTUM_GOOGLE_CLIENT_ID` in `.env`
- the dashboard account menu will render the Google sign-in button when configured

PSA cert lookup:

- set `POKEMON_MOMENTUM_PSA_ACCESS_TOKEN` in `.env` for server-side PSA cert verification
- optional: set `POKEMON_MOMENTUM_PSA_CACHE_TTL_DAYS` to override the default 30-day cache window


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
8. build screener, sparkline, series, health, and index-overview snapshots
9. export parquet history

The host automation runs the flow for both English (`category_id=3`) and Japanese
(`category_id=85`) market data. See `docs/data_flow.txt` for the detailed source and
fallback rules.


## Testing

The repo now includes a lightweight test harness for:

- backend API route coverage
- frontend dashboard contract checks
- shared dashboard/query helper logic

Python app and test dependencies such as `fastapi` and `pytest` are installed in the
Docker image from `requirements.txt`. You do not need to install them on the Linux
host unless you intentionally want a separate non-Docker Python workflow.

Start the app container if it is not already running:

```bash
docker-compose up -d
```

Run the suite inside the app container:

```bash
make test
```

Run every discovered test with plain `pytest`:

```bash
make test-all
```

Run just the API route module:

```bash
make test-api
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

- [docs/project_status.md](/opt/pokemon-momentum/docs/project_status.md)
- [docs/dashboard_how_to.txt](/opt/pokemon-momentum/docs/dashboard_how_to.txt)
- [docs/data_flow.txt](/opt/pokemon-momentum/docs/data_flow.txt)
- [docs/operator_runbook.txt](/opt/pokemon-momentum/docs/operator_runbook.txt)
- [docs/todo.txt](/opt/pokemon-momentum/docs/todo.txt)


## Notes

- This project has moved beyond the old `top200` prototype workflow.
- The application currently spans market research, collector tools, and protected
  Squarespace/store operations; `docs/project_status.md` is the concise project map.
- Many generated outputs are rebuildable artifacts, but the repository policy for
  committing placeholder/checklist outputs still needs to be finalized before cleanup.
