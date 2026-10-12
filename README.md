# Pokemon Momentum

Pokemon Momentum is a market research and screening project for Pokemon TCG data.

It combines:
- a daily TCGCSV-backed pipeline, with Discord alerts
- DuckDB and Parquet storage
- a desktop app (Buy, Discover, Research, Track, Learn in one page) and a mobile page
- set, sealed-product, and budget research tools
- a 6-month "hold check" and PriceCharting graded prices on every card
- a family of English and Japanese market indexes
- collector placeholder/checklist resources
- Google-based synced tracking auth
- Squarespace pricing and listing operations

The goal is simple: make it easier to spot what is moving, what may be starting to move, and which sets or products deserve attention.


## Public Pages

- `/home-preview`
  - the new desktop app: every Buy, Discover, Research, Track and Learn view in one
    three-column page; planned to replace `/` for desktop visitors (see
    `docs/desktop_app.md`)
- `/`
  - user-agent-aware entry point: the research dashboard on larger screens and the mobile decision dashboard on phones/tablets
- `/dashboard`
  - alias of `/`, with the same responsive routing
- `/mobile`
  - direct access to the mobile decision dashboard
- `/buy`, `/discover`, `/research`, `/track`, `/learn`
  - area landing pages built from the shared header's menu (`site_shell.js`)
- `/dashboard-lab`
  - desktop command-center design/workflow reference (development page)
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

`/set-explorer`, `/budget-builder`, `/sealed-deals` and the index pages are covered by views
in the desktop app and are planned to forward there once it becomes the main page.


## Protected Operator Pages

These routes require a Google sign-in whose email is listed in
`POKEMON_MOMENTUM_ADMIN_USERS` (exact match):

- `/admin` (index of the tools below)
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
- judges whether a card is likely to sell at a profit in six months after fees and
  shipping through `/hold_check` (`scripts/dashboards/hold_check.py`)
- shows PriceCharting graded prices (PSA 9/10, CGC 10) with a nightly history through
  `/graded_prices`; every price links back to PriceCharting, as its license requires
- compares sealed products with buying their booster packs loose (desktop app)
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

Settings live in `.env` (never commit it). How each one reaches the code matters:

| Setting | Used for | Read by |
| --- | --- | --- |
| `POKEMON_MOMENTUM_GOOGLE_CLIENT_ID` | Google sign-in button | Website, via the `environment:` list in `docker-compose.yml` |
| `POKEMON_MOMENTUM_ADMIN_USERS` | Comma-separated admin emails (exact match; default `sllim126@gmail.com`) | Website environment only. It is **not** in `docker-compose.yml`, so a value in `.env` is ignored until it is added there |
| `PRICECHARTING_API_TOKEN` | Nightly PriceCharting graded-price CSV | `fetch_pricecharting.py`, which reads `.env` itself |
| `POKE6S_DISCORD_WEBHOOK_URL`, `POKE6S_DISCORD_MENTION` | Pipeline and price-sync alerts (mention defaults to `@everyone`; `none` disables) | `notify_discord.py` on the host, which reads `.env` itself |
| `POKE6S_TCGCSV_USER_AGENT` | Optional override of the `Poke6sMarket/1.0.0` User-Agent | Pipeline runs (`docker-compose run`), via the `environment:` list in `docker-compose.yml`; blank means the default |
| `SQUARESPACE_*` | Store price sync and listing scripts | The Squarespace scripts, which read `.env` themselves |

The website only sees the variables listed under `environment:` in `docker-compose.yml`;
add a setting there (and restart) before relying on it inside the app.

PSA cert lookup:

- set `POKEMON_MOMENTUM_PSA_ACCESS_TOKEN` in `.env` for server-side PSA cert verification
- optional: set `POKEMON_MOMENTUM_PSA_CACHE_TTL_DAYS` to override the default 30-day cache window


## Daily Pipeline

The nightly run is `scripts/pipeline/run_daily_update_host.sh`, started at 02:00 UTC by
the `pokemon-momentum-daily-update` systemd timer. It stops the app (DuckDB has one
writer), runs the pipeline for English then Japanese, restarts the app, and posts a summary
to Discord.

Run the pipeline for one market manually (inside the container):

```bash
python scripts/pipeline/run_daily_update.py --category-id 3
```

Validate outputs:

```bash
python scripts/pipeline/validate_pipeline.py
```

The current daily flow is:
1. download today's prices from TCGCSV, one request per set (TCGCSV retired its daily
   archive in September 2026; there are no prices for 2026-09-16 to 2026-10-07)
2. load price history into DuckDB
3. refresh group metadata
4. refresh product metadata (only sets whose product file changed)
5. rebuild joined/named exports
6. build product signal snapshot
7. build group signal snapshot
8. build screener, sparkline, series, and health snapshots
9. download PriceCharting graded prices (English pass)
10. export parquet history
11. build index-overview snapshots (they read parquet, so they run last)

All TCGCSV requests go through `scripts/utilities/tcgcsv_client.py`, which follows TCGCSV's
usage guidelines (one request at a time, spaced out, stop on 403/429).

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

or directly:

```bash
docker-compose exec -T pokemon-momentum python -m pytest -q -p no:cacheprovider tests
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

Start with [AGENTS.md](/opt/pokemon-momentum/AGENTS.md) (rules for anyone changing the
repo, and the "milestone" wrap-up routine) and [docs/todo.txt](/opt/pokemon-momentum/docs/todo.txt)
(the work list). Detailed docs live here:

- [docs/project_status.md](/opt/pokemon-momentum/docs/project_status.md)
- [docs/desktop_app.md](/opt/pokemon-momentum/docs/desktop_app.md)
- [docs/dashboard_how_to.txt](/opt/pokemon-momentum/docs/dashboard_how_to.txt)
- [docs/data_flow.txt](/opt/pokemon-momentum/docs/data_flow.txt)
- [docs/index_overview_maintenance.md](/opt/pokemon-momentum/docs/index_overview_maintenance.md)
- [docs/operator_runbook.txt](/opt/pokemon-momentum/docs/operator_runbook.txt)
- [docs/responsive_qa.md](/opt/pokemon-momentum/docs/responsive_qa.md)
- [docs/todo.txt](/opt/pokemon-momentum/docs/todo.txt)


## Notes

- This project has moved beyond the old `top200` prototype workflow.
- The application currently spans market research, collector tools, and protected
  Squarespace/store operations; `docs/project_status.md` is the concise project map.
- Generated-artifact handling is defined in `docs/project_status.md`; runtime outputs stay
  ignored while published Collector Hub assets remain versioned until deployment builds them.
