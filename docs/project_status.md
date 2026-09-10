# Pokemon Momentum Project Status

Last reviewed: 2026-09-10

This is the concise map of what exists, what is operational, and what was in progress
when development paused. Detailed implementation and operating instructions remain in
the other files under `docs/`.

## Current state

- Production entry point: `https://market.poke6s.com/`
- Primary branch: `main`
- Market data: daily English and Japanese TCGCSV-backed updates
- Storage: DuckDB working database, partitioned Parquet history, and generated snapshots
- Frontend: static HTML/CSS/JavaScript pages served by FastAPI
- Accounts: Google-backed lightweight tracking accounts with saved tags and views
- Commerce: Squarespace storefront linking, price synchronization, listing intake, and supplier-profitability workflows
- Collector resources: generated placeholder libraries, checklists, print pages, review queues, and downloads

At the 2026-09-10 review, the health snapshot reported 38,831,595 English price rows with
latest data dated 2026-09-09. The daily pipeline and Squarespace price audit were running.

## Public page map

| Route | Purpose | Status |
| --- | --- | --- |
| `/`, `/dashboard` | Responsive market research entry point | Live |
| `/mobile` | Direct mobile decision dashboard | Live |
| `/dashboard-lab` | Desktop command-center reference | Working reference |
| `/mobile-rebuild` | Earlier mobile concept retained for comparison | Legacy reference |
| `/set-explorer` | Set cost, concentration, and depth | Live; mobile refinement pending |
| `/budget-builder` | Budget-based recommendation builder | Active; duplicate-name regression under review |
| `/sealed-deals` | Sealed price-per-pack/value screening | Live; mobile refinement pending |
| `/account-settings` | Synced tracking account management | Live |
| `/collector-hub` | Collector workflow navigation | Live |
| `/placeholders` | Placeholder/checklist downloads | Live |
| `/index-overview` | Market-index hub | Live |
| `/index-overview-*` | English/Japanese and era index detail pages | Live |

Current index detail routes are:

- `/index-overview-pokemon100`
- `/index-overview-sv100`
- `/index-overview-mega100`
- `/index-overview-wotc100`
- `/index-overview-neo100`
- `/index-overview-ecard100`
- `/index-overview-ex100`
- `/index-overview-dp100`
- `/index-overview-bw100`
- `/index-overview-xy100`
- `/index-overview-sm100`
- `/index-overview-swsh100`
- `/index-overview-jp-pokemon100`
- `/index-overview-jp-sv100`

## Protected operator pages

The following pages require an authenticated admin tracking account:

| Route | Purpose |
| --- | --- |
| `/eod-dashboard` | End-of-day market data view |
| `/embed` | Lightweight internal/embed dashboard |
| `/bug-reports` | Internal feedback and bug queue |
| `/pricing-upload` | Upload and compare Squarespace product exports |
| `/single-listings-upload` | Convert/upload single-card listing intake |
| `/supplier-pricing` | Review and save supplier quotes |
| `/supplier-profitability` | Compare supplier, market, and store economics |

## Major completed work

- Daily English and Japanese ingestion and snapshot pipeline
- Desktop screening dashboard and mobile decision dashboard
- Server-side search, paged pickers, product/set history, and saved URL state
- Screeners for movers, pullbacks, breakouts, trend confirmation, early movement, set strength, and budget/value research
- Google sign-in, synced tracking tags, saved views, and structured bug reports
- English/Japanese market index family
- Squarespace price sync and inventory audit
- Singles listing workflow with cached, square-padded Scrydex artwork
- Collector Hub and generated placeholder/checklist workflow

## Work in progress at the last checkpoint

- Cohesive navigation and visual shell across the dashboard and secondary pages
- Tablet and narrow-desktop layouts
- Mobile versions of Set Explorer, Sealed Deals, Browse Set, Browse Species, Set Strength, and Time to Buy
- Search and tracked-list behavior across English/Japanese and Singles/Sealed
- Sealed Squarespace listing intake and creation scripts
- PSA cert data is available server-side but has not been given a settled public UI
- Scrydex/PriceCharting provider evaluation beyond the image workflow
- Placeholder/checklist source gaps and generated-artifact repository policy

The detailed active queue and design decisions live in `docs/todo.txt`.

## Known engineering follow-up

- Restore a fully green test run by resolving the Budget Builder duplicate-name behavior.
- Split the large `scripts/dashboards/api.py` module by route/service domain.
- Replace remaining full-catalog browser loads with paged server-side search.
- Profile the slow `under_the_radar` and `breakouts` queries.
- Consolidate duplicated index HTML behind shared templates/configuration.
- Extract shared frontend shell, API, chart, formatting, and tracking code.
- Pin runtime dependencies and separate development-only packages.
- Decide which generated collector artifacts belong in Git.

## Before resuming feature work

1. Review and checkpoint all intentional modified/untracked source and data files.
2. Separate generated artifacts from hand-authored changes in the commit plan.
3. Fix or explicitly document the Budget Builder regression.
4. Run `make test` and the pipeline validator.
5. Tag or otherwise record the resulting known-good checkpoint.

Do not treat `output/`, database files, raw downloads, or other ignored runtime data as a
substitute for source control. Conversely, do not delete local runtime/generated files until
their rebuild and backup path has been confirmed.

## Repository artifact policy

The repository currently uses three artifact classes:

1. Hand-authored and reviewed source: application code, tests, documentation, configuration,
   source CSVs, overrides, and additions. These belong in normal source-review commits.
2. Published generated collector assets: `TCG Placeholders/output*/` and
   `TCG Placeholders/checklists_*/`. The live Collector Hub serves these files directly, so
   they remain versioned until deployment builds them independently. They are marked as
   generated in `.gitattributes` and should be committed separately from builder changes.
3. Local runtime artifacts: `output/`, `data/raw/`, `data/extracted/`, `data/processed/`,
   `logs/`, and `.locks/`. These remain ignored and must not be committed.

Cached files under `images/cards/` are deployable binary assets used by Squarespace and
storefront workflows. Keep their commits separate from application logic when practical.
