# Pokemon Momentum Project Status

Last reviewed: 2026-10-23

This is the concise map of what exists, what is operational, and what was in progress
when development paused. Detailed implementation and operating instructions remain in
the other files under `docs/`.

## Current state

- Production entry point: `https://market.poke6s.com/`
- Primary branch: `main`
- Market data: daily English and Japanese TCGCSV-backed updates, fetched per set since
  TCGCSV retired its daily archive (no prices exist for 2026-09-16 to 2026-10-07)
- Graded data: nightly PriceCharting snapshot, kept as our own history
- Alerts: nightly update, Squarespace price sync and the other-games archive post
  pass/fail summaries to Discord
- Other games: Magic, Yu-Gi-Oh, One Piece and Lorcana prices are archived nightly
  (collect only, since 2026-10-12; no site yet) in `data/tcgcsv_archive/`
- Storage: DuckDB working database, partitioned Parquet history, and generated snapshots
- Frontend: static HTML/CSS/JavaScript pages served by FastAPI; a new single-page desktop
  app (`/` on desktop, `docs/desktop_app.md`) covers Buy, Discover, Research, Track and Learn
- Accounts: Google-backed lightweight tracking accounts with saved tags and views
- Commerce: Squarespace storefront linking, price synchronization, listing intake, and supplier-profitability workflows
- Collector resources: generated placeholder libraries, checklists, print pages, review queues, and downloads
- Standalone binder planners: ATLA and KPDH are served by separate read-only nginx
  containers at `/atla/` and `/kpdh/`; collection state stays in each visitor's browser

At the 2026-10-12 review, the health snapshot reported 39,285,648 English and 13,660,254
Japanese price rows, both with latest data dated 2026-10-11. The daily pipeline,
PriceCharting download and Squarespace price sync were running.

## Public page map

| Route | Purpose | Status |
| --- | --- | --- |
| `/` | Desktop: the desktop app (all five areas in one page). Phones: the mobile page | Live; desktop app since 2026-10-12 |
| `/dashboard` | Phones: the mobile page. Desktop: forwards to the desktop app | Live |
| `/dashboard-classic`, `/dashboard-dev` | The older research dashboard's desktop addresses | Retired 2026-10-16; forward like `/dashboard` |
| `/home-preview` | The desktop app's former preview address | Forwards to `/` on desktop |
| `/buy`, `/discover`, `/research`, `/track`, `/learn` | Area landing pages from the shared header menu | Live on phones; desktop forwards to the app |
| `/mobile` | Direct mobile decision dashboard | Live; frozen until the new mobile app replaces it |
| `/m-next` | New mobile app (`mobile_app.html`; plan in `docs/mobile_app_plan.md`) | Preview, step 1.1; opens on any device |
| `/dashboard-lab` | Desktop command-center reference | Development page; left public on purpose (decided 2026-10-12) |
| `/mobile-rebuild` | Earlier mobile concept retained for comparison | Development page; left public on purpose (decided 2026-10-12) |
| `/set-explorer` | Set cost, concentration, and depth | Live; covered by desktop app Market Stats → Set values |
| `/budget-builder` | Budget-based recommendation builder | Live; covered by desktop app Budget Builder |
| `/sealed-deals` | Sealed price-per-pack/value screening | Live; covered by desktop app Sealed Deals |
| `/account-settings` | Google sign-in, synced lists, saved views, account deletion | Live; restyled 2026-10 |
| `/collector-hub` | Collector resources with live preview | Live; restyled 2026-10 |
| `/placeholders` | Placeholder/checklist downloads | Live; restyled 2026-10 |
| `/binder-art` | Binder Art Maker: free tool that cuts one image into card-size pieces for a binder page (PDF or PNGs, optional set logo); runs in the browser | Live 2026-10-21 |
| `/index-overview` | Market-index hub | Live; covered by desktop app Market Stats → Indexes |
| `/index-overview-*` | English/Japanese and era index detail pages | Live; all routes use one configured detail template |
| `/atla/` | Static ATLA binder layout, collection tracking and browser-side valuation | Live; separate nginx container, unlisted/noindex |
| `/kpdh/` | Static Kayou KPop Demon Hunters master-binder planner | Live; separate nginx container, unlisted/noindex |

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

The following pages require a Google sign-in listed in `POKEMON_MOMENTUM_ADMIN_USERS`
(exact match; the old PIN sign-in is retired):

| Route | Purpose |
| --- | --- |
| `/admin` | Index of the admin tools |
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
- Google sign-in, synced tracking tags, saved views, and a mobile feedback form (bug reports go to Discord)
- English/Japanese market index family
- Squarespace price sync and inventory audit
- Singles listing workflow with cached, square-padded Scrydex artwork
- Collector Hub and generated placeholder/checklist workflow

Added 2026-10:

- Per-set TCGCSV download that follows TCGCSV's usage guidelines, replacing the retired archive
- Discord pass/fail alerts for the nightly update and the Squarespace price sync
- Squarespace listing retirement script (hide + stock 0, logged, reversible)
- PriceCharting graded prices with a nightly history (shown with a required link back)
- 6-Month Hold check (break-even after fees and shipping, trend, past track record)
- Shared site header and five area pages
- Desktop app with Buy (Sealed Deals with loose-pack comparison, card-art Budget Builder),
  Discover, Track (saved lists with price targets), Research (Sets, Pokémon, Market Stats)
  and Learn (How it works), plus a legal footer
- Collector Hub, Placeholder Library and Account restyled on a shared desktop stylesheet
- Index snapshots now build after the parquet export, so indexes are no longer a day behind
- The desktop app is the desktop home page at `/`; older desktop addresses forward to it
  (phones unchanged); the older research dashboard is retired on desktop (2026-10-16) and
  `dashboard.html` now serves only the mobile menu's Browse Set, Browse Pokémon and Set Strength
- Card views answer in about 0.01-0.05 s: cached table checks, one shared DuckDB
  connection, a weekly long-history snapshot for "All" and the 6-Month Hold, browser caching
- Discover lists scale their activity thresholds to feed gaps
- Nightly collect-only archive of Magic, Yu-Gi-Oh, One Piece and Lorcana prices
- Sign-in sessions expire and are stored hashed; public endpoints that cost money or
  rebuild data are admin-only or rate-limited
- Full card page in the desktop app (`&card=1`), release-date sorting for sets, 29 more
  set logos, rebuilt Collector Hub and Placeholder Library
- Sealed Deals pack counts corrected for cases, UPCs and Super-Premium Collections
- Nightly jobs wait for DNS after a reboot; runbook covers recovering from a power cut
- Pullbacks redefined (2026-10-17, trial week): cards 10%+ below their 90-day high for a week,
  well behind cards of a similar price and their own set, still up over the year; cards whose
  whole set is falling are listed as Whole-set Dips (`/set_dips`)
- Discord #bug-reports messages are copied into the admin bug list every 15 minutes by the
  "Poke6s Control" bot (marked ✅); the admin bug page escapes report text
- Small Dips list (2026-10-21): strong cards (+15% over 90 days) pausing 3-12% below their high
- Weekly Discord posts (Fridays): buy picks, market snapshot, set of the week, card of the week
- Japanese Sealed Deals box sizes confirmed with the owner (specialty sets 10 packs, 151 / Black
  Bolt / White Flare / 30th Celebration / Dark Phantasma / Pokemon GO 20, 25th Anniversary 16)
- Sealed Deals reads the owner's checked pack counts and pack mixes from
  `scripts/dashboards/sealed_pack_overrides.csv` and prices collections by the packs inside
- `data/sealed_pack_checklist.csv` lists every sealed product whose pack count is still a guess;
  `scripts/utilities/sealed_checklist_server.py` serves a one-item-at-a-time page (port 8099) to fill it in
- ATLA Binder planner at `/atla/`, isolated in a read-only nginx container on host port
  8090 and reached through the Caddy proxy on `192.168.1.30`
- ATLA follow-up: generated local-scan manifest avoids failed image probes, mixed-category
  ranges name both categories, missing foil quotes stay unpriced, Scryfall refreshes are
  daily and paced, collection JSON can be exported/imported, destructive page clears ask
  first, and Caddy compresses the public page with zstd/gzip
- KPDH Binder planner at `/kpdh/`, isolated in a read-only nginx container on host port
  8091 and reached through the same Caddy proxy
- Confirmed and Consistent Movers show how many days each card has been in the list (sortable);
  Set values' top-hit card opens in the side pane; browser Back closes the full card page
- Binder planners on phones (2026-10-21/22): cards near the top, sideways page strip with a
  swipe hint; ATLA also offers 6x6, 4x4, 4x3 and 3x3 pages
- New mobile app preview at `/m-next` (2026-10-21/22; plan `docs/mobile_app_plan.md`): "Check a
  card" home screen and Deal Checker (asking price, To keep / To resell, verdict, "pay up to",
  spike/falling/few-sales warnings); `/hold_check` gained `net_at_market` and `profit_now`
- Desktop app on iPad-portrait widths (<1100 px): the list menu folds into a "☰ Lists" button
- How it works credits TCGCSV with a support link and drops the empty card column
- Placeholder Library sync (2026-10-23): `scripts/utilities/sync_placeholder_library.py`, daily
  04:30 UTC, adds new Mega-era sets and promos from the catalog and rebuilds the library

## What's next

The work list is `docs/todo.txt`: priorities, open decisions, parked mobile and admin
work, and known engineering follow-up all live there, so this file doesn't keep a second
copy. `AGENTS.md` explains how to keep it current. The responsive audit and the outstanding
real-browser checklist are in `docs/responsive_qa.md`.

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

## Index audit status

The 2026-09-10 audit rebuilt and validated every configured snapshot against market data
dated 2026-09-09. Both global indexes contain exactly 151 holdings; every era index contains
exactly 100. The validator checks contiguous ranks, duplicate product/variant keys, included
set membership, excluded groups, metadata placeholders, summary counts, and freshness.

Generation indexes cover expansion cards and recognized special subsets. Known
preconstructed/reprint buckets—Trainer Kits, Battle Academy products, World Championship
Decks, Deck Exclusives, Jumbo Cards, and EX Battle Stadium—are explicitly excluded in the
central index configuration. Kalos Starter Set is explicitly assigned to XY rather than Black
& White. Japanese SV currently includes its named era starter decks and deck-build boxes;
generic Battle Academy, energy, and box-collection buckets remain visible for a future scope
decision rather than being silently removed.

Run `python scripts/validate_index_overviews.py` after rebuilding snapshots. Its generated
operator report is written to ignored runtime output at `output/index_overview_validation.md`.
