# Site routes

Last reviewed: 2026-11-05

Every route served by `scripts/dashboards/api.py` (FastAPI, port 8001 in the container,
public at market.poke6s.com). `tests/test_api_routes.py::test_every_route_is_in_the_route_reference`
fails when a route exists in `api.py` but not in this file, so add a line here when you add a
route.

Access: **public** = anyone; **signed in** = a Google sign-in token (`pokemon-momentum-tracking-token`
or the `pm_tracking_token` cookie); **admin** = signed in with an address listed in
`POKEMON_MOMENTUM_ADMIN_USERS` (`require_admin_user`; 401/403 otherwise).

## Pages

| Route | Page file | Access | What it is |
| --- | --- | --- | --- |
| `/` | `desktop_home.html` (desktop), `mobile_app.html` (phones/tablets) | public | Home. Desktop app guide: `docs/desktop_app.md` |
| `/dashboard`, `/dashboard-classic`, `/dashboard-dev` | `mobile_app.html` (phones) | public | Older research dashboard; desktop visitors are forwarded to `/` (see `docs/desktop_app.md`); phones get the mobile app (old Browse Set, Browse Pokémon and Set Strength links open its Sets and Pokémon views since 2026-10-31); nobody is served `dashboard.html` any more, and it is deleted in the mobile clean-up (1.6) |
| `/buy`, `/discover`, `/research`, `/track`, `/learn` | `area_hub.html` | public | Landing page for each area (registered in a loop in `api.py`); desktop visitors are forwarded to the matching app view |
| `/home-preview` | | public | Former preview address of the desktop app; forwards to `/` |
| `/mobile` | `mobile_app.html` | public | The mobile app (switched on early by the owner, 2026-10-31; `docs/mobile_app_plan.md`) |
| `/mobile-classic`, `/mobile-rebuild` | 301 to `/mobile` | public | The old phone page and the old mobile concept page, deleted 2026-10-31 |
| `/m-next` | `mobile_app.html` | public | The mobile app's address while it was built; kept for links and review notes |
| `/dashboard-lab` | `dashboard_lab.html` | public | Experimental dashboard, linked from Admin only |
| `/collector-hub` | `collector_hub.html` | public | Collector resources (Learn) |
| `/placeholders` | `placeholder_library.html` | public | Placeholder Library: print pages and downloads (Learn) |
| `/binder-art` | `binder_art.html` | public | Free Binder Art Maker (Learn); see `docs/desktop_app.md` |
| `/set-explorer`, `/budget-builder`, `/sealed-deals` | `set_explorer.html`, `budget_builder.html`, `sealed_deals.html` | public | Older standalone pages; desktop visitors are forwarded into the app |
| `/index-overview`, `/index-overview-<index>` | `index_overview_hub.html`, `index_overview_detail.html` | public | Market index pages; desktop visitors are forwarded to Market Stats |
| `/account-settings` | `account_settings.html` | public (works when signed in) | Sign-in, saved lists, saved views |
| `/admin` | `admin_hub.html` | admin | Links to the admin tools below |
| `/bug-reports` | `bug_reports.html` | admin | Review queue: Discord bug reports, old mobile Feedback form entries and `/m-next` owner review notes |
| `/pricing-upload` | `pricing_upload.html` | admin | Upload a Squarespace export and preview price coverage |
| `/single-listings-upload` | `single_listings_upload.html` | admin | Singles listing workflow (intake, drafts, follow-up) |
| `/supplier-pricing` | `supplier_pricing.html` | admin | Read supplier price sheets with OCR (tesseract.js) and compare to market |
| `/supplier-profitability` | `supplier_profitability.html` | admin | Per-SKU profit for supplier buys |
| `/eod-dashboard`, `/embed` (GET and HEAD) | `eod_dashboard.html`, `embed_dashboard.html` | admin | Index/EOD research views |

## Shared files

| Route | Access | What it is |
| --- | --- | --- |
| `/site-shell.js` | public | Shared header and menu for every page (`site_shell.js`) |
| `/desktop-theme.css` | public | Shared look for desktop pages outside the app |
| `/dashboard-common.js` | public | Helpers shared by the older pages (`dashboard_common.js`) |
| `/images/{filename}` (GET and HEAD) | public | Files under `images/` (logos, set logos in `images/logos/<groupId>.png`) |
| `/set-logos.json` | public | Group IDs that have a set logo |
| `/binder-art/logos.json` | public | Binder Art Maker's logo dropdown (one entry per distinct logo) |
| `/collector-manifest`, `/collector-assets/{bucket}/{path}` | public | Collector Hub's list of resources and their files |
| `/placeholder-downloads-manifest`, `/placeholder-downloads/{asset_id}` | public | Placeholder Library's list of files and the downloads |

## Market data (JSON, public)

`category_id` picks the market (3 = Pokémon English, 85 = Pokémon Japanese; see `/categories`).

| Route | Used by | What it returns |
| --- | --- | --- |
| `/health` | monitoring, every page | Latest data date and history source for a market |
| `/categories` | market pickers | The markets the site covers |
| `/search` | search boxes | Cards and sets matching a query (server-side search) |
| `/product_picker` | pickers | One page of the catalogue |
| `/universe` | desktop app | The tradable card list for a market |
| `/groups` | pickers | Sets with active products |
| `/group_products` | Browse a Set | Every card in one set with prices and signals |
| `/browse_species` | Browse a Pokémon | Every print of one Pokémon |
| `/group_signals`, `/group_series` | Set Strength, set charts | Set-level signals and a set's price series (`since_release=true` starts at the set's release date) |
| `/group_sparklines` | Desktop Sets view, 90-day trend column | Every set's 90-day equal-weight index in one call (about 15 points each; sets released inside the window start at release); cached until the next night's prices |
| `/set_baskets` | Market Stats > Set values | What a set costs and where its value sits |
| `/series` | every chart | Price history for one product |
| `/card_details` | desktop card page | Everything the nightly snapshot knows about one printing |
| `/graded_prices` | card page | PriceCharting graded prices (always shown with a PriceCharting link) |
| `/hold_check` | 6-Month Hold | Whether buying today could sell at a profit after a horizon |
| `/sparkline_batch` (POST) | lists | Small price series for many products at once |
| `/store_link` | card page | The Poke6s store listing for a product, when in stock |
| `/sealed_deals` | Sealed Deals | Sealed products with pack counts and price per pack |
| `/budget_builder` | Budget Builder | Cards that fit a budget, spread across sets; `exclude_keys` skips cards (owned or already shown), `keep_keys` keeps cards and fills the rest around them |
| `/top_movers`, `/breakouts`, `/sma30_holds`, `/confirmed_uptrends`, `/under_the_radar`, `/early_uptrends` | screener lists | One screener each: `{columns, rows}` |
| `/good_buys`, `/small_dips`, `/set_dips` | Pullbacks, Small Dips, Whole-set Dips | Pullback screeners; rules in `query_support.build_pullback_sql` |
| `/time_to_buy` | none (its page was deleted 2026-10-31) | Older Time to Buy screener, dropped from the desktop and mobile apps; the route remains for now |
| `/product_signals` | no page today | Raw nightly signal rows; kept for scripts and checks |
| `/index-overview-data` | Market Stats, index pages | Everything one market index page needs. Public; the rebuild option is admin-only |
| `/api/psa/cert/{cert_number}` | card page | PSA cert details. Fresh cached results are public; a new lookup uses our PSA account and is admin-only |

## Sign-in and saved lists

| Route | Access | What it does |
| --- | --- | --- |
| `GET /tracking/auth_config` | public | Google client settings for the sign-in button |
| `POST /tracking/google_session` | public | Exchange a Google sign-in for a site token |
| `GET /tracking/session`, `DELETE /tracking/session` | signed in | Who is signed in (and whether they're an admin); sign out |
| `POST /tracking/session` | public | Retired username + PIN sign-in; always refuses |
| `GET/PUT /tracking/tags`, `POST /tracking/tags/merge` | signed in | Saved lists (Buy List, Watchlist, Favorites, Needs Review) |
| `GET/POST /tracking/views`, `DELETE /tracking/views/{view_id}` | signed in | Saved views |
| `POST /tracking/items/resolve` | public | Turn saved-list rows into display-ready items |
| `DELETE /tracking/account` | signed in | Delete the account and its saved data |

## Feedback

| Route | Access | What it does |
| --- | --- | --- |
| `POST /bug_reports` | public | Old mobile Feedback form (the new pages send people to Discord instead) |
| `GET /bug_reports` | admin | The review queue's rows |
| `POST /m-next/review-notes` | admin | The owner's floating review note on `/m-next`, with optional files |
| `GET /m-next/review-attachments/{stored_name}` | admin | Download a review-note attachment |

## Admin data

| Route | Access | What it does |
| --- | --- | --- |
| `/eod/market_details`, `/eod/index_list`, `/eod/index_components`, `/eod/series` | admin | EOD Historical Data index research |
| `POST /pricing-upload/compare` | admin | Archive a Squarespace export and preview pricing coverage |
| `/single-listings/intake-download`, `/single-listings/draft-download`, `/single-listings/followup-download` | admin | Singles workflow downloads |
| `POST /single-listings/intake-upload`, `/single-listings/source-upload`, `/single-listings/draft-upload` | admin | Singles workflow uploads |
| `POST /supplier-pricing/enrich`, `POST /supplier-pricing/save` | admin | Match OCR-read supplier rows to SKUs and market prices; save reviewed quotes |
| `POST /supplier-profitability/data`, `GET /supplier-profitability/fx` | admin | Profit rows per SKU; latest JPY-per-USD rate |
