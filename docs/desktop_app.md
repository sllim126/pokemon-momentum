# Desktop App: Maintainer Guide

Last reviewed: 2026-11-09

The desktop app is one page that holds the site's five areas (Buy, Discover, Research,
Track, Learn) in a single three-column layout built for 1080p screens. Since 2026-10-12 it
is the desktop home page at `/`. Phones and tablets keep the separate mobile page.

- Page: `scripts/dashboards/desktop_home.html` (HTML, CSS and JavaScript in one file)
- Route: `dashboard` (`/`) in `scripts/dashboards/api.py`

## Older addresses forward here (desktop only)

`desktop_app_redirect` and the `forward_desktop_to_app` middleware in `api.py` send desktop
visitors from each older page to the matching view, so bookmarks, shared links and the
shared header's menu links (which still name the older pages) keep working. Phones and
tablets are never forwarded and still get the older pages.

| Older address | Desktop app view |
| --- | --- |
| `/dashboard`, `/?tab=…` (old dashboard links) | the same list (`OLD_TAB_TO_APP_LIST`), with `product`, set, market and Singles/Sealed carried over |
| `/home-preview` | `/` (the app's former preview address) |
| `/sealed-deals`, `/budget-builder`, `/set-explorer`, `/index-overview` | Sealed Deals, Budget Builder, Market Stats → Set values, Market Stats |
| `/index-overview-<index>` (e.g. `-sv100`, `-jp-pokemon100`) | that index |
| `/buy`, `/discover`, `/research`, `/track`, `/learn` | Sealed Deals, Consistent Movers, Sets, All saved, How it works |
| `/mobile?mode=tracked&tracked_tag=<tag>` | that saved list |

The older research dashboard was retired on desktop on 2026-10-16: `/dashboard-classic` and
`/dashboard-dev` forward to the app like `/dashboard`. `dashboard.html` was deleted on
2026-10-31, once the mobile app had its own Sets, Pokémon and Market Stats views.
- Shared header: `scripts/dashboards/site_shell.js`

The top of the page's `<script>` has an overview comment with the same essentials as this
guide; keep the two in step.

## Layout

| Column | What it shows |
| --- | --- |
| Left | Every view, grouped Buy / Rising / Starting to move / Dipped / Research / My lists / Learn (the header menu's order), with item counts |
| Middle | The current view: a table of cards, or a special view (Sets, Market Stats, Budget Builder grid, How it works) |
| Right | The selected card: price chart, 6-Month Hold, Targets, Graded and Stats tabs, plus Save, TCGplayer, Our store and Full data |

**Full card page.** "Full data →", or double-clicking a card's picture, switches to the full
card page (`&card=1` in the address): the card panel widens over the list, shows every tab at
once, and adds every snapshot number (`/card_details`), the market lists the card is in today,
and its other printings. "← Back", Esc or any menu item returns to the list, which stays
loaded underneath. See `setFullCard()` and `loadFullExtra()`.

The page itself never scrolls; each column scrolls on its own. It is checked at 1920×1080,
1366×768 and 1280×720. Below 1500 px wide the trend column hides and spacing tightens.
Below 1100 px wide (an iPad in portrait) the left column folds into a "☰ Lists" button in the
toolbar that opens it as a panel over the page; the price date and the "/" hint hide, card
names wrap to two lines, and the card's buttons wrap. Checked at 768×1024 and 820×1180.

## Views

Every view is one entry in the `LISTS` array; its fields are documented above the array.

| Area | View (`?list=`) | Data |
| --- | --- | --- |
| Buy | Sealed Deals (`sealed_deals`) | `/sealed_deals` (all products, no price limits) |
| Buy | Budget Builder (`budget`) | `/budget_builder` |
| Discover | Top Movers, Confirmed Movers, Consistent Movers, Early Movers, Under the Radar, Breakouts, Pullbacks, Small Dips, Whole-set Dips | `/<key>` (for example `/top_movers`; Pullbacks is `/good_buys`, Whole-set Dips `/set_dips`, Small Dips `/small_dips`; rules in `query_support.build_pullback_sql`) |
| Track | All saved, Buy List, Watchlist, Favorites, Needs Review (`track_*`) | `/tracking/items/resolve`, with tags from browser storage or `/tracking/tags` |
| Research | Sets (`sets`, with a 90-day trend line per set) → one set (`set`, with the set's value chart since release) | `/group_signals` and `/group_sparklines` → `/group_products` and `/group_series?since_release=true` |
| Research | Pokémon (`species`) | `/browse_species` |
| Research | Market Stats → Indexes (`indexes`) → one index (`index`) | `/index-overview-data` |
| Research | Market Stats → Set values (`set_values`) → one set with its chart | `/set_baskets`, then `/group_products` and `/group_series` |
| Research | Market Stats → Market mood (`mood`): 0-100 breadth reading, its three parts, two-year chart | `/market_mood` |
| Research | Market Stats → Track record (`track_record`): each list's hit rate after 7/14/30 days against the market, the same split by price with after-fees results, latest picks (click one to open it) | `/track_record` |
| Research | Market Stats → Releases (`releases`): coming up, and sets out in the last 120 days (row opens the set, top card opens the card) | `/releases` |
| Learn | How it works (`guide`; the header's Learn opens it) | Static text written by `renderGuide()`, including the "Thanks to TCGCSV" support link. No card column (`.layout.no-detail`) |

The three views added 2026-11-06 (mood, track record, releases) are `INSIGHT_VIEWS`: they render
into the guide panel (`loadInsight`), keep the card column, and have no rows or menu counts.
The card column's Graded tab shows four price boxes (ungraded, grade 9, PSA 10, CGC 10) and,
under them, a "See all grades" dropdown (closed by default) with every grade PriceCharting lists
(grades 7-9.5, the four 10s, each with its multiple of the raw price); the Full data page adds Price records (all-time high
and low, 1/3/5-year change). Searching a
collector nickname ("moonbreon") shows a note above the results.

Other endpoints: `/search` (top search box), `/universe` (opening a card from a link),
`/health` ("Prices as of"), `/series` (price chart), `/sparkline_batch` (trend column),
`/hold_check`, `/graded_prices`, `/card_details` (full card page), `/store_link`, `/set-logos.json`.

### Adding a view

- **A new market screener** whose API route is `/<key>` and returns `{columns, rows}`: add
  one `LISTS` entry with `price`, `change` and `changeLabel`. Nav, counts, sorting, filters
  and the right column work without further code.
- **Anything else**: add a `LISTS` entry, a branch in `fetchList()` that returns rows
  through `normalizeRow()`, and, if it needs its own table or controls, the show/hide lines
  in `loadList()` plus a renderer called from `rerender()`.

## Rules worth knowing

- **Graded prices must link back.** PriceCharting allows display only if every price links to
  its PriceCharting product page. The Graded tab hides the link only when no price is shown. Its "Ungraded" box is our
  TCGplayer market price (the card's main price); PriceCharting's ungraded price is in the note
  below it (owner, 2026-10-29).
- **Sealed pack counts** come from the product name (`_infer_pack_count` in `api.py`) unless
  entered by hand (`_SEALED_PACK_COUNT_OVERRIDES_BY_PRODUCT_ID`,
  `_SEALED_PACK_COMPOSITION_OVERRIDES`); cases multiply their contents (ETB cases via
  `_etb_case_override`). A product type must be in the confidence sets in `sealed_deals`
  or the product is left out of the list.
- **Sealed Deals compares with loose packs, not retail.** Only plain "<Set> Booster Pack"
  listings count as loose prices (no mini, promo, pin-bundle or 1st Edition/Unlimited
  packs), and only booster boxes, half boxes, bundles, ETBs, blisters and sleeved packs are
  compared, unless the pack mix was entered by hand. Collection boxes and tins otherwise
  show "—", because their contents are only a guess. See `loosePackPrices()` and
  `packBreakdown()`.
- **Budget Builder** picks are chosen on the server (`budget_builder_recommendations` and
  `_select_budget_candidates` in `api.py`). It needs four price readings in the last week,
  or every reading available after a feed gap. Popular Pokémon (`POPULAR_POKEMON` in `api.py`)
  get a +10 tie-breaker and a "Popular Pokémon" label; price momentum stays the main signal.
- **Budget Builder keep and cycle** (desktop and phone, 2026-11-05). "Keep" only marks a pick
  on the page (no reload) and adds it to the link as `keep=`. "New picks" adds every unkept
  pick on screen to `budgetSeen` and asks again with `keep_keys` (kept cards go in first,
  whatever they cost) and `exclude_keys` (owned + seen), so it never repeats a card. When no
  new card is left it clears `budgetSeen` and starts over. Changing the budget or any menu
  clears `budgetSeen` but keeps the kept cards.
- **Graded Budget Builder (Beta)** (`graded_budget`, desktop and phone, 2026-11-05) shares
  Budget Builder's controls, Keep and New picks (`isBudgetList`, `pickKey`). It calls
  `/graded_budget_builder`: PriceCharting prices for PSA 10 / grade 9 / grade 8, each grade
  scored on its own history (never assuming a higher grade costs more). Every graded price is
  a link to PriceCharting (licence condition). Clicking a pick opens the card's raw details
  (`loadCardByKey` / `openProductFromLink`), not the slab price. Switching between the two
  builders clears kept, seen and swapped cards. Trends use up to 30 days; the history started
  2026-10-09, so the window is shorter until mid-November.
- **Back button into sets and indexes** (2026-11-05). Opening a set or an index pushes a
  history entry (`pushDrillEntry`), so the browser's Back returns to Sets / Market Stats, and
  "← All sets" goes back through it. The `popstate` handler reopens whatever view the
  address names. Picking cards inside a view still replaces the address (no entry per card).
- **Indexes leave out cards with flip-flopping prices** (2026-11-05). A card whose market price
  jumped 3x or more day to day at least 4 times in the last 365 days is left out of the index
  for its whole history (`UNSTABLE_*` and `_unstable_cards_cte` in `api.py`). The Method tab
  lists the left-out cards that could have qualified. Base Set Charizard 1st Edition (27 jumps
  between ~$400 and $10,000) had made WOTC 100 move 25-38% in a day on 56 days.
- **Breakouts** (2026-11-05) need the price above the previous 90-day high *and* at least 5%
  above the 30-day average (`min_vs_sma30_pct` on `/breakouts`), so a flat card ticking a cent
  past its old high doesn't count.
- **Cards TCGplayer moved to another set** keep one history: the series, sparkline and signal
  snapshot builders read prices through `prices_by_current_group` (`scripts/common/price_views.py`),
  which files every price under the card's current set. Before 2026-11-05 such cards had two
  chart rows and `/series` sometimes showed the old one (e.g. a chart ending Aug 2025).
- **Discover lists survive feed gaps.** Each screener's activity bars ("10 different prices
  in 30 days", "4 readings in 7 days") are scaled to the days that actually have prices in
  that window (`gap_scaled_threshold` in `query_support.py`), so a gap in the TCGCSV feed
  doesn't empty every list. With a complete feed the bars are unchanged. Early Movers and
  Under the Radar can still be empty: they also need three rises in a row.
- **Owner Note tab.** Signed in as an admin, a floating "Note" tab on the right saves a review
  note (with files) to `/bug-reports`, with the list, card and screen size, as on the phone
  app. For testing on an iPad, which gets the desktop site. Visitors never see it; the same
  admin check (`checkOwner()`) also drops the affiliate wrapper from TCGplayer links.
- **Start list.** With no list in the address the app opens on Consistent Movers (`DEFAULT_LIST`;
  owner's choice, confirmed 2026-11-09); `/discover` points at the same list.
- **TCGplayer links** preselect the language, the printing and Near Mint (not for reverse holos,
  where TCGplayer's combined filter often finds nothing), wrapped in the affiliate link. When an
  admin is signed in the link goes straight to TCGplayer, so the owner's own buys are never
  self-referred (`checkOwner()`, `/tracking/session`). The phone app does the same.
- **Charts and the 6-Month Hold read nightly snapshots**, not the raw history: 7D/30D/90D/1Y
  from the daily 730-day series snapshot, "All" and the hold check from that plus the
  weekly long-history snapshot (`snapshot_history` in `api.py`). In the hold check's track
  record each weekly point counts for the days it stands for, so its numbers match daily data.
- **6-Month Hold** comes from `scripts/dashboards/hold_check.py`; fee and shipping
  assumptions are in `scripts/common/selling_costs.py` (see the operator runbook).
- **Stale responses are dropped.** `listRequestSeq` and `detailRequestSeq` increase with
  every request, so an answer for a view or card the visitor has already left is ignored.
  Follow the same pattern for any new request.
- **`[hidden]` needs CSS.** Elements given `display: flex` or `inline-flex` ignore the
  `hidden` attribute unless a `[hidden] { display: none }` rule exists for them. Each such
  component in this file has one; add one for new components.
- **The header replaces its slot.** `site_shell.js` swaps `<div id="site-shell">` for a
  `<header class="ps-shell">`, so select `.ps-shell`, not `#site-shell`. The app updates
  the header's highlighted area itself when the visitor changes views.
- **Footer.** The legal line (not financial advice, Pokémon trademarks, affiliate links)
  must stay on the page.

## State, links and storage

`state` holds everything on screen; `syncUrl()` writes it into the address bar, so every
view can be bookmarked. URL parameters: `market=JP`, `kind=sealed`, `list`, `product`
(`productId||subType`), `set`, `filter`, `from=stats`, `q`, `era`, `index`, `tab`, `type`,
`budget`, `max`, `rarity`, `cards`, `per_set`, `skip`.

Browser storage, shared with the mobile site:

| Key | Contents |
| --- | --- |
| `pokemon-momentum-tags` | Saved lists: `{"productId||subType": ["owned", "watchlist", ...]}` |
| `pokemon-momentum-price-targets` | `{"categoryId||productId||subType": {buy, sell}}`; stays in each browser |
| `pokemon-momentum-tracking-token` | Sign-in token (also the `pm_tracking_token` cookie); when present, lists sync with `/tracking/tags` |

Saved-list tag names: `owned` is shown as Buy List, `research` as Needs Review.

## Other desktop pages in the same style

Collector Hub (`collector_hub.html`), Placeholder Library (`placeholder_library.html`) and
Account (`account_settings.html`) load `scripts/dashboards/desktop_theme.css`, served at
`/desktop-theme.css`. Collector Hub and Placeholder Library are scrolling "reading pages"
(since 2026-10-13): Collector Hub explains master-set collecting in three steps and shows
each resource as a tile; Placeholder Library lists every file by who it's for. Both take
their items from the manifests and their wording from a `COPY` table in the page, keyed by
item id (a new item falls back to the manifest's summary). Resources open in a new tab;
nothing is embedded. Its color tokens copy the `:root` block in `desktop_home.html`;
change both together.

Account links each saved list to `/?list=track_<tag>` through its `APP` constant.

Binder Art Maker (`binder_art.html`, route `/binder-art`, linked from Learn and Collector Hub)
also uses `desktop_theme.css` and fills the window like the app: settings on the left (they
scroll on their own), the preview on the right. It is a free tool that cuts one image into
63 × 88 mm pieces for a 2×2 to 5×5 binder page or two-page spread, leaving out the strips
behind the pocket seams so the picture lines up. Everything happens in the visitor's browser
(the image is never uploaded); downloads are a print PDF with cut marks (jsPDF) or a ZIP of
300 DPI PNGs (JSZip), both loaded from cdnjs. The set-logo dropdown comes from
`/binder-art/logos.json`, built from `images/logos/` (identical files that
`sync_set_logos.py` copied to several groupIds become one entry; POP, Black Star and
McDonald's badges are listed last under family names, see `BINDER_ART_LOGO_FAMILIES` in
`api.py`). A new logo shows up there once it's added with `sync_set_logos.py`.

## Testing

- Server tests: `docker-compose exec -T pokemon-momentum python -m pytest -q -p no:cacheprovider tests`
- There are no automated browser tests for this page in the repository yet. After a change,
  open each area at 1920×1080 and 1366×768 and check:
  - the page doesn't scroll;
  - the browser console shows no errors;
  - links with `?list=` and `?product=` reopen the same view and card.

## Planned next steps

These are tracked, with their status, in `docs/todo.txt`.

1. Retire the older research dashboard behind "Full data →" (decided 2026-10-12): move
   what's worth keeping into the Stats tab, then remove the link.
