# Desktop App: Maintainer Guide

Last reviewed: 2026-10-12

The desktop app is one page that holds the site's five areas (Buy, Discover, Research,
Track, Learn) in a single three-column layout built for 1080p screens. It is served at
`/home-preview` while it is reviewed, and is meant to replace the older research dashboard
at `/` for desktop visitors. Phones keep the separate mobile page (`/mobile`).

- Page: `scripts/dashboards/desktop_home.html` (HTML, CSS and JavaScript in one file)
- Route: `desktop_home_preview` in `scripts/dashboards/api.py`
- Shared header: `scripts/dashboards/site_shell.js`

The top of the page's `<script>` has an overview comment with the same essentials as this
guide; keep the two in step.

## Layout

| Column | What it shows |
| --- | --- |
| Left | Every view, grouped Buy / Rising / Starting to move / Dipped / My lists / Research / Learn, with item counts |
| Middle | The current view: a table of cards, or a special view (Sets, Market Stats, Budget Builder grid, How it works) |
| Right | The selected card: price chart, 6-Month Hold, Targets, Graded and Stats tabs, plus Save, TCGplayer, Our store and Full data |

The page itself never scrolls; each column scrolls on its own. It is checked at 1920×1080,
1366×768 and 1280×720. Below 1500 px wide the trend column hides and spacing tightens.

## Views

Every view is one entry in the `LISTS` array; its fields are documented above the array.

| Area | View (`?list=`) | Data |
| --- | --- | --- |
| Buy | Sealed Deals (`sealed_deals`) | `/sealed_deals` (all products, no price limits) |
| Buy | Budget Builder (`budget`) | `/budget_builder` |
| Discover | Top Movers, Confirmed Movers, Consistent Movers, Early Movers, Under the Radar, Breakouts, Pullbacks | `/<key>` (for example `/top_movers`) |
| Track | All saved, Buy List, Watchlist, Favorites, Needs Review (`track_*`) | `/tracking/items/resolve`, with tags from browser storage or `/tracking/tags` |
| Research | Sets (`sets`) → one set (`set`) | `/group_signals` → `/group_products` |
| Research | Pokémon (`species`) | `/browse_species` |
| Research | Market Stats → Indexes (`indexes`) → one index (`index`) | `/index-overview-data` |
| Research | Market Stats → Set values (`set_values`) → one set with its chart | `/set_baskets`, then `/group_products` and `/group_series` |
| Learn | How it works (`guide`) | Static text written by `renderGuide()` |

Other endpoints: `/search` (top search box), `/universe` (opening a card from a link),
`/health` ("Prices as of"), `/series` (price chart), `/sparkline_batch` (trend column),
`/hold_check`, `/graded_prices`, `/store_link`, `/set-logos.json`.

### Adding a view

- **A new market screener** whose API route is `/<key>` and returns `{columns, rows}`: add
  one `LISTS` entry with `price`, `change` and `changeLabel`. Nav, counts, sorting, filters
  and the right column work without further code.
- **Anything else**: add a `LISTS` entry, a branch in `fetchList()` that returns rows
  through `normalizeRow()`, and, if it needs its own table or controls, the show/hide lines
  in `loadList()` plus a renderer called from `rerender()`.

## Rules worth knowing

- **Graded prices must link back.** PriceCharting allows display only if every price links to
  its PriceCharting product page. The Graded tab hides the link only when no price is shown.
- **Sealed Deals compares with loose packs, not retail.** Only plain "<Set> Booster Pack"
  listings count as loose prices (no mini, promo, pin-bundle or 1st Edition/Unlimited
  packs), and only booster boxes, half boxes, bundles, ETBs, blisters and sleeved packs are
  compared, unless the pack mix was entered by hand. Collection boxes and tins otherwise
  show "—", because their contents are only a guess. See `loosePackPrices()` and
  `packBreakdown()`.
- **Budget Builder** picks are chosen on the server (`budget_builder_recommendations` and
  `_select_budget_candidates` in `api.py`). It needs four price readings in the last week,
  or every reading available after a feed gap.
- **Discover lists survive feed gaps.** Each screener's activity bars ("10 different prices
  in 30 days", "4 readings in 7 days") are scaled to the days that actually have prices in
  that window (`gap_scaled_threshold` in `query_support.py`), so a gap in the TCGCSV feed
  doesn't empty every list. With a complete feed the bars are unchanged. Early Movers and
  Under the Radar can still be empty: they also need three rises in a row.
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
`/desktop-theme.css`. Its color tokens copy the `:root` block in `desktop_home.html`;
change both together.

Account links each saved list to `/home-preview?list=track_<tag>` through its `APP`
constant. Update that constant when the app moves to `/`.

## Testing

- Server tests: `docker-compose exec -T pokemon-momentum python -m pytest -q -p no:cacheprovider tests`
- There are no automated browser tests for this page in the repository yet. After a change,
  open each area at 1920×1080 and 1366×768 and check:
  - the page doesn't scroll;
  - the browser console shows no errors;
  - links with `?list=` and `?product=` reopen the same view and card.

## Planned next steps

These are tracked, with their status, in `docs/todo.txt`.

1. Serve this page at `/` for desktop visitors (phones keep `/mobile`), point the header menus
   (`AREAS` in `site_shell.js`) at its views, and forward old pages (`/sealed-deals`,
   `/budget-builder`, `/set-explorer`, `/index-overview*`, the area pages) to their new views.
2. Retire the older research dashboard behind "Full data →" (decided 2026-10-12): move
   what's worth keeping into the Stats tab, then remove the link.
