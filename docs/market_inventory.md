# market.poke6s.com inventory: what to keep, move or retire

Last reviewed: 2026-11-20 (owner chose option B; Binder Art Maker and Collector Hub moved)

## Why this exists

market.poke6s.com was built as a separate site because the old Squarespace store couldn't host
custom tools: price research, Japanese box pricing, collector printables. poke6s.com now runs on
our own code (`storefront/`), so the owner asked (2026-11-20) whether the market site still
needs to be its own site, or whether it should move into poke6s.com (for example
poke6s.com/market). This page lists everything the market site does, with a suggested home for
each piece, so that decision can be made piece by piece.

**Decided 2026-11-20: option B** (below). Done so far: Binder Art Maker (now
poke6s.com/binder-art) and Collector Hub (poke6s.com/collector-hub). Next: the Placeholder
Library. The "Decide" rows
are still open.

Already done towards "one site": the same Shop / Market / Admin bar across the top of all three
sites (step 9a, 2026-11-20). Still planned: one Poke6s account (9b) and one owner home (9c),
see `docs/storefront_plan.md` step 9.

## How much each part is used

From the sites' own privacy-preserving visit counts (`data/processed/visitors.sqlite3` and
`data/storefront/visitors.sqlite3`; counting started 2026-11-05):

- **market.poke6s.com**: about 510 visitors in 16 days, 57% desktop, 41% phone, nearly all
  coming direct (few from search). About 600 of ~700 page views are `/`, the app itself.
  **Views inside the app (Discover lists, Sets, Market Stats, Track...) aren't counted
  separately**: they change the address's `?list=` part, which the counter drops. So the numbers
  say which standalone pages are used, not which app views are.
- Standalone pages, 16 days: Collector Hub 15, Account 15, Binder Art Maker 14, Placeholder
  Library 8, area landing pages under 5 each, Set Explorer 2, Budget Builder 1, market index
  pages a handful, printable checklists a few each.
- **poke6s.com**, for comparison: about 3,150 visitors in 12 days, but 97% with no referrer, so
  probably many undetected bots; treat it as "has traffic", not as a count. Its `/sets` pages
  (which already show market data) and articles get steady views.

**Suggested first step before deciding:** count app views by `?list=` for two to four weeks
(a small change to `visitors.py`: record the `list` value for `/` page loads). Then the
Discover / Research / Track decisions below rest on numbers.

## The parts, and where each could live

Homes: **Market** = stays a research site (at market.poke6s.com or poke6s.com/market);
**Store** = becomes part of poke6s.com's own pages; **Admin** = the store admin (home network);
**Retire** = remove; **Decide** = needs the owner.

### 1. Data pipeline (behind everything)

| Part | What it does | Suggested home |
| --- | --- | --- |
| Nightly TCGCSV price download and snapshots | Daily prices for every English and Japanese card and sealed product | **Keep, wherever the site lives.** The store depends on it: `market_reference.json` (03:00) sets store prices and the Pricing page |
| PriceCharting graded prices (one CSV a day) | Graded prices on card pages | Keep; must stay linked back to PriceCharting and deletable in one step (AGENTS.md) |
| Discord weekly posts, Grafana monitoring | Weekly market post, job and site health | Keep |
| Lorcana collection (not public) | Data for the planned Lorcana research site (`docs/lorcana_market_plan.md`) | Keep; the move decision affects where Lorcana pages would go |

### 2. The research app (`/`, desktop and phone)

| Part | What it does | Suggested home |
| --- | --- | --- |
| Discover lists (Top Movers, Pullbacks, Breakouts... 9 lists) | Daily screens of what's moving | **Market**. This is the research product; per-view counts would show which lists earn their place |
| Research: Sets, one set, Pokémon | Every set and card with prices and signals | **Market**, with the store's `/sets` pages linking to it (they already overlap: the store's set pages show market data for sealed) |
| Market Stats: Indexes, Set values, Market mood, Track record, Releases | Market-wide readings | **Market** |
| Buy: Sealed Deals, Budget Builder | Price per pack; best cards for a budget | **Decide.** Sealed Deals is close to what a shop shows (value per pack); it could become a store page that links to products we sell |
| Card page (prices, chart, graded tab, full data) | Everything about one card | **Market**; the store's product pages could link to the matching card page for price history |
| Track: Buy List, Watchlist, Favorites (signed in) | Saved cards per account | **Market**, and the heart of the "one account" step (9b): the same account could hold "notify me" items from the store |
| Search, nicknames ("moonbreon") | Find any card | Market; a shared search across store and market is a later idea |

### 3. Collector tools (Learn)

| Part | What it does | Suggested home |
| --- | --- | --- |
| Collector Hub (`/collector-hub`) | Master-set checklists and printables, explained | **Store. Moved 2026-11-20** to poke6s.com/collector-hub; its tiles still open the market's files until the Placeholder Library moves |
| Placeholder Library (`/placeholders`) and the printable checklists | Printable binder placeholders | **Store**, with Collector Hub |
| Binder Art Maker (`/binder-art`) | Free tool: one picture cut into card-size pieces for a binder page | **Store. Moved 2026-11-20** to poke6s.com/binder-art; the market address 301s there |
| How it works (guide) | Explains the market app | Market |

### 4. Owner tools (admin, signed in)

| Part | What it does | Suggested home |
| --- | --- | --- |
| Supplier Pricing (`/supplier-pricing`) | Read a supplier's price sheet (OCR), save quotes | **Admin.** Owner-only and about buying stock for the store; the costs file is already in the admin |
| Supplier Profitability (`/supplier-profitability`) | Supplier cost vs market vs store price, per channel | **Admin**, next to the Pricing page (both answer "what should this sell for") |
| Bug Reports (`/bug-reports`) | Reports from Discord and review notes | **Admin** (one owner home, step 9c) or stays; Discord stays the inbox either way |
| End of Day, Embed | Older research views | Kept as they are by D11 (2026-11-14). **Decide** whether they still earn their keep. (Dashboard Lab, the old prototype, was deleted 2026-11-20) |
| Admin index (`/admin`) | Links to the above | Becomes the "one owner home" (9c) |

### 5. Accounts

The market site has accounts (Google sign-in; saved lists and views, `/account-settings`). The
store has none (guest checkout through Stripe). Step 9b would make the market account the one
Poke6s account. On one domain (poke6s.com/market) that's simple: one sign-in cookie for both.
On two domains it needs a cookie shared across `.poke6s.com`. **This is the main technical
reason to settle the domain question before building 9b.**

## Three ways to go

**A. Two sites, made to feel like one (least work).** Keep market.poke6s.com. The shared bar
(done), one account across `.poke6s.com`, the collector tools and owner tools moved as above.
The research app stays where its links and Google ranking already are.

**B. Move the collector tools into the store, keep the research app separate (suggested).** As
A, plus Collector Hub, Placeholder Library and Binder Art Maker become poke6s.com pages (with
301 redirects from the old addresses). These are what collectors arrive for, they fit a shop,
and they're the most-used standalone pages. The research app stays a dedicated tool, linked
from the store's set and product pages.

**C. Everything under poke6s.com/market (most work).** The market app assumes it lives at the
root of its own domain: every page, script and data call uses addresses like `/search` and
`/images/...`. Serving it under `/market` means either changing all of those or routing
`poke6s.com/market/*` to the market container and rewriting addresses in the proxy, then
redirecting every old market.poke6s.com address. Worth it only if one domain matters more than
the work; it would also decide where the Lorcana site goes.

## Before deciding (owner)

1. Count app views per `?list=` for a few weeks (small change, no new costs), so the research
   app's parts are judged on use.
2. ~~Pick A, B or C.~~ B, 2026-11-20; Binder Art Maker moved first.
3. For each "Decide" row above: keep, move or retire.

Related: `docs/storefront_plan.md` (Phase 3 step 9), `docs/desktop_app.md` (the app's views),
`docs/api_routes.md` (every market route), `docs/lorcana_market_plan.md`.
