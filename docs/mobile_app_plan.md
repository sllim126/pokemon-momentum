# New mobile app: plan

Last reviewed: 2026-10-23

The owner asked on 2026-10-21 for a full rebuild of the phone site: same features as today,
in the desktop app's look, and a layout that fills any screen from a phone in portrait to a
tablet in landscape. This file is the plan, the inventory of what phones get today, and the
list of decisions the owner needs to make. Work items are tracked in `docs/todo.txt`
("New mobile app").

Background: `docs/mobile_redesign_notes.txt` (April 2026). Its main idea still holds: on a
phone the site is a quick "is this a good deal?" tool at a card table, not a research desk.

## Why a rebuild

- Phones get two old pages: `mobile_dashboard.html` (`/mobile`, and `/` on phones) and, for
  Browse Set, Browse Pokémon, Set Strength and Time to Buy, the retired research dashboard
  (`dashboard.html`). Both use the old look, and the menu is the old one.
- The mobile page is a fixed-width column (about 430 px). In landscape it leaves empty space on
  both sides, and the Save / Full Data bar overlaps the card.
- Many desktop views are missing on phones: Small Dips, Whole-set Dips, Budget Builder, Sets,
  Pokémon, Market Stats (indexes and set values) and How it works.

## How it is built

- **One new page, `scripts/dashboards/mobile_app.html`, served at `/m-next`** while it is being
  built. The current `/mobile` keeps running untouched until the switch (stage 1, last step).
  `/m-next` opens on any device, so it can also be checked in a desktop browser.
- **The desktop app's look and data.** Same colors and type as `desktop_home.html`, and the
  same endpoints (`/top_movers`, `/search`, `/series`, `/hold_check`, ...). Saved lists and
  price targets use the same browser storage keys as desktop and the old mobile page, so
  nothing is lost at the switch.
- **Fluid from the start (stages 1 and 2 together).** Nothing has a fixed width. The layout
  follows the screen size, not the device type:

  | Screen width | Layout |
  | --- | --- |
  | under 700 px (phone, portrait) | One pane. A list, or a card on its own screen. Menu button now; a tab bar at the bottom from step 1.3. |
  | 700 px and up (phone landscape, tablet portrait) | Two panes: list on the left, card on the right. |
  | 1100 px and up (tablet landscape) | Two panes with the menu shown as a side rail. |

- **Back button works.** Opening a card, a list or a set adds a browser history entry, so the
  phone's Back gesture goes back inside the app instead of leaving the site.
- **Footer legal line** (not financial advice, trademarks, affiliate links) stays, as on
  desktop. Graded prices keep their PriceCharting link.

## Stages

**Owner feedback, 2026-10-22:** step 1.1 looked right but didn't answer the phone's real
question, "I have a card in front of me: is it a good buy?". So the Deal Checker moved
ahead of everything else: the home screen is now "Check a card" (search, then type the
asking price), and the browsing lists moved to the menu.

### The Deal Checker (in `mobile_app.html`, `judgeDeal()`; numbers in `DEAL`)

- **To keep:** 10%+ under market is a "Good buy", up to 5% over is "Fair price", more is
  "Overpriced". "Pay up to" is 90% of market.
- **To resell:** what a sale at market leaves after TCGplayer fees and shipping
  (`/hold_check`'s `net_at_market`, from `scripts/common/selling_costs.py`) minus the asking
  price. 20%+ profit is a "Good flip", 0-20% "Thin profit", below that "Loss if resold now".
  "Pay up to" leaves a 20% profit.
- **Warnings:** a card 40%+ above its 30-day average has "just spiked" ("pay up to" then
  uses the average); one down 10%+ in 30 days "is falling". Either turns a buy into
  "Careful: …". Fewer than 3 different prices in 30 days, or no change for 30 days, adds "few
  recent sales, the market price is a rough guide".
- Under the card name: how active its price is, and buttons for its other printings
  (Holofoil, Reverse Holofoil, …), so you can check you have the right one.
- Below: the price chart and the 6-Month Hold for the typed price.

1. **Rebuild the phone site in the new design (fluid from the start).** Steps, each one usable
   on `/m-next` before the next starts:
   1. Framework and the Deal Checker: "Check a card" home screen with recent checks, the
      Deal Checker, 6-Month Hold, chart, menu with the Discover lists. (Done 2026-10-22.)
   2. Rest of the card view: Graded prices, Stats, Targets, Save, Our store.
   3. My lists (All saved, Buy List, Watchlist, Favorites, Needs Review) and sign-in.
   4. Research: Sets → one set, Pokémon, Market Stats (indexes and set values).
   5. Buy: Sealed Deals, Budget Builder. Learn: How it works.
   6. Switch: phones get the new page at `/` and `/mobile`; the old page stays at
      `/mobile-classic` for a few weeks, then it and `dashboard.html` are deleted.
2. **Bigger screens.** Tune the two-pane layouts for phone landscape and tablets, and decide
   whether iPads get this app or the desktop app (see decisions below).
3. **Refine.** Fixes from real-phone testing; speed on a weak signal at a card show; search
   that understands "025/165" or "TLA 037".
4. **New features.** Camera / OCR card lookup (todo item "Card scanning"). Reading the card
   number with on-device OCR is free; recognising a card from its picture usually needs a
   paid service, so the plan starts with the number.

## What phones get today, and the plan for each

| Today (old mobile page) | New app |
| --- | --- |
| Search box, English/Japanese, Singles/Sealed | Keep (step 1.1) |
| Home feed of cards with sparklines | Discover lists, opening on Top Movers like desktop (1.1) |
| Menu "Trending": Top Movers, Early Movers, Pullbacks, Under the Radar, Confirmed Movers, Consistent Movers, Breakouts | Keep, plus Small Dips and Whole-set Dips (1.1) |
| Selected card: picture, price, trend, Stats / Graph / Insight tabs | Card view (1.1, 1.2) |
| Deal Checker | Keep; it is the core phone tool (1.2) |
| 6-Month Hold, Graded prices | Keep (1.2) |
| Save sheet (Favorite, Watchlist, Needs Review, Buy List) | Keep (1.2) |
| Full Data pop-up with TCGplayer and Store links | Folded into the card view (1.2) |
| Profile: Tracked Items, Favorites, Watchlist, Needs Review, Buy List | My lists (1.3) |
| Account settings, sign out | Keep (1.3) |
| Browse Set, Set Strength (old research dashboard) | Sets → one set (1.4) |
| Browse Pokémon (old research dashboard) | Pokémon (1.4) |
| Time to Buy (old research dashboard) | **Decision 3** |
| 14 separate index pages (English and Japanese) | Market Stats view, like desktop (1.4) |
| Set Explorer page | Covered by Sets (1.4); **decision 4** |
| Sealed Deals page | Sealed Deals view (1.5) |
| Alerts & Targets (hidden, never finished) | **Decision 2** |
| Feedback & Support: on-site feedback form, Discord bug link, Quick Guide | **Decision 1**; guide becomes How it works (1.5) |
| Not on phones today: Budget Builder | Add (1.5) |

## Decisions for the owner

1. **Feedback form.** Today's phone page has an on-site feedback form. The rule is "bug
   reports go to Discord, no on-site forms on new pages". Proposal: a "Report a bug / send
   feedback" link to the Discord server, nothing on the page.
2. **Alerts & Targets.** Hidden on today's page and never finished. Proposal: leave it out;
   the card view gets the desktop Targets tab (your buy and sell prices per card). Alerts can
   come back in stage 4.
3. **Time to Buy.** Only the old research dashboard has it; the desktop app dropped it.
   Proposal: drop it.
4. **Set Explorer.** A separate page; the Sets view covers the same ground. Proposal: no
   separate page on phones.
5. **iPads (stage 2).** Today iPads in their default "desktop website" mode get the desktop
   app, and iPads that report as mobile get the phone page. Proposal: decide after using
   `/m-next` on the iPad in stage 2.
6. **Switch date.** Proposal: switch after step 1.5 and a week of phone testing, ideally
   before the Black Friday traffic (late November).

## What the owner does at each step

- After each step: open `market.poke6s.com/m-next` on the phone, in portrait and landscape,
  and post anything wrong in Discord `#bug-reports` (or tell Claude directly).
- Answer the decisions above (one line each is enough).
- Before the switch: a last pass on the phone, then say "switch the mobile site".
