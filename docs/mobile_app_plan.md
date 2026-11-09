# New mobile app: plan

Last reviewed: 2026-11-09

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
  Pokémon, Market Stats (indexes, set values, and since 2026-11-06 market mood, track record
  and releases) and How it works.

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
  | under 700 px (phone, portrait) | One pane. A list, or a card on its own screen. Menu button, plus a tab bar at the bottom (Check / Discover / My lists / Menu, since step 1.3). |
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
  "Overpriced".
- **To resell:** what a sale at market leaves after TCGplayer fees and shipping
  (`/hold_check`'s `net_at_market`, from `scripts/common/selling_costs.py`) minus the asking
  price. 20%+ profit is a "Good flip", 0-20% "Thin profit", below that "Loss if resold now".
- **Counter-offers** (owner, 2026-10-29, replacing "Pay up to"): a table of 95, 90, 85, 80, 75
  and 70% of the asking price, each with its dollar amount and how it rates by the rules above
  (% vs market to keep, profit after fees to resell), colored good / fair / bad. Example: market
  $87.53, seller asks $90: 90% is $81.00, 7% under market.
- **Warnings:** a card 40%+ above its 30-day average has "just spiked" (the counter-offers are
  then rated against the average); one down 10%+ in 30 days "is falling". Either turns a buy into
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
| Home feed of cards with sparklines | Discover lists; the Discover tab starts on Consistent Movers like desktop (2026-11-09), then remembers the last list opened (1.1) |
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

## Decisions for the owner (answered 2026-10-24)

1. **Public feedback form: removed.** Visitors send feedback through the Discord link in the
   menu. The signed-in owner alone gets a floating review-note tab on `/m-next` (agreed
   2026-10-25): it saves a note, automatic page/card/list context and optional files to the
   internal `/bug-reports` queue, then collapses so mobile review can continue uninterrupted.
   Text drafts survive a close or reload; offline submission retry remains a possible follow-up.
2. **Alerts & Targets: left out for now.** The card view gets the desktop Targets tab (your own
   buy and sell prices per card) in step 1.2; alerts may come back in stage 4.
3. **Time to Buy: dropped** on phones (the desktop app had already dropped it).
4. **Set Explorer: no separate page** on phones; the Sets view (step 1.4) covers it.
5. **iPads: still deciding.** Today iPads in their default "desktop website" mode get the
   desktop app (which fits portrait since 2026-10-22), and iPads that report as mobile get the
   phone page. To be settled in stage 2.
6. **Switched early: 2026-10-31** (owner: "it has been working fine; we can still make
   changes"). Phones get the app at `/`, `/mobile` and `/dashboard`; `/m-next` stays as an
   address; the old page is at `/mobile-classic` for a few weeks. Steps 1.2 (Graded, targets, stats,
   Save, Our store) and 1.3 (My lists, sign-in, tab bar) and 1.4 (Sets → one set, Pokémon, Market
   Stats → one index, Set values) and 1.5 (Sealed Deals, Budget Builder, How it works) went
   live the same day. Old
   links to Browse Set, Browse Pokémon and Set Strength (`?tab=group_products&group_id=…`
   etc.) open the app's own views since 1.4, so phones no longer get `dashboard.html`.

## What the owner does at each step

- After each step: open `market.poke6s.com/m-next` while signed in as the owner ("Sign in"
  at the top right; it returns to `/m-next`), in portrait and
  landscape, and use the floating Note tab for anything wrong. Public testers use Discord
  `#bug-reports`.
- Decide decision 5 (iPads) during stage 2.
- The switch is done (2026-10-31), and so is the clean-up (1.6, the same day):
  `mobile_dashboard.html`, `dashboard.html` and `mobile_rebuild_mockup.html` are deleted,
  `/mobile-classic` and `/mobile-rebuild` forward to `/mobile`. The last copy is in git tag
  `backup/before-mobile-cleanup-2026-10-31`.
