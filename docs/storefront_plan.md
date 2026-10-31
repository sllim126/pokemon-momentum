# New storefront for poke6s.com: plan

Last reviewed: 2026-10-31
**Status: Phase 1 under way (local only), started 2026-10-31.** Cloudflare and all other online
changes are paused by the owner (2026-10-31): build and run locally, Stripe test mode only.
Developer guide for the code: `storefront/README.md`. See "Progress log" at the end.

The owner decided on 2026-10-31 to replace the Squarespace store with our own storefront on
`poke6s.com`, hosted on the owner's servers. This file is the whole plan:
- what to build, and how it fits together,
- what it costs,
- the order of work,
- the decisions still open.

It's written so Codex can start the build, and so Claude (or anyone) can pick it up wherever
the last agent stopped. Work items are in `docs/todo.txt` under "Own storefront on poke6s.com".
What the current Squarespace site has and does: `docs/storefront_current_site.md`.

## Goals

- **Lean, but not invisible.** No monthly platform fee. Fixed costs are the domain and nothing
  else, but the store must be easy to find: search engines, Google's free product listings,
  social sharing cards, and links from market.poke6s.com.
- **Full ownership.** Products, prices, photos, articles, page text, customer list and order
  history all live in files and a database we control. No piece should be hard to move to
  another host. The site is plain server-rendered HTML that any Linux machine can run.
- **What other card shops online offer:** browse and search, clear product pages with photos
  and condition, a cart, a familiar card checkout (Apple Pay and Google Pay included), order
  emails, shipping and returns policies, and articles.
- **Fits the existing tools.** Reuse this repo's intake, pricing and market data. Show our
  market price next to our price; that's something Squarespace and TCGplayer can't do.
- **Simple to run.** One person runs the business part-time. Orders, shipping, sales tax
  reports and bookkeeping exports must take minutes, not evenings.

## Decided (owner, 2026-10-31)

- **No Squarespace renewal.** The website plan ends 2026-11-14; the domain stays registered.
- **Our own storefront on poke6s.com**, hosted on the owner's server. market.poke6s.com stays
  the research site.
- **Stripe for payments.** Approved, no monthly fee. We use Stripe's hosted Checkout page, so
  card details never reach our server.
- **TCGplayer stays** for its buyer traffic (marketplace and the Pro site
  `poke6s.tcgplayerpro.com`). The Pro site can't use our domain or be embedded, which is part
  of why we're building our own.
- **Custom themed binders and accessories** will be sold on our own site, since TCGplayer can't
  list custom products.

## How it fits together

```
                        Internet
                           |
                 Cloudflare (free plan)
        DNS · Tunnel · page cache · Web Analytics
              |                         |
        poke6s.com                market.poke6s.com
              |                         |
  ┌─ owner's server, Docker ───────────────────────────────────┐
  │  storefront container           pokemon-momentum container   │
  │  (FastAPI + Jinja2, SQLite)     (existing market site)       │
  │        |   reads market prices from data/storefront/         │
  │        |   market_reference.json (written by the market      │
  │        |   app's jobs)                                       │
  │  store admin (same container, separate port, LAN only)       │
  └──────────────────────────────────────────────────────────────┘
              |  Checkout Session          ^ webhooks / polling
              v                            |
          Stripe hosted Checkout ──────────┘
```

- **A separate `storefront` service in this repo's `docker-compose.yml`**, with code under
  `storefront/`. Keeping it out of the market app means a crash or a heavy job there can't take
  the shop down, and the shop can later move to another machine on its own.
- **Server-rendered pages** (FastAPI + Jinja2 templates; small vanilla JavaScript for the cart).
  That keeps it fast, readable by search engines, and free of a build toolchain.
- **SQLite** (WAL mode) for products, stock and orders, at `data/storefront/store.db`. It's one
  file that's easy to back up and handles transactions properly. The market site's DuckDB stays
  separate.
- **Market prices** come from a small JSON file the market app writes after its daily update
  (`data/storefront/market_reference.json`: product ID → market price, 30-day trend, page link).
  The store never queries the market database directly.
- **Cloudflare Tunnel** publishes the site without opening ports and keeps working when the home
  IP changes. Cloudflare caching keeps product pages loading during short server outages. This
  needs the domain's DNS moved to Cloudflare (free; the domain can stay registered at
  Squarespace). The Microsoft 365 mail records must be copied exactly. See Phase 0.
- **The admin pages run on a separate port, reachable only on the home network.** They're never
  published through the tunnel. Remote access later would go through Cloudflare Access, which is
  free for small teams.
- **Later option, if home uptime becomes a problem:** publish the catalog as static files to
  Cloudflare Pages (free) and run the checkout step as a Cloudflare Function. Orders then wait at
  Stripe until the server comes back. Not part of the first build; the design shouldn't rule it
  out.

## Services and costs

Prices as published on 2026-10-31; check current pricing before relying on them.

| Service | Used for | Cost |
|---|---|---|
| Owner's server, Docker | Hosting the store, admin and jobs | $0 extra (already running) |
| Domain `poke6s.com` (Squarespace Domains) | The address | about $20/yr + tax. Optional later: transfer to Cloudflare Registrar at cost (about $10/yr) |
| Cloudflare free plan | DNS, Tunnel, caching, Web Analytics, Access | $0 |
| Stripe | Card, Apple Pay and Google Pay checkout; receipts; refunds | 2.9% + 30¢ per US card payment; +1.5% international cards; +1% currency conversion; $15 per dispute. No monthly fee |
| Stripe Tax (optional, see "Sales tax") | Working out sales tax from the address | 0.5% per transaction where it calculates tax |
| Microsoft 365 (existing) | Order and shipping emails from admin@poke6s.com | $0 extra |
| Google Search Console, Merchant Center | Search indexing; free product listings in Google Shopping | $0 |
| Pirate Ship (existing) | Shipping labels | $0 (postage only) |
| Discord webhook (existing) | New-order and dispute alerts | $0 |
| Later, optional: small rented server (Hetzner about $5/mo) | If home hosting proves unreliable | Only if needed |

**No other paid services** without the owner's OK (`AGENTS.md`).

## What the store does

### First release (Phases 1–3)

**For shoppers**
- Home page: featured products, new arrivals, links into categories, short About blurb.
- Catalog: browse by category (sealed, singles, accessories), language (English / Japanese),
  set, and type (booster box, bundle, pack, ETB, tin, collection box, single). Sort by newest
  and by price. Keyword search over title, set, card number and Pokémon name.
- Product page:
  - photos (zoom on click), title, set and number, language, condition (singles),
    price, stock ("Only 2 left"), shipping class;
  - **our market price and 30-day trend** with a link to the market.poke6s.com page, and
    the PriceCharting link-back where its data is shown (`AGENTS.md`);
  - "Also on TCGplayer" link (optional, per product).
- Cart kept in the browser (localStorage). Quantity limits per product, matching stock.
- **Checkout on Stripe's hosted page:**
  - cards, Apple Pay, Google Pay and Link;
  - shipping address collection (US only at launch);
  - shipping options;
  - promotion codes;
  - Stripe's emailed receipt.
- Order confirmation page, and an **order status page** (order number + email) showing
  paid / shipped / tracking.
- Pages:
  - About, FAQ, Shipping, Returns & cancellations, Terms, Privacy;
  - Contact: email link to admin@poke6s.com, no form (spam, and the project's no-form
    rule for bugs).
- Mobile-first layout that works from phones to wide desktop screens.

**For the owner (admin, local network only)**
- **Products:**
  - add and edit products, upload photos, set price, stock and visibility;
  - bulk import from CSV, reusing the existing intake scripts for singles (SKU = TCGplayer
    product ID) and sealed (mapping in `data/squarespace_tcgplayer_mapping.csv`, to be
    renamed);
  - suggested prices from the existing store price rules
    (`scripts/build_store_price_targets.py`, `data/store_price_rules.csv`,
    `scripts/common/selling_costs.py`).
- **Orders:** list and detail; mark shipped with carrier and tracking (sends the shipping email);
  partial or full refund (through Stripe); notes.
- **Shipping:** a **Pirate Ship CSV export** of unshipped orders, and packing slips (printable
  HTML).
- **Stock:**
  - every change is logged with a reason (sale, restock, adjustment, sold on TCGplayer);
  - a **TCGplayer reconciliation CSV**: what sold here since the last export, so TCGplayer
    quantities can be lowered (TCGplayer gives no API access to new developers).
- **Sales tax report:** Utah taxable sales and tax collected, by period and by rate area, ready
  for the TAP return.
- **Bookkeeping export:** monthly CSV of orders, refunds, Stripe fees and payouts in the column
  layout the owner's private ledger uses (agree the columns with the owner before building).
- **Alerts:** a Discord message for each new order, dispute or failed payment webhook.

**Behind the scenes**
- SEO:
  - server-rendered HTML, clean addresses, `sitemap.xml`, `robots.txt`, canonical tags;
  - JSON-LD structured data (Product with Offer price and availability, BreadcrumbList,
    Organization);
  - Open Graph and Twitter card tags, so links shared on social media show the photo, title
    and price.
- **301 redirects** from every old Squarespace address (`/shop/p/<slug>`, category pages,
  `/faq-1`, `/new-page`, …) to the new ones. Page and category addresses are in a committed
  `storefront/redirects.csv`; product addresses are recorded by the importer in the
  `product_redirects` table, so they follow a product's current slug.
- **Google Merchant Center feed** (`/feeds/google.xml`) for free Shopping listings.
- **Images:** originals kept, plus resized WebP versions for lists and pages. Served with long
  cache headers.
- **Cloudflare Web Analytics** (no cookies, so no cookie banner needed).
- **Accessibility:** proper labels, keyboard use, colour contrast, alt text on photos.

### Later (Phases 4–5)

- **Articles / blog:** Markdown files with front matter under `storefront/content/articles/`,
  rendered to pages with their own social cards. Version history comes from git, so the
  owner fully owns the writing.
- **Newsletter:** sign-up with a confirmation email, stored in our database, with unsubscribe
  links. Sending starts as collect-only; choose a sending method when the list is big enough
  (Microsoft 365 for a small list, or a free tier elsewhere with the owner's OK).
- **Custom binders and accessories:** products with variants (design, size), made-to-order or
  pre-order flags, longer shipping times shown on the page.
- **"Buy from Poke6s" on market.poke6s.com** switched from the Squarespace export to the new
  store's stock. Then retire the Squarespace scripts.
- **Nice to have:** back-in-stock email alerts, wishlists (browser-stored), bundles,
  local-pickup option (needs a sales-tax answer), customer reviews.

### Not planned

- Customer accounts and passwords: guest checkout plus the order status page is enough.
- Storing card details. Stripe holds all payment data; our server sees only order totals and
  the shipping address.
- An on-site contact or bug form (bugs go to Discord `#bug-reports`).

## Addresses (URL scheme)

| Address | Page |
|---|---|
| `/` | Home |
| `/shop` | All products (filters as query parameters: `?lang=ja&type=booster-box&set=sv11`) |
| `/c/<category>` | Category, e.g. `/c/booster-boxes`, `/c/singles`, `/c/accessories` |
| `/sets/<set-slug>` | Everything we stock from one set |
| `/p/<slug>` | Product page |
| `/cart`, `/checkout` (POST), `/order/thanks`, `/order/status` | Buying |
| `/articles`, `/articles/<slug>` | Articles (later) |
| `/about`, `/faq`, `/shipping`, `/returns`, `/terms`, `/privacy`, `/contact` | Pages, from Markdown in `storefront/content/pages/` |
| `/sitemap.xml`, `/robots.txt`, `/feeds/google.xml` | For search engines |
| `/stripe/webhook` | Stripe events (signature checked) |

## Data model (SQLite)

- `products`:
  - id, sku (unique), slug (unique), title, kind (`sealed`, `single`, `accessory`, `bulk`);
  - language, set_code, set_name, card_number, rarity, finish, condition;
  - description_md, price_cents, compare_at_cents, stock, max_per_order, visible;
  - weight_oz, length/width/height_in, ship_class;
  - tcgplayer_product_ids (text, `|`-separated), created_at, updated_at.
- `product_images`: product_id, file name, alt text, sort order.
- `categories`, `product_categories`, `tags`, `product_tags`: carry over the rules in
  `docs/store_category_tree_proposal.md` and `docs/store_tag_policy.md`.
- `orders`:
  - id, number (`P6-2026-0001` style), status (`pending`, `paid`, `shipped`, `refunded`,
    `partially_refunded`, `cancelled`, `disputed`);
  - email, name, shipping address (JSON);
  - subtotal, shipping, tax, discount, total (cents);
  - Stripe session ID and payment intent ID;
  - created_at, paid_at, shipped_at, carrier, tracking_number, notes.
- `order_items`: order_id, product_id, sku, title and unit price **as sold**, quantity.
- `stock_movements`: product_id, change (+/−), reason (`sale`, `reservation`, `release`,
  `restock`, `adjustment`, `tcgplayer_sale`), reference, at. Stock on the product row always
  equals the sum of its movements; a test checks this.
- `newsletter_subscribers` (later): email, source, consent_at, confirmed_at,
  unsubscribed_at.
- `webhook_events`: Stripe event ID (unique) and when it was handled, so a repeated delivery
  does nothing twice.

Prices are stored in cents. All times are UTC in the database, shown in Mountain Time.

## Checkout and orders

1. **The shopper clicks Checkout.** The browser sends the cart (SKUs and quantities only) to
   `POST /checkout`.
2. **The server rebuilds the order from the database.** It never trusts prices sent by the
   browser. It checks stock, creates a `pending` order and **reserves** the stock for 30 minutes
   (a `reservation` stock movement).
3. **The server creates a Stripe Checkout Session:**
   - `mode=payment`, line items with the database prices;
   - shipping address collection (US);
   - shipping options for the cart's shipping class;
   - `allow_promotion_codes`;
   - order number in `metadata`;
   - `expires_at` = 30 minutes;
   - for orders over a set amount (setting, about $150 to start), the session asks the card's
     bank for **3-D Secure** (`payment_method_options.card.request_three_d_secure=any`).
     This moves most fraud chargebacks to the bank.
4. **The shopper pays on Stripe** and returns to `/order/thanks?session_id=…`.
5. **`checkout.session.completed` (paid):**
   - the order becomes `paid`, and the reservation turns into a `sale`;
   - Discord alert; Stripe sends the receipt.
   
   **`checkout.session.expired`:** the reservation is released. **`charge.dispute.created`:** the
   order is marked `disputed`, with a Discord alert. **`charge.refunded`:** the refund is recorded.
6. **Missed webhooks:** if the server was down, Stripe retries webhooks for up to 3 days. A
   reconciliation job also runs at start-up and every 15 minutes. It lists recent Checkout
   Sessions from the Stripe API and applies any it missed, so no paid order is lost.
7. **Shipping:**
   - the owner buys labels in Pirate Ship from the CSV export, then enters tracking in the
     admin;
   - the shopper gets a shipping email from admin@poke6s.com via Microsoft 365 SMTP;
   - SMTP AUTH may need switching on for that mailbox.
8. **Development uses Stripe test mode** and the Stripe CLI for webhooks. **Switching to live
   keys is the owner's call** (it reaches real customers and money).

**Overselling with TCGplayer:** if a product is listed both here and on TCGplayer, the same item
can sell twice. At launch, follow the stock split the owner chooses (decision D1). The
reconciliation CSV and a "sold on TCGplayer" stock adjustment cover the rest. A rare double sale
is refunded.

## Sales tax

- **Out-of-state orders:** no Utah tax. Other states don't require us to collect until sales into
  them are far above our size; recheck yearly.
- **Utah orders: we must collect Utah sales tax.** Utah rates vary by city and county. Whether the
  buyer's local rate applies to shipped orders is being confirmed with the Utah State Tax
  Commission on 2026-11-02, along with whether the price may include tax.
- **Two ways to do it (decision D2, after that call):**
  - **A. Stripe Tax:** Stripe works out the tax from the shipping address at checkout. Simplest
    and always correct, but costs 0.5% per transaction where it calculates.
  - **B. Our own rate table:**
    - load Utah's published rate file into the database;
    - ask for state and ZIP in the cart before checkout, and add a "Utah sales tax" line item;
    - after payment, check the Stripe shipping address against what was entered, and flag a
      mismatch for the owner.
    - No fee, more code.
- **Either way, the admin sales-tax report** produces the figures for each TAP filing period.

## Shipping

- US only at launch. International later, with the owner's OK; it adds customs forms.
- **Shipping classes** on each product:
  - single card: plain envelope with tracking or bubble mailer;
  - small sealed: packs, small boxes;
  - large sealed: booster boxes, ETBs, collection boxes;
  - accessory.
  
  The cart uses the largest class present. Rates and the free-shipping threshold are settings
  (decision D3).
- **Signature confirmation** above a set order value (setting; about $250 to start), to protect
  against "never arrived" disputes.
- Policies on `/shipping` and `/returns` state handling time, carriers, damage claims, returns
  window and condition rules.

## Content and ownership

- Page text and articles are Markdown files in `storefront/content/`, committed to git. The repo
  is public, so nothing private goes there.
- **Product photos, orders, customer emails and the database live under `data/storefront/`.**
  They're never committed (personal data and business data).
- **Backups:**
  - a nightly copy of `store.db`, using SQLite's online backup, plus the images folder to a
    second disk;
  - keep 30 days;
  - document the restore steps in `docs/operator_runbook.txt`.
- Look and feel: decision D4. Default: the market site's colors and type
  (`scripts/dashboards/desktop_theme.css`), so the two sites feel like one brand.

## Security and privacy

- **Secrets in `.env` only, never in code, logs or commits:**
  - `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`;
  - `STOREFRONT_BASE_URL`;
  - `STOREFRONT_SMTP_USER`, `STOREFRONT_SMTP_PASSWORD`;
  - `STOREFRONT_DISCORD_WEBHOOK` (or reuse the existing alerts webhook).
- **Webhooks:** every request is checked against the Stripe signature; event IDs are stored so
  repeats are ignored.
- **The public app can't change products or orders.** Only the admin app, on the LAN port, can.
- **Form protection:** CSRF tokens on admin forms; rate limits on `/checkout` and
  `/order/status`.
- **PCI:** with Stripe's hosted Checkout we're in the lightest category (SAQ A). Our pages never
  load card fields.
- **Privacy policy:** says what we keep (name, email, shipping address, order history), why, and
  for how long; and that Stripe processes payments.

## Moving off Squarespace

1. **Before 2026-11-14 (owner):** the export steps in `docs/storefront_current_site.md`.
2. **From 2026-11-14 until launch, as a stopgap (decision D5):** forward `poke6s.com` to
   `poke6s.tcgplayerpro.com` (or to market.poke6s.com) in the Squarespace Domains panel, so
   the address never shows an error.
3. **Import the product export** into the new database:
   - keep SKUs;
   - turn Squarespace slugs into the redirect list;
   - copy the images;
   - set visibility from current stock.
4. **Launch:** point `poke6s.com` at the tunnel; check redirects, sitemap, a live test order
   (refunded), emails and Discord alerts. Submit the sitemap to Google Search Console and the
   feed to Merchant Center.
5. **After launch:**
   - switch the market site's buy button to the new store;
   - remove the Squarespace price sync from cron (`deploy/crontab.txt`, runbook);
   - retire the Squarespace-only scripts, or keep the parts reused for intake;
   - update `docs/project_status.md`, `docs/operator_runbook.txt`, `docs/data_flow.txt` and
     `docs/api_routes.md`.

## Order of work

Each phase ends with something that works and is tested. Codex starts with Phase 1 after the
owner has reviewed this plan. **Phase 0 needs the owner** (accounts and DNS), so it runs alongside.

**Phase 0: accounts, DNS and stopgap (owner + Claude)**
- Squarespace exports (before 2026-11-14) and auto-renew off.
- Domain forwarding stopgap (D5).
- Cloudflare account. Move DNS:
  - copy every record, especially the Microsoft 365 mail records;
  - check mail still flows before and after;
  - this also fixes the existing home-IP item in `docs/todo.txt`.
- Cloudflare Tunnel for market.poke6s.com first (proves the setup), then poke6s.com at launch.
- Stripe account in test mode. The owner keeps the live keys until launch.
- *Done when:* mail works, market.poke6s.com runs through the tunnel, and poke6s.com forwards
  somewhere useful.

**Phase 1: catalog site, local (Codex)**
- `storefront/` app, Dockerfile and compose service; health endpoint; settings from `.env`.
- SQLite schema with migrations (plain SQL files, applied in order at start-up).
- Import from the Squarespace export and the intake CSVs; images resized to WebP.
- Pages: home, shop with filters, search, category, set, product, Markdown pages, 404.
- Market reference JSON written by the market app, shown on product pages.
- SEO basics: sitemap, robots, canonical, JSON-LD, Open Graph; redirects CSV.
- Tests: import, slugs, filters, redirects, sitemap, structured data.
- *Done when:* the full catalog browses correctly on a phone and a 1920×1080 screen with no
  console errors, and it's reachable on the LAN. No checkout yet; products show
  "Coming soon / Buy on TCGplayer".

**Phase 2: cart and Stripe checkout, test mode (Codex)**
- Cart, `/checkout`, reservations, the Stripe session, webhook handler, reconciliation job,
  thanks page, order status page.
- Shipping classes and rates (settings), 3-D Secure rule, promotion codes.
- Sales tax per decision D2.
- Discord alerts; shipping email through Microsoft 365 SMTP (test mailbox or test mode).
- Tests: price tampering rejected, stock never below zero, reservation expiry, repeated
  webhooks, missed-webhook reconciliation, tax lines, shipping-class choice.
- *Done when:* a Stripe test order goes from cart to paid to shipped email, and stock and the
  order record match.

**Phase 3: admin and operations (Codex)**
- LAN-only admin: products and photos, stock adjustments, orders, ship with tracking, refunds.
- Exports: Pirate Ship CSV, packing slips, TCGplayer reconciliation, sales-tax report,
  bookkeeping CSV.
- Nightly backup and restore steps; runbook section; cron or systemd entries in `deploy/`.
- *Done when:* the owner can run a day's orders from the admin alone, and a restore from backup
  has been tested.

**Launch (owner + Claude):** content review (policies, About), live Stripe keys, DNS to the tunnel,
live test order, Search Console and Merchant Center, then the "After launch" steps above.

**Phase 4: content and reach (Codex or Claude)**
- Articles, social cards per article, Merchant Center feed checks. Newsletter only if the
  owner asks for it then (D9).
- Market site buy button switched to the new store.

**Phase 5: custom binders and accessories**
- Product variants, pre-order and made-to-order flags, design gallery.

## Owner decisions

Reviewed with the owner on 2026-10-31. D2 waits for the 2026-11-02 Utah call.

| # | Decision | Answer |
|---|---|---|
| D1 | Which stock is listed here and which on TCGplayer | **Decided:** our site lists sealed product and accessories; singles stay on TCGplayer. Revisit after launch |
| D2 | Sales tax method | **Open:** decide after the 2026-11-02 call. Stripe Tax for a fast launch, our own Utah table if the fee matters. Build the tax step behind one interface so either can be plugged in |
| D3 | Shipping | **Decided:** flat rate by shipping class, free shipping over a threshold. The owner sets the amounts from Pirate Ship costs; until then use settings with placeholder values (small sealed $5, large sealed $9, free over $100) |
| D4 | Look and feel | **Decided:** match market.poke6s.com (`scripts/dashboards/desktop_theme.css`) |
| D5 | poke6s.com from 2026-11-14 until launch | **Decided:** forward to `poke6s.tcgplayerpro.com` in the Squarespace Domains panel |
| D6 | When to move DNS to Cloudflare | **Decided:** soon (within a week or two), with Claude walking the owner through it, mail records double-checked |
| D7 | Admin access | **Decided:** home network only |
| D8 | Returns | **Decided:** sealed product accepted back **unopened in original wrap within 14 days**. The refund is issued **after the owner inspects the returned item**. Buyer pays return shipping unless we made an error or it arrived damaged. The policy page must state the inspection step |
| D9 | Newsletter | **Decided:** not in the first release; Phase 4 at the earliest |

## Rules for whoever builds this

- Read `AGENTS.md` first. Its rules apply here too:
  - work on a branch;
  - comment the non-obvious parts;
  - update docs in the same commit;
  - no new paid services without the owner's OK;
  - never commit `.env` or anything under `data/`.
- **Never touch anything live without the owner's go-ahead:** live Stripe keys, DNS, the
  Squarespace store, cron on the server.
- Tests: `docker-compose exec -T pokemon-momentum python -m pytest -q -p no:cacheprovider tests`
  for the market app. Give the storefront its own `storefront/tests/` with the same pytest
  setup, and add a `make test-storefront` target.
- **End every session the same way:**
  - commit, even partway, on a `storefront/...` branch;
  - update the todo items;
  - add a line to the Progress log below: date, agent, what was done, where it stopped, what's
    next.

  The next agent (Codex or Claude) starts from that line.

## Progress log

| Date | Who | What happened | Next |
|---|---|---|---|
| 2026-10-31 | Claude | Plan drafted with the owner; current site documented in `docs/storefront_current_site.md`; todo items added | Owner reviews this plan with Claude, then Codex starts Phase 1 |
| 2026-10-31 | Claude | Owner review: D1 and D3–D9 decided (see "Owner decisions"); D2 waits for the Utah call | Codex: Phase 1. Owner + Claude: Phase 0 (Squarespace exports, D5 forwarding, Cloudflare DNS) |
| 2026-10-31 | Claude | Phase 1 built and running locally on branch `storefront/phase1-catalog` (owner paused Cloudflare and other online changes): `storefront/` service on port 8010 (LAN), SQLite schema + migrations, importer (94 sealed/accessory products from the 2026-10-09 export, 30 visible, 151 photos copied from Squarespace's CDN as WebP), shop/search/category/set/product pages, market price + 30-day trend from `data/storefront/market_reference.json` (writer `scripts/build_storefront_market_reference.py`), sitemap, robots (blocked until launch), JSON-LD, Open Graph, 301 redirects, draft policy pages. 46 storefront tests + 3 writer tests pass; checked at 1920×1080, 1366×768 and phone with no console errors. **Stopped at:** Phase 1 done except the items in the next column | Owner: check the shop on a phone on the home network; review the draft pages in `storefront/content/pages/` (handling time in Shipping, tax-record years in Privacy). Then Phase 2 (cart + Stripe **test mode**). Not done: market reference in the daily job (cron, owner), re-import from the final pre-11-14 export, Google Merchant feed (Phase 4) |
| 2026-10-31 | Claude | Saved the old site before the plan ends (`storefront/tools/archive_squarespace_site.py`): page text, 10 Resources blog posts (missed in the first site survey) and 256 images, in the owner's records outside the repo. `docs/storefront_current_site.md` corrected; redirects added for `/new-page-1` and `/pre-order-interest` | Owner: where the blog posts go at launch (todo); compare draft policies with the old ones (6% cancellation fee). Re-run the script on the last day |
