# Poke6s storefront (poke6s.com)

Our own shop, replacing Squarespace. The plan, decisions and progress log are in
`docs/storefront_plan.md`; this file is the developer guide for the code in `storefront/`.

**Status (2026-11-05): Phase 2 built, local only, Stripe test mode.** Port 8010 listens on the
server itself only (since 2026-11-12; the public site arrives through the Cloudflare Tunnel), so
from another computer use https://poke6s.com or an SSH tunnel (`ssh -L 8010:localhost:8010 <server>`). Cart and checkout switch on when
`STOREFRONT_CHECKOUT=1` and a Stripe **test** key are in `.env` (see "Checkout" below);
otherwise product pages link to the TCGplayer store. The public launch (2026-11-11) is
catalog-only: `STOREFRONT_CHECKOUT` is unset, which means off.
While checkout is off, product pages show no stock counts (only "Sold out") and the structured
data leaves out `availability` instead of claiming `InStock` (owner, 2026-11-11).
Also while it is off (owner, 2026-11-11): a "we're rebuilding" note under the header, and product
pages offer "Buy on TCGplayer" (the product's TCGplayer page through the affiliate link,
`STOREFRONT_TCGPLAYER_AFFILIATE`; a product's own `tcgplayer_url` wins) plus an "email me when
the store opens" form. Sign-ups go to the `notify_signups` table (`app/notify_list.py`); list
them with
`sqlite3 data/storefront/store.db "SELECT email, source, created_at FROM notify_signups"`.
Nothing is public: robots are blocked and every page says `noindex` until
`STOREFRONT_ALLOW_INDEXING=1` at launch.

## Run it

```sh
deploy/update_storefront.sh                  # run the store on :8010 from the latest main
make test-storefront                         # synthetic data only, in the "test" image
```

**Package versions are pinned** (since 2026-11-12): `requirements.txt` (what the store uses),
`requirements-dev.txt` (pytest and httpx, test image only) and `constraints.txt` (every package
underneath, exact). The base image is pinned to `python:3.12.16-slim-trixie`. The served image
(Dockerfile stage `runtime`) has no test tools; `make test-storefront` builds stage `test`. To
upgrade anything: change the version, `make test-storefront`, refresh `constraints.txt` from
`docker run --rm poke6s-storefront-test pip freeze`, commit, deploy. Bump the base image about
monthly for security fixes.

The store on :8010 runs from its own clone, `~/poke6s-store`, which only holds `main`, so
coding sessions switching branches in the repo folder don't change it. The script pulls `main`
into the clone, rebuilds the image if needed, restarts and checks `/healthz`. To try a branch,
run a second container from a copy of `data/storefront` on another port instead.

The container (`poke6s-storefront`, port 8010) sees only:
- `storefront/` (the code, mounted, so no rebuild for code changes);
- `data/` **read-only** (Squarespace exports, mapping and intake CSVs);
- `data/storefront/` read-write: `store.db`, `images/`, `market_reference.json`.

`data/storefront/` is git-ignored: it holds business data and, from Phase 2, customer data.

CSS and JavaScript changes need only a browser reload. Python, templates and the Markdown
pages are read at start-up, so restart the container after changing them.
The container runs as the host user (uid/gid 1000, override with `STOREFRONT_UID` and
`STOREFRONT_GID`), so the database, photos and article files it writes stay editable without
sudo. Also restart it after switching git branches: a checkout that removes and recreates
`storefront/` leaves the running container looking at the old, empty folder.

## Load the catalog

```sh
# 1. Market prices for product pages (market container; reads the daily snapshots):
docker-compose exec -T pokemon-momentum python scripts/build_storefront_market_reference.py
# 2. Products from the newest Squarespace export (add --dry-run first to preview):
docker-compose exec -T storefront python -m storefront.app.importer --fetch-images
```

The importer (`app/importer.py`, docstring has the full rules):
- one product per export row; Squarespace variants (desk-mat sizes) become separate products;
- singles are skipped (decision D1: singles stay on TCGplayer); `--include-singles` imports
  them hidden;
- visible only if visible on Squarespace, priced above 0, and in stock or untracked
  ("Unlimited" stock becomes `track_stock = 0`);
- clean slugs from titles; each old `/shop/p/<slug>` goes into `product_redirects`;
- stock changes are written as `import` stock movements;
- `--fetch-images` downloads the photos from Squarespace's public CDN (0.25 s apart) and
  makes 480 px and 1200 px WebP copies. Without it, no network request is made.

Re-running updates existing SKUs but keeps their slugs. Once products are edited in the admin
(Phase 3), use `--only-new`.

The market reference is rebuilt daily at 03:00 UTC by cron (after the 02:00 market update).

## Prices

`tools/reprice.py` sets prices at or near today's market and never below break-even (owner,
2026-11-11): each product's rule in `data/store_price_rules.csv` (usually 5% under market,
ending in .99) applied to the market price the product page shows, raised to break-even when
that's higher. Break-even is the unit cost plus Stripe's fees (2.9% + 30¢, plus 0.5% for
Stripe Tax), and the label and packing too when the price alone reaches free shipping ($150).
With `--hide-above-market`, products whose break-even is above market are hidden instead.
Costs are private: they come from a CSV the owner keeps outside this repository (`--costs`),
and the output never shows them. Dry run unless `--apply`; standard library only, run on the
host:

```
python3 storefront/tools/reprice.py --costs <store_sku_costs.csv> --hide-above-market
python3 storefront/tools/reprice.py --costs <store_sku_costs.csv> --hide-above-market --apply
python3 storefront/tools/reprice.py --costs <store_sku_costs.csv> --show ENG-PO-BOX --apply
```

Products without a rule, a market price or a cost are left alone and listed. Re-running the
Squarespace importer resets prices and visibility, so run this again after any import.

**Floors.** A product's floor is the higher of break-even and its `min_price` in the rules file
(the owner's own minimum; six Mega Collection products have $29.99). No run, by hand, on the
Pricing page or at night, sets a price under it.

**Nightly** (owner, 2026-11-25): `scripts/pipeline/run_store_reprice_host.sh` runs it with
`--apply --max-market-age-days 2 --max-change-pct 30` at 03:30 UTC, after the 03:00 market
prices and the 03:15 backup. Stale market prices change nothing (exit 2, orange in Discord); the
prices it changed are posted to Discord (`store-reprice`) and show in each product's history as
"repricing". A change over 30% in one night is held back and listed in Discord as "held" for the
owner to check on the Pricing page (a bad market reading can't move a price far). The costs file
is found through `STOREFRONT_COSTS_DIR` in `.env`, the folder the Pricing page reads.

**Priced by hand** (`products.price_manual`, migration 011): the admin's price form has "Keep this
price (nightly repricing leaves it alone)", ticked by default, so a price typed there isn't put
back the next night. Untick it and save to hand the product back to its rule. The switch is in
the product's history; the Pricing page lists such products under "Left alone".

### Pricing page

The admin's **Pricing** page (http://192.168.1.240:8011/pricing, Phase 3 step 6) runs the same
rules with a preview: it lists the prices that would change (now, market, new, vs market,
"raised to break-even"), and **Apply the ticked prices** writes only the ticked lines, each
logged in its product's history as "repricing". A line whose price changed after the page
loaded is skipped and reported. It never hides products (`--hide-above-market` stays a
command-line choice) and never shows costs. Also listed: products already at their price,
products left alone (no rule, manual, no market price, no cost) and hidden in-stock products
that could sell at or under market without a loss.

Set-up: the admin reads the costs file through a read-only mount. Put the folder holding
`store_sku_costs.csv` in `.env` as `STOREFRONT_COSTS_DIR=/path/to/folder` (the folder stays
outside this public repo), then `bash deploy/update_storefront.sh`. Without it the page says the
costs file is missing. Rules come from `data/store_price_rules.csv` and market prices from
`data/storefront/market_reference.json` (refreshed at 03:00). The page and the script share
`reprice.plan()` and `reprice.apply()`, so they always agree.

### Reports page (for the books)

The admin's **Reports** page (http://192.168.1.240:8011/reports, Phase 3 step 7) shows one shop
month (Utah time) and offers two downloads. poke6s.com orders are not entered in QuickBooks one
by one (owner, 2026-11-20): QuickBooks sees only Stripe's payouts in the bank feed, and these
files carry the detail into the books.

- **Sales** (`poke6s-sales-YYYY-MM.csv`): one row per order paid that month, in the columns of
  the books' Utah sales worksheet (`date, channel, buyer, items, sale_amount, tax_charged,
  how_paid, delivery_city_state, utah_taxable, source, notes`). `sale_amount` is before tax
  (items - discount + shipping); a fully refunded order shows 0.00 and "no (refunded)".
- **Stripe money** (`poke6s-stripe-YYYY-MM.csv`): every charge and refund dated that month with
  Stripe's fee, the net and the expected payout date, plus a TOTAL line: the gross-up of the
  month's payouts into sales, tax, shipping and fees. A refund keeps the original fee (Stripe
  doesn't return it), so a refunded order still costs its fee. Fees are fetched from Stripe on
  download with the admin's key (`STOREFRONT_ADMIN_STRIPE_KEY`), which can read charges but not
  payouts, so payouts are matched by date in the bank feed.

The page also shows Utah sales tax totals for the month and year to date (from the store's
orders; file from Stripe Tax's own report, which these should match) and the month's "Sold on
TCGplayer" stock changes, to compare with TCGplayer's Sales Summary. Test orders are left out.

## Save a copy of the old Squarespace site

The Squarespace export has no page text, blog posts or images, so
`tools/archive_squarespace_site.py` fetches them from the public site (read-only, 0.3 s
apart, stops on 403/429). Run it on the host, not in the container (it needs `requests` and
`beautifulsoup4`), and point `--out` at a folder **outside the repo**:

```sh
python3 storefront/tools/archive_squarespace_site.py --out <folder> --products-csv <products export.csv>
```

It writes pages and the Resources blog posts as Markdown (posts with front matter: title,
date, old address, tags, categories, cover image), the pages as served, the original images
and `index.csv`/`images.csv`. Re-runs skip images already saved.

## Collector Hub

`/collector-hub` explains the free master-set resources (checklists, placeholder print pages,
CSVs) in three steps and a tile per resource (moved from market.poke6s.com on 2026-11-20).
The list and its wording are in `app/collector_hub.py`; its tiles open the Placeholder
Library's files (below), and `tests/test_free_tools.py` checks every tile link resolves.

## Placeholder Library

`/placeholders` lists every printable placeholder page, checklist and spreadsheet, by who it's
for (moved from market.poke6s.com on 2026-11-20). `app/placeholder_library.py` holds the list
and serves the files: `/placeholders/files/<bucket>/<path>` (the generated print and checklist
pages and their CSVs) and `/placeholders/download/<id>` (as attachments). Only the folders in
`BUCKETS`, the files in `DOWNLOADS` and the types in `MEDIA_TYPES` are reachable.

One download is made by hand, not generated: `TCG Placeholders/title_cards/` holds the owner's
Journey Together promo title cards (exported from Publisher to PDF), listed under "Title cards".
To add another, put the PDF in that folder, commit it, and add a `DOWNLOADS` id and an `Item`.

The files are not part of the store. They are generated in the main checkout's
"TCG Placeholders" folder, rebuilt every night at 04:30 UTC by the market's
`scripts/utilities/sync_placeholder_library.py` (docs/tcg_placeholders_guide.md), and mounted
read-only into the store container, so a rebuild shows on the site straight away and the page
says when it last happened. The generated pages have inline styles and scripts (saving
checklist ticks), which the store's CSP refuses; each one is served with a CSP that allows
exactly its own scripts by SHA-256 hash, plus inline styles (`page_csp`).

## Binder Art Maker

`/binder-art` is the free tool that cuts one picture into card-size pieces for a binder page
(moved from market.poke6s.com on 2026-11-20). It all runs in the visitor's browser
(`static/binder-art.js`, with jsPDF and JSZip in `static/vendor/`: the CSP allows no outside
script hosts, so update them by downloading the new pinned version there). Only this page's
CSP allows `blob:` images, for the visitor's picture. The set-logo list (`app/binder_art.py`)
reads `images/logos/<groupId>.png`, mounted read-only into the container, and names each logo
from `data/extracted/pokemon_groups.csv`; a logo added with `scripts/utilities/sync_set_logos.py`
shows up within an hour (the list's cache time).

"Pockets to fill" (2026-11-20) lets a visitor use only some pockets, say 1 and 4, or 8 and 9,
leaving the rest for real cards. The picture is fitted to the box around the kept pockets, and
the PDF and ZIP hold only those cards (file names gain e.g. `-p1-4`). Pockets are numbered row
by row on each page; at least one is always kept.

## Articles

The blog lives at `/articles` (list, with category and tag filters) and `/articles/<slug>`.
Each article is a Markdown file in `content/articles/<slug>.md`; the front matter is described
in `app/articles.py`. To write a new one, copy an existing file, change the front matter and
text, and restart the container. `draft: true` keeps it off the list, the home page and the
sitemap while it can still be previewed at its address. Pictures go in like product photos
(`images.store_original`) and are referenced as `/media/1200/<name>.webp`.

**Market boxes.** A line holding only `[[market <id> <id> ...]]` (TCGplayer product IDs, as
in `data/storefront/market_reference.json`) shows those products as a row of boxes: TCGplayer's
picture, today's market price, the 30-day change and a link to the price history on
market.poke6s.com. The prices are filled in on every page view, so they stay current as the
daily file updates; IDs missing from the file are skipped. `[[cards <id> ...]]` shows only the pictures, names
and sets, with no prices, for when a post is about the art (captioned "Card images from TCGplayer.").
Price rows have no caption, so a post using them should say once, near its first prices, that
the numbers in the text are from the day it was written and the boxes show today's prices. Rows of 3, 5 or 6 products go three to a row on wider screens; 4 fit on one row, so 4 or 6 look tidiest. Every sealed product (English and
Japanese) is in the file, and so is any single card named in an article's `[[market ...]]` line
(the nightly `scripts/build_storefront_market_reference.py` reads the articles). A row of only
single cards is shown bigger, card-shaped, for the art; cards also show their set, and
Japanese products say "Japanese". The article page's CSP
allows pictures from `tcgplayer-cdn.tcgplayer.com` for this.

**TCGplayer links.** `[Perfect Order ETB](tcgplayer:672401)` links to that product's TCGplayer
page through the shop's affiliate link (`STOREFRONT_TCGPLAYER_AFFILIATE`, the same one as the
"Buy on TCGplayer" button), marked `rel="sponsored"`. A post that uses them says once that
they're affiliate links. Products we have in stock link to our own page instead, automatically, when our price is at or below TCGplayer's market price (whichever is cheaper wins).
Box pictures are links too: to our product page when we have it in stock at or below
TCGplayer's market price (`catalog.stocked_by_tcgplayer_id`), otherwise to TCGplayer through the affiliate link. A page
with affiliate links that doesn't mention them in its text gets a one-line note under the post.

**Scheduling.** A finished article (`draft: false`) with a future `date` is scheduled: it stays
off the list, the home page and the sitemap until that date (Mountain time), then appears on
its own, with no restart, because the date is checked on every request. Its page shows a
"Scheduled" note until then, so it can still be proofread at its address. The file still has to
be in place when the container starts, so restart after adding one. Workflow for the regular
posts: Claude (or whoever) drafts each one a few days ahead with fresh data as `draft: true`;
the owner proofreads it and adds their own voice, then sets `draft: false` and the date it
should go live. The topic list is in `docs/todo.txt` ("Blog posts"). Every draft or scheduled
page has a folded "Not live yet" list of all the drafts and scheduled posts, soonest date first,
so the owner can move from one to the next; live posts don't show it. Anyone with a draft's
link can see that list too (drafts aren't secret, by the owner's choice).

The 10 posts from Squarespace were converted from the saved copy by `app/article_import.py`
(slugs from the titles; old `/resources/...` addresses 301 to the new ones, including the old
category and tag pages). The copy is outside the repo, so mount it:

```sh
docker-compose run --rm -v <copy folder>:/archive:ro storefront \
    python -m storefront.app.article_import --from /archive
```

Existing article files are kept unless `--force` is given.

## Checkout (Phase 2, Stripe test mode)

How it works: `app/orders.py` (module docstring), plan "Checkout and orders".

**Switch it on.** Put the Stripe **test** keys in `.env` (never in code or chat), then
`deploy/update_storefront.sh` (it recreates the container, so the new settings are read).
While the store is catalog-only the key line stays commented out (`# STRIPE_SECRET_KEY=...`),
so the container holds no Stripe key; uncomment it when checkout opens:

```
STOREFRONT_CHECKOUT=1                 # master switch; unset or 0 = catalog only (the default)
STRIPE_SECRET_KEY=sk_test_...         # Stripe dashboard > Developers > API keys (test mode)
STRIPE_WEBHOOK_SECRET=whsec_...       # the webhook's signing secret (required: checkout stays off without it)
```

**Limits per visitor** (`main.py`): 10 checkout attempts an hour, and at most 3 unpaid
checkouts and 12 units held at once (paid, cancelled and expired ones stop counting). The
visitor is the CF-Connecting-IP address when the request comes from the tunnel, as in `visitors.py`.

Live keys (`sk_live_...`) are refused unless `STOREFRONT_ALLOW_LIVE=1`; going live is the
owner's call. Other settings (all optional, defaults in `app/settings.py`):
`STOREFRONT_FREE_SHIPPING_OVER_CENTS` (15000), `STOREFRONT_SHIP_<CLASS>_CENTS` (small sealed
1000, large sealed 1000, oversize 2000, accessory 1000, single 1000; a product is oversize when
"Oversize box" is ticked on its admin Details panel), `STOREFRONT_TAX_MODE`
(decision D2: `stripe` = Stripe Tax adds Utah sales tax on top at checkout, once Utah is
registered under Tax -> Registrations in the Stripe dashboard; `none` = no tax, the default;
anything else keeps checkout off), `STOREFRONT_DISCORD_WEBHOOK_URL` (order
alerts; off when empty), and the order-email settings below.

**Order emails** go out through Microsoft Graph (since 2026-11-12; Microsoft turns off
password SMTP by default at the end of 2026). Settings: `STOREFRONT_GRAPH_TENANT_ID`,
`STOREFRONT_GRAPH_CLIENT_ID`, `STOREFRONT_GRAPH_CLIENT_SECRET`, from the Entra app registration
`poke6s-store-mail`. That app has no Mail.Send permission in Entra; instead Exchange Online
(RBAC for Applications: a management scope on orders@poke6s.com plus the "Application
Mail.Send" role) lets it send only as orders@poke6s.com (a free shared mailbox that shows up in
the owner's Outlook; replies land there), so a leaked secret can't send as any
other mailbox. The client secret expires after 24 months (renew it in Entra and update `.env`).
`STOREFRONT_SMTP_HOST` / `_USER` / `_PASSWORD` still work as a fallback. With neither, emails
are written to `data/storefront/outbox/*.eml`, and live checkout refuses to open.
A failed send is queued in `outbox/retry/`, Discord is told, and the 15-minute job retries it for
24 hours before moving it to `outbox/failed/` with a "gave up" alert. Check the setup with
`orders_cli test-email --to <you>`; resend an order's email with `orders_cli resend <number>`.
DKIM for poke6s.com is on (2026-11-12): two `selector*._domainkey` CNAMEs in Cloudflare (DNS only),
and the first test email to Gmail showed SPF, DKIM (poke6s.com) and DMARC all passing. The app is
refused when it tries to send as any other mailbox (checked: 403 for admin@).

Order emails (confirmation, shipped, refund and the review request) have a plain-text part and a designed HTML part,
`app/templates/email/order.html`: tables and inline styles only, because email apps ignore most
CSS. The logo is attached to the email itself (`cid:`), so it shows before the site is public.
The shipped email links USPS, UPS, FedEx and DHL numbers to the carrier's tracking page
(`notify.TRACKING_URLS`; other carriers show the number without a button). The review request
(5 days after shipping, once per order) is short: the items, a "Leave a Google review" button
and an invitation to reply if anything's wrong; `orders.send_review_requests` has the rules.

**A test order.** Add something to the cart, Checkout, and pay on Stripe's page with card
`4242 4242 4242 4242`, any future date, any CVC, any US address. Orders are numbered
`P6T-2026-0001` in test mode (`P6-...` live).

**The webhook is required** (since 2026-11-12, security review item 11): without
`STRIPE_WEBHOOK_SECRET` checkout stays off and the log says why. Set it up in the Stripe
dashboard under Developers > Webhooks > Add endpoint: URL `https://poke6s.com/stripe/webhook`,
events `checkout.session.completed`, `checkout.session.async_payment_succeeded`,
`checkout.session.expired`, `checkout.session.async_payment_failed`, `charge.refunded` and
`charge.dispute.created`; then copy its signing secret (`whsec_...`) into `.env`. Test mode and
live mode each have their own endpoint and secret. For local testing, the Stripe CLI works too
(`stripe listen --forward-to localhost:8010/stripe/webhook` prints a `whsec_...`).

Payment is confirmed three ways, whichever comes first: the webhook, the thanks page asking
Stripe directly, and the reconcile job (start-up and every 15 minutes), which checks every
pending order and then applies the last 3 days of Stripe events the webhook may have missed
(server or tunnel down). That last step is the only other way a refund or dispute on a paid
order is recorded; each event is applied once, whichever route brings it.

**Orders from the command line.** Day to day, use the admin's Orders page (see "Admin"); the
command line does the same things and stays for scripts and emergencies:

```
docker-compose exec storefront python -m storefront.app.orders_cli list
docker-compose exec storefront python -m storefront.app.orders_cli show P6T-2026-0001
docker-compose exec storefront python -m storefront.app.orders_cli ship P6T-2026-0001 --carrier USPS --tracking 9400...
docker-compose exec storefront python -m storefront.app.orders_cli reconcile
```

**Stock.** Checkout reserves stock for 30 minutes; payment turns it into a sale; an abandoned or
cancelled checkout gives it back. Every change is a `stock_movements` row. Refunds don't
restock (returns are inspected first). **Careful:** re-running the Squarespace importer resets
stock to the export's numbers, which would undo sales made here. Once real orders exist, change
stock by hand until the Phase 3 admin.

## Marketing emails

For the few notifications to the email list (we're open, new stock, back in stock; owner,
2026-11-14: "basic notifications", not promo mail). Rules and
message format: `app/mailing.py` (module docstring). In short, every message gets an honest
From and Reply-To, the reason the reader gets it, a one-click unsubscribe link (also as the
List-Unsubscribe headers Gmail and Yahoo ask for), and the business's postal address.

1. Once: put the postal address in `.env` as `STOREFRONT_POSTAL_ADDRESS="..."` (a street address,
   USPS PO box or private mailbox; it is printed in every email, so a PO box keeps a home address
   private), then `deploy/update_storefront.sh`.
2. Write the message in a text file under `data/storefront/mailings/` (not committed):

   ```
   Subject: Fresh stock this week
   Preheader: Six ETBs in, shipped from Utah.
   Button: Shop now | https://poke6s.com/shop
   Products: EN-AH-ETB, ENG-PO-BOX

   # Fresh stock is in

   Ascended Heroes ETBs are back. Blank lines separate paragraphs.
   ```

   `Products` (optional, up to 6 SKUs) adds a card per product after the text: photo, title,
   price and a link, read from the store when the email is built. A SKU that is unknown,
   hidden or sold out stops the send, so an email never offers something you can't buy.

3. Check it: `docker-compose exec storefront python -m storefront.app.orders_cli mailing /app/data/storefront/mailings/news.txt`
   (writes `news.preview.html`, its pictures and a `.txt` beside it, and says how many people
   would get it; open the `.html` in a browser).
4. `--test-to you@example.com` sends you one copy; `--send` sends it to everyone still
   subscribed (about 24 a minute, under Microsoft's limit; Discord gets a summary).

Only people who typed their address into the site's form are on the list. Never add customers
or anyone else by hand: the email would then have to say it's an advertisement, and it doesn't.
The subject must describe the email truthfully.

## Admin (Phase 3, home network only)

The owner's admin is a separate app, `storefront/admin/`, in its own container
(`poke6s-storefront-admin`) on port **8011**: open **http://192.168.1.240:8011/** from any
device on the home network. It is never reachable from the internet. The walls, each enough
on its own:

1. docker-compose publishes 8011 on the LAN address only (`STOREFRONT_ADMIN_BIND`, default
   192.168.1.240), and the admin is on its own Docker network, so the tunnel connector can't
   reach it. The tunnel has no route to it.
2. The app refuses requests carrying Cloudflare headers, and any address outside the private
   ranges (`refuse_reason()` in `admin/main.py`).
3. A password sign-in on every page, and a per-session token on every form; form posts from
   another site are refused.

The public app can't grow admin features by accident: `tests/test_public_surface.py` fails if
it gains `/admin` pages or a new write route.

**Sign-in.** One owner password, stored only as a PBKDF2 hash in `.env`. To set or change it,
on the server from the repo folder (asks twice, prints nothing secret):

    python3 -m storefront.admin.passwd
    bash deploy/update_storefront.sh

The first run also adds `STOREFRONT_ADMIN_SESSION_SECRET`. A device stays signed in for 30
days; changing the password signs every device out. Five wrong passwords in 15 minutes lock
that address out for 15 minutes and post a Discord alert.

**Pages so far** (built in steps; plan "Phase 3"):

| Address | Page |
|---|---|
| `/` | Overview: orders to ship (late ones flagged after 24 hours), this month's net sales (live orders only), order emails retrying or failed, last nightly backup, disputes, sold-out products still on show, store set-up |
| `/orders?view=&q=` | Orders: tabs To ship (oldest first), Shipped, Refunds & disputes, All sales, Unfinished checkouts; search by number, name or email |
| `/orders/<number>` | One order: items, totals, ship-to address, customer, Stripe link, timeline, notes |
| `POST /orders/<number>/ship` | Mark shipped with carrier and tracking (spaces allowed); sends the tracking email and says whether it went, went to the outbox, or is queued for retry |
| `POST /orders/<number>/note` | Add a dated note (only the owner sees notes) |
| `POST /orders/<number>/address` | Change the address of a paid, unshipped order (e.g. USPS's corrected version, once the customer confirms); the old address goes into the notes |
| `POST /orders/<number>/refund` | Refund any amount up to what's left through Stripe, and/or put returned units back on the shelf; emails the customer and notes it on the order (see "Refunds and returns") |
| `POST /orders/<number>/resend` | Send the order confirmation or tracking email again |
| `/orders/<number>/packing-slip.pdf` | Packing slip, US Letter PDF; `?gift=1` leaves the prices off |
| `/products?view=&q=` | Products: tabs On the store, Sold out on the store, Hidden, Hidden in stock, All; search by title, set or SKU; price next to the market price (% above or below) and stock |
| `/products/<id>` | One product: price, stock (available, in a checkout, on the shelf, sold here), what the store shows, and the history of every change |
| `POST /products/<id>/price` | New price and optional “was” price, with a reason (required). A change of more than 50% must be confirmed (catches 1299 typed for 12.99) |
| `POST /products/<id>/settings` | Shown or hidden, featured on the home page, per-order limit |
| `POST /products/<id>/stock` | Received (+N), Sold on TCGplayer (−N) or Counted (on the shelf = N; the difference is logged, leaving units in open checkouts alone) |
| `/products/new`, `POST /products/new` | Add a product: type TCGplayer ID(s), “Check and fill in” fills title, kind, language, set and a starting price from the market data and warns if the store already has a product with that ID (often hidden: show it instead); “Add product” saves it **hidden** with its starting stock |
| `POST /products/<id>/details` | Title, kind, language, set, TCGplayer IDs, barcode, description (the web address and SKU never change) |
| `POST /products/<id>/photos` | Upload photos (JPEG, PNG or WebP, 15 MB each, 12 per product; checked to be real images) |
| `POST /products/<id>/photos/<photo>/main`, `.../remove` | Make a photo the main one; remove one (its resized copies go, the original stays in `images/original` and the backup) |
| `/shipping` | Shipping: paid orders waiting for a label, with what the Pirate Ship file fills in (see "Shipping through Pirate Ship") |
| `POST /shipping/export` | Download the ticked orders as a CSV for Pirate Ship's spreadsheet import; marks them "in a file" |
| `POST /shipping/tracking`, `POST /shipping/tracking/apply` | Upload Pirate Ship's shipments CSV, see which orders it matches, then mark the ticked ones shipped (each customer gets the tracking email) |
| `POST /products/<id>/packed` | One unit's packed weight (oz, or "1.5 lb") and box size, for the Pirate Ship file |
| `/pricing`, `POST /pricing/apply` | Pricing: the repricing rules with a preview; applies only the ticked lines (see "Pricing page") |
| `/reports`, `/reports/sales.csv`, `/reports/stripe.csv` | Reports: a month's sales for the books (`?month=YYYY-MM`); read-only (see "Reports page") |
| `/supplier-profitability` | Suppliers: each SKU's newest supplier quote (`data/storefront/supplier_quotes.csv`) against store, market and target prices, per channel, with editable assumptions and a quick calculator. Market column (market price and its % above landed); the Verdict cell shows the best channel's margin, profit and return on cost. Sort (best buys, margin %, profit $, return on cost, market gap, cheapest landed, newest, name, or one channel's margin; also by clicking a heading), judge on one channel only, minimum margin %. Read-only; a GET form. Moved from the market site 2026-11-21 |
| `/supplier-best-buys` | Suppliers → Best buys: per box, every route (import or buy on TCGplayer + Utah tax; sell on the own site with its real buyer shipping, eBay or TCGplayer), the best route and the best one off the own site, fastest TCGplayer sellers first, and a suggested order (4 fast / 2 steady / 1 slow, halved when falling, less stock, never at a loss). POST `/supplier-best-buys/sales` appends the owner's TCGplayer sales numbers to `data/storefront/tcgplayer_sales.csv` |
| `/supplier-pricing` (GET, POST) | Suppliers, "Enter a quote": paste a supplier's price list (one item per line, or a whole copied shop page such as MACHO TCG's: each product's sealed box price, other versions in a note), or read a screenshot into the box in the browser (tesseract.js from `admin/static/vendor/tesseract/`), check each row's SKU against the name-to-SKU list (`data/storefront/supplier_name_mapping.csv`) with store and market prices beside it, and save the rows as one quote (`admin/supplier_quotes.py`) |
| `/login`, `POST /login`, `POST /logout` | Sign-in |
| `/healthz` | `{"ok", "schema", "sign_in_ready"}` (still home network only) |

The admin reads the same settings as the store (the compose file shares them), so it shows
the real checkout, Stripe, tax and email state. Every change is a form post carrying the
session's form token, then a redirect back to the page with a message (`DONE` in
`admin/main.py`). Order changes themselves live in `app/orders.py` (`mark_shipped`, `add_note`,
`correct_address`), shared with `orders_cli`, so the two can't drift apart.
Product changes live in `app/product_changes.py`: stock moves only by a logged
`stock_movements` row (its sum must equal `products.stock`), and price and visibility changes go
in `product_changes` (migration 005) with the old and new value and the reason; `tools/reprice.py`
logs its changes there too, marked “repricing”.
Adding products, detail edits and photos live in `app/product_details.py` (same rules as the
importer: kind, category and shipping class follow the type; the set comes from TCGplayer).
**Once products are added or edited here, don't re-run the Squarespace importer**: it would
reset their stock and details from the old export.

**Shipping one order:** Overview or Orders → open the order → "Packing slip (PDF)" (print at
Actual size) → buy the label → paste the tracking number → "Mark shipped and email tracking".

### Refunds and returns

On the order page, **Refund or return** (code: `admin/refunds.py`):
- **Refund** any amount up to what's left (the whole order, or e.g. only the shipping), with a
  reason Stripe passes to the bank. It goes through Stripe with the admin's **own restricted live
  key**, `STOREFRONT_ADMIN_STRIPE_KEY` in `.env` (Stripe: Charges and Refunds write, Charges and
  PaymentIntents read, nothing else). docker-compose passes it to the admin container only; the
  public store's key can't refund. Without it the form says refunds aren't set up.
- The order is updated at once through `orders.record_refund` (the same code as the
  `charge.refunded` webhook, which then finds nothing new and stays quiet), the customer gets a
  refund email (`notify.refund_email`), and a dated note says what was done.
- A **partial refund on an unshipped order leaves it paid**, so it stays in To ship.
- **Put back** returned units: one `restock` stock movement per product, referenced
  `Return <order number>`; the form never offers more than the order sold minus what's already
  back. A $0 refund with units put back is a return with no money back (yet).
- Safety: never more than paid minus refunded; a confirmation tick; an idempotency key from the
  order, its refunded total and the amount, so a double click can't refund twice. Disputed
  payments can't be refunded (answer the dispute in Stripe); test orders are refunded in Stripe's
  test dashboard. Stripe keeps its processing fee on refunded payments.
- Returns (D8): inspect the box first, then refund, and put back only units you can sell again.

Setting up the key (once): Stripe dashboard, live mode → Developers → API keys → Create
restricted key, "poke6s admin refunds", Charges and Refunds: Write, PaymentIntents: Read,
everything else None. Add it without showing it:

    read -rsp "Paste the rk_live key: " K; echo; printf 'STOREFRONT_ADMIN_STRIPE_KEY=%s\n' "$K" >> .env; unset K
    bash deploy/update_storefront.sh

### Shipping through Pirate Ship

For several orders at once (code: `admin/shipping.py`):

1. **Shipping** page → tick the orders (live orders not yet in a file are ticked) → "Download the
   Pirate Ship file". On pirateship.com, import it as a spreadsheet; the first time, Pirate Ship
   asks which column is which. Columns: Order ID, Name, Company (empty), Address, Address Line 2,
   City, State, Zipcode, Country, Weight (oz), Length/Width/Height (in), Rubber Stamp 1 (what goes
   in the box, e.g. `1x EN-AH-ETB`, printed on the label).
2. **Tick Signature Confirmation** for the batch in Pirate Ship (every order ships with one, plan
   D3); a spreadsheet import can't set it. Then buy the labels.
3. Export the shipments from Pirate Ship as CSV and upload it on the Shipping page, **unopened**
   (a spreadsheet turns USPS numbers into 9.4E+21; the upload refuses those). The preview shows
   which orders match; "Mark shipped and email tracking" marks the ticked ones, through the same
   `mark_shipped` as the order page.

Rules worth knowing:
- Weight is the sum of each item's packed weight; the box size is filled in only for a one-item
  order (two items may share a bigger box). Unknown values are left blank to type in Pirate Ship,
  and the page lists the products with no packed size yet. Packed sizes live in the products
  columns `weight_oz`, `length_in`, `width_in`, `height_in` (first filled by the Squarespace
  importer, which stored Squarespace's shipping weight) and are logged as "Packed size".
- No email column: the store sends its own tracking email, so Pirate Ship shouldn't send another.
- Text cells lose a leading `=`, `+` or `@`, so a customer-typed address can't act as a formula if
  the file is opened in a spreadsheet.
- Reading the tracking file is forgiving because Pirate Ship's export columns aren't documented:
  the order number is found by its pattern in any cell, the tracking number in the column whose
  header says "tracking", the carrier from a "carrier" column or the number's shape (`1Z` = UPS).
  Rows without one of our order numbers (TCGplayer or eBay labels) are only counted; rows whose
  status says voided, refunded or cancelled are skipped; an order with two different tracking
  numbers in the file isn't guessed (mark it on its page).
- `orders.labels_exported_at` (migration 007) records when an order last went out in a file, so
  the page warns before a second label is bought.

## Code map

| File | What it does |
|---|---|
| `app/support.py` | Support messages: checks, save, the Overview list, Mark answered, delete answered after a year |
| `app/wishlist.py` | The wishlist: save, remove, list; drops lists of accounts deleted on the market (reconcile job) |
| `app/accounts.py` | Who's signed in: the market's shared `poke6s_session` cookie checked against its session table (read-only); "My orders" |
| `app/notify_list.py` | The email list: sign-up (POST /notify), unsubscribe tokens, who is still subscribed |
| `app/mailing.py` | Marketing emails to the list, with the legal parts (see "Marketing emails"); HTML in `templates/email/marketing.html` |
| `app/main.py` | FastAPI app: routes, 404 + redirect handling, security headers, reconcile loop |
| `app/head.py` | Answers HEAD like GET without the body (FastAPI's GET routes would say 405); sits inside the metrics and visitor counting, so HEADs aren't page views |
| `app/orders.py` | Cart pricing, checkout, reservations, payment, expiry, refunds, disputes, shipping |
| `app/product_changes.py` | Admin price, visibility and stock changes, each logged (see "Pages so far") |
| `app/product_details.py` | Admin: add products, edit details, photos (each change logged) |
| `app/stripe_api.py` | The Stripe calls (Checkout Sessions) over HTTPS; webhook signature check |
| `app/notify.py` | Discord order alerts; order and shipping emails (Microsoft Graph, SMTP or the outbox folder; retry queue; HTML in `templates/email/order.html`) |
| `app/orders_cli.py` | `list`, `show`, `ship`, `reconcile`, `resend`, `test-email`, `mailing` from the command line |
| `app/catalog.py` | Read-only queries: filters, search, facets, product, sets, sitemap |
| `app/importer.py` | Squarespace export + intake CSVs → database |
| `app/classify.py` | Product type, language, category, shipping class; slugs; search text |
| `app/db.py`, `app/migrations/*.sql` | SQLite connection (read-only for pages, a writer for orders), migrations applied in order at start-up |
| `app/market.py` | Reads `market_reference.json` (reloads when the file changes) |
| `app/images.py` | Originals + WebP copies, named with a content hash |
| `app/seo.py` | JSON-LD (Product, BreadcrumbList, Organization), sitemap, robots |
| `app/redirects.py`, `redirects.csv` | 301s from old Squarespace addresses |
| `app/markdown.py` | Markdown rendering (raw HTML escaped); Squarespace HTML → Markdown |
| `app/templates/`, `app/static/` | Jinja2 pages, CSS (market site's colours, D4), small JS |
| `app/collector_hub.py` | Collector Hub's resources and their wording (links to the market's files) |
| `app/binder_art.py` | Binder Art Maker's set-logo list (the tool itself is `static/binder-art.js`) |
| `app/articles.py`, `app/article_import.py` | Articles (Markdown, loaded at start-up); import of the old blog posts |
| `content/articles/*.md` | The articles |
| `tools/reprice.py` | Prices at or near market, never below break-even; hides products that can't be (see "Prices"); `plan()`/`apply()` shared with the admin |
| `admin/pricing_view.py` | The Pricing page: reads the inputs, checks ticked lines against a fresh plan |
| `admin/reports.py` | The Reports page: shop months, worksheet rows, Utah totals, Stripe fees and refunds |
| `admin/supplier_profit.py` | Supplier Profitability: landed cost, break-even and target-margin prices per channel; the JPY/USD rate |
| `admin/best_buys.py` | Best buys: buy-and-sell routes per box, sales speed from the owner's TCGplayer numbers, suggested order |
| `admin/static/supplier-ocr.js`, `admin/static/vendor/tesseract/` | Supplier Pricing's screenshot reader (the admin's only script; pinned tesseract.js 5.1.1, served with `'wasm-unsafe-eval'`) |
| `admin/supplier_quotes.py` | Supplier Pricing: reads a price list's text or a copied shop page, matches names to SKUs, appends quotes to the ledger and remembers new matches |
| `tools/archive_squarespace_site.py` | One-off copy of the old site's pages, blog posts and images |
| `content/pages/*.md` | About, FAQ, Shipping, Returns, Terms, Privacy, Contact |
| `admin/main.py` | The admin app: the network walls, sign-in routes, Overview |
| `admin/auth.py` | Password hashing, signed session cookies, form tokens, lockout (standard library only) |
| `admin/overview.py` | What the Overview shows (read-only queries) |
| `admin/orders_view.py` | The Orders list: tabs, search, counts, Stripe link, timeline (read-only queries) |
| `admin/refunds.py` | Refunds through Stripe (the admin's own key) and returned units put back |
| `admin/shipping.py` | The Shipping page: Pirate Ship file out, tracking file back, packed sizes |
| `admin/packing_slip.py` | Packing slip PDFs (reportlab); prints the logo without its navy background |
| `admin/passwd.py` | Sets the admin password in `.env` |
| `admin/templates/`, `admin/static/admin.css` | Admin pages (no JavaScript) |
| `tests/` | pytest suite with synthetic fixtures |

The writer for the market prices lives with the market app:
`scripts/build_storefront_market_reference.py` (tested in `tests/test_storefront_market_reference.py`).

## Addresses

| Address | Page |
|---|---|
| `/` | Home: featured, new arrivals, categories, sets |
| `/shop` | All products. Query: `q`, `lang` (`en`/`ja`), `type`, `set`, `sort` (`newest`, `price-asc`, `price-desc`), `page` |
| `/c/<category>` | Category (same filters) |
| `/sets`, `/sets/<set-slug>` | Sets in stock |
| `/p/<slug>` | Product page, with the market price (linked to its history) and how far below it we are |
| `/cart` | Cart (kept in the browser; priced by `POST /cart/quote`). `?cancelled=<number>&k=<token>` releases a checkout the shopper backed out of |
| `POST /cart/quote` | `{"items": [{"sku", "qty"}]}` → lines, problems, subtotal, shipping, total (JSON) |
| `POST /checkout` | Same body → `{"url": <Stripe checkout page>}`; 409 with a new quote if the cart changed, 503 when checkout is off |
| `/order/thanks?session_id=` | After payment (confirms with Stripe if the webhook hasn't arrived) |
| `/order/<number>?k=<token>` | Order status; the link is in the order emails |
| `/account` | Your account (one Poke6s account): signed out, a "Sign in with Google" link to the market site; signed in, the live orders placed with that email (`app/accounts.py`) and the wishlist. Never cached |
| `POST /support` | The Contact page's message box (`name`, `email`, `order_number`, `message`; `website` is a bot trap): saved, Discord ping with the number only, 3 an hour per visitor and 30 for the site (`app/support.py`) |
| `/contact?order=<number>` | Contact page with email, Discord and the message box; the order number filled in ("Get help with this order" on `/account`) |
| `POST /wishlist` | The ♡ on product pages and Remove on `/account` (`product_id`, `action` add/remove, `back=account`); signed out goes to sign-in; other origins refused (`app/wishlist.py`) |
| `POST /stripe/webhook` | Stripe events (signature checked; each event applied once) |
| `/unsubscribe/<token>` | Email list opt-out: GET shows a confirm button, POST unsubscribes (also mail apps' one-click unsubscribe) |
| `/<page>` | Markdown pages from `content/pages/` |
| `/media/<480\|1200>/<name>.webp` | Product photos, cached for a year |
| `/sitemap.xml`, `/robots.txt`, `/healthz` | Search engines and monitoring |

## Redirects

On a would-be 404, in order: `redirects.csv` (pages and categories), a trailing slash
(one 301 to the address without it), then `product_redirects` for `/shop/p/...`. Unknown
`/shop/p/...` addresses (singles, deleted products) go to `/shop`. Squarespace's
`/shop?tag=Japanese` links are translated in the `/shop` route.

Product redirects live in the database rather than in `redirects.csv` so they follow a
product's current slug if it's renamed later.

## Text pages

`content/pages/<slug>.md` with a front-matter block (`title`, `description`, `draft`).
Pages marked `draft: true` show a "Draft" note and are left out of the sitemap; the owner
reviews them before launch, then removes the line.

## Not yet (later phases)

The rest of the admin: Pirate Ship export and tracking upload, refunds, products and stock,
pricing, reports (Phase 3; sign-in, Overview and Orders are done, see "Admin"); Google Merchant feed, per-article social cards, newsletter (Phase 4). See
the plan.
