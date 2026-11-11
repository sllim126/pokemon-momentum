# Poke6s storefront (poke6s.com)

Our own shop, replacing Squarespace. The plan, decisions and progress log are in
`docs/storefront_plan.md`; this file is the developer guide for the code in `storefront/`.

**Status (2026-11-05): Phase 2 built, local only, Stripe test mode.** The catalog browses on
the home network at `http://<server>:8010`. Cart and checkout switch on when
`STOREFRONT_CHECKOUT=1` and a Stripe **test** key are in `.env` (see "Checkout" below);
otherwise product pages link to the TCGplayer store. The public launch (2026-11-11) is
catalog-only: `STOREFRONT_CHECKOUT` is unset, which means off.
Nothing is public: robots are blocked and every page says `noindex` until
`STOREFRONT_ALLOW_INDEXING=1` at launch.

## Run it

```sh
deploy/update_storefront.sh                  # run the store on :8010 from the latest main
make test-storefront                         # synthetic data only
```

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

## Articles

The blog lives at `/articles` (list, with category and tag filters) and `/articles/<slug>`.
Each article is a Markdown file in `content/articles/<slug>.md`; the front matter is described
in `app/articles.py`. To write a new one, copy an existing file, change the front matter and
text, and restart the container. `draft: true` keeps it off the list, the home page and the
sitemap while it can still be previewed at its address. Pictures go in like product photos
(`images.store_original`) and are referenced as `/media/1200/<name>.webp`.

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
`deploy/update_storefront.sh` (it recreates the container, so the new settings are read):

```
STOREFRONT_CHECKOUT=1                 # master switch; unset or 0 = catalog only (the default)
STRIPE_SECRET_KEY=sk_test_...         # Stripe dashboard > Developers > API keys (test mode)
STRIPE_WEBHOOK_SECRET=whsec_...       # from `stripe listen` or the dashboard webhook (optional)
```

Live keys (`sk_live_...`) are refused unless `STOREFRONT_ALLOW_LIVE=1`; going live is the
owner's call. Other settings (all optional, defaults in `app/settings.py`):
`STOREFRONT_FREE_SHIPPING_OVER_CENTS` (15000), `STOREFRONT_SHIP_<CLASS>_CENTS` (placeholders:
small sealed 500, large sealed 900, accessory 500, single 500), `STOREFRONT_TAX_MODE`
(decision D2: `stripe` = Stripe Tax adds Utah sales tax on top at checkout, once Utah is
registered under Tax -> Registrations in the Stripe dashboard; `none` = no tax, the default;
anything else keeps checkout off), `STOREFRONT_DISCORD_WEBHOOK_URL` (order
alerts; off when empty), `STOREFRONT_SMTP_HOST` / `_USER` / `_PASSWORD` (Microsoft 365, port
587). Without SMTP, order emails are written to `data/storefront/outbox/*.eml`.

Order emails (confirmation and shipped) have a plain-text part and a designed HTML part,
`app/templates/email/order.html`: tables and inline styles only, because email apps ignore most
CSS. The logo is attached to the email itself (`cid:`), so it shows before the site is public.
The shipped email links USPS, UPS, FedEx and DHL numbers to the carrier's tracking page
(`notify.TRACKING_URLS`; other carriers show the number without a button).

**A test order.** Add something to the cart, Checkout, and pay on Stripe's page with card
`4242 4242 4242 4242`, any future date, any CVC, any US address. Orders are numbered
`P6T-2026-0001` in test mode (`P6-...` live).

**Webhooks are optional locally.** Payment is confirmed three ways, whichever comes first: the
webhook (`POST /stripe/webhook`), the thanks page asking Stripe directly, and the reconcile job
(start-up and every 15 minutes) that checks every pending order. To test webhooks, run the
Stripe CLI (`stripe listen --forward-to localhost:8010/stripe/webhook`) and put the `whsec_...`
it prints in `.env`.

**Orders from the command line** (until the Phase 3 admin):

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

## Code map

| File | What it does |
|---|---|
| `app/main.py` | FastAPI app: routes, 404 + redirect handling, security headers, reconcile loop |
| `app/orders.py` | Cart pricing, checkout, reservations, payment, expiry, refunds, disputes, shipping |
| `app/stripe_api.py` | The Stripe calls (Checkout Sessions) over HTTPS; webhook signature check |
| `app/notify.py` | Discord order alerts; order and shipping emails (SMTP or the outbox folder; HTML in `templates/email/order.html`) |
| `app/orders_cli.py` | `list`, `show`, `ship`, `reconcile` from the command line |
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
| `app/articles.py`, `app/article_import.py` | Articles (Markdown, loaded at start-up); import of the old blog posts |
| `content/articles/*.md` | The articles |
| `tools/archive_squarespace_site.py` | One-off copy of the old site's pages, blog posts and images |
| `content/pages/*.md` | About, FAQ, Shipping, Returns, Terms, Privacy, Contact |
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
| `/p/<slug>` | Product page, with the market price and 30-day trend |
| `/cart` | Cart (kept in the browser; priced by `POST /cart/quote`). `?cancelled=<number>&k=<token>` releases a checkout the shopper backed out of |
| `POST /cart/quote` | `{"items": [{"sku", "qty"}]}` → lines, problems, subtotal, shipping, total (JSON) |
| `POST /checkout` | Same body → `{"url": <Stripe checkout page>}`; 409 with a new quote if the cart changed, 503 when checkout is off |
| `/order/thanks?session_id=` | After payment (confirms with Stripe if the webhook hasn't arrived) |
| `/order/<number>?k=<token>` | Order status; the link is in the order emails |
| `POST /stripe/webhook` | Stripe events (signature checked; each event applied once) |
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

Utah registration in live-mode Stripe Tax and live keys (owner); LAN-only admin, exports,
signature confirmation on large orders and backups (Phase 3); Google Merchant feed, per-article social cards, newsletter (Phase 4). See
the plan.
