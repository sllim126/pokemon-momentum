# Poke6s storefront (poke6s.com)

Our own shop, replacing Squarespace. The plan, decisions and progress log are in
`docs/storefront_plan.md`; this file is the developer guide for the code in `storefront/`.

**Status (2026-10-31): Phase 1, local only.** The catalog browses on the home network at
`http://<server>:8010`. There is no checkout yet: product pages link to the TCGplayer store.
Nothing is public: robots are blocked and every page says `noindex` until
`STOREFRONT_ALLOW_INDEXING=1` at launch.

## Run it

```sh
docker-compose up -d --build storefront      # first time, or after editing requirements.txt
docker-compose restart storefront            # after Python, template or Markdown page changes
make test-storefront                         # 46 tests, synthetic data only
```

The container (`poke6s-storefront`, port 8010) sees only:
- `storefront/` (the code, mounted, so no rebuild for code changes);
- `data/` **read-only** (Squarespace exports, mapping and intake CSVs);
- `data/storefront/` read-write: `store.db`, `images/`, `market_reference.json`.

`data/storefront/` is git-ignored: it holds business data and, from Phase 2, customer data.

CSS and JavaScript changes need only a browser reload. Python, templates and the Markdown
pages are read at start-up, so restart the container after changing them.
Also restart it after switching git branches: a checkout that removes and recreates
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

The market reference isn't in the daily job yet: adding it to cron is an owner step.

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
and `index.csv`/`images.csv`. Re-runs skip images already saved. The posts are ready for
`content/articles/` when articles are built.

## Code map

| File | What it does |
|---|---|
| `app/main.py` | FastAPI app: routes, 404 + redirect handling, security headers |
| `app/catalog.py` | Read-only queries: filters, search, facets, product, sets, sitemap |
| `app/importer.py` | Squarespace export + intake CSVs → database |
| `app/classify.py` | Product type, language, category, shipping class; slugs; search text |
| `app/db.py`, `app/migrations/*.sql` | SQLite connection (read-only for pages), migrations applied in order at start-up |
| `app/market.py` | Reads `market_reference.json` (reloads when the file changes) |
| `app/images.py` | Originals + WebP copies, named with a content hash |
| `app/seo.py` | JSON-LD (Product, BreadcrumbList, Organization), sitemap, robots |
| `app/redirects.py`, `redirects.csv` | 301s from old Squarespace addresses |
| `app/markdown.py` | Markdown rendering (raw HTML escaped); Squarespace HTML → Markdown |
| `app/templates/`, `app/static/` | Jinja2 pages, CSS (market site's colours, D4), small JS |
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

Cart, Stripe test-mode checkout, orders and webhooks (Phase 2); LAN-only admin, exports and
backups (Phase 3); Google Merchant feed, articles (Phase 4). See the plan.
