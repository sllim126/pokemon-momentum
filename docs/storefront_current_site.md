# poke6s.com today (Squarespace): what the new storefront replaces

Last reviewed: 2026-10-31

A snapshot of the current Squarespace store, so the new storefront (`docs/storefront_plan.md`)
can cover everything it does and redirect its old addresses. Taken from the public site on
2026-10-31, the latest product export
(`data/archive/squarespace_exports/squarespace_export_20261009T082110Z.csv`) and the scripts
in this repo. The Squarespace **website plan ends 2026-11-14 and won't be renewed**; the
domain stays registered at Squarespace Domains.

## Hosting and accounts

- **Squarespace Core website plan**, billed yearly, on the site
  `polygon-springtail-44rm.squarespace.com`, with `poke6s.com` connected to it.
- **Domain:** `poke6s.com` is registered at Squarespace Domains. Its DNS is served by Google Cloud
  DNS nameservers (`ns-cloud-d*.googledomains.com`) and edited in the Squarespace Domains panel.
  The `market` A record points at the home server (see the IP-change item in `docs/todo.txt`).
- **Email:** `admin@poke6s.com` is Microsoft 365. Its MX, SPF, DKIM and autodiscover records sit in
  the same DNS zone. **Any DNS move must copy them exactly**, or mail stops.
- **Payments:** Squarespace Payments, paid out to the business bank account. Chargebacks are taken
  from future payouts or debited from the bank.

## DNS records today (public lookup, 2026-10-31)

Copy all of these into Cloudflare when DNS moves (plan Phase 0). DNSSEC is off (no DS record), so
the nameservers can be changed without turning it off first.

| Name | Type | Value | Purpose |
|---|---|---|---|
| `poke6s.com` | NS | `ns-cloud-d1..d4.googledomains.com` | Replaced by Cloudflare's two nameservers |
| `poke6s.com` | A | 198.185.159.144, 198.185.159.145, 198.49.23.144, 198.49.23.145 | Squarespace website. Keep until 2026-11-14, then replace with forwarding (D5) or the tunnel |
| `www` | CNAME | `ext-sq.squarespace.com` | Squarespace website. Same as above |
| `market` | A | the home server's public IP | Market site. Becomes a Cloudflare Tunnel record |
| `poke6s.com` | MX | `0 poke6s-com.mail.protection.outlook.com` | **Microsoft 365 mail** |
| `poke6s.com` | TXT | `v=spf1 include:spf.protection.outlook.com -all` | **Mail sender check (SPF)** |
| `poke6s.com` | TXT | `MS=ms28030294` | Microsoft 365 domain verification |
| `autodiscover` | CNAME | `autodiscover.outlook.com` | Outlook setup |
| `_dmarc` | TXT | `v=DMARC1; p=none` | Mail reporting policy |

No DKIM records (`selector1._domainkey` / `selector2._domainkey`) were found. Turning DKIM on in
Microsoft 365 (free) adds two CNAMEs and helps order emails reach inboxes. Do it after the DNS move.

## Site map (public pages)

| Address | Page | Notes |
|---|---|---|
| `/` | Home | Hero "Welcome to Poké6s!" with Shop Now and Browse by Set; three featured products; featured groups (English Booster Bundles, Japanese Booster Boxes, Booster Packs); "More about Poké6s" blurb |
| `/shop`, `/shop-1` | Shop all | Filter links `?tag=Japanese`, `?tag=English`, `?tag=Booster Bundle` |
| `/shop/booster-box`, `/shop/booster-box/japanese`, `/shop/booster-pack` | Category pages | Category tree: `docs/store_category_tree_proposal.md` |
| `/shop/p/<slug>` | Product pages | e.g. `/shop/p/abyss-eye-booster-box`, `/shop/p/ninja-spinner-booster-box`, `/shop/p/perfect-order-art-set-4` |
| `/browse-by-set` | Browse by set | |
| `/resources`, `/resources/<slug>` | Resources (blog) | 10 posts, 2025-12-15 to 2026-03-19, with categories and tags (`/resources/category/...`, `/resources/tag/...`). No new home yet: see `docs/todo.txt` |
| `/about` | About / mission | |
| `/faq-1` | FAQs | |
| `/cancellation` | Return and cancellation policy | No returns; damaged or wrong items made right by email; cancellation before shipping costs a 6% fee (covers card processing). D8 replaces the no-returns rule |
| `/new-page` | Terms & Conditions | Placeholder-looking address. Effective 2026-04-07; also covers market.poke6s.com |
| `/new-page-1` | Reviews | Squarespace template testimonials ("Former Customer"), not real reviews. Not carried over |
| `/pre-order-interest` | Pre-order interest | Heading and two images only |
| `/home` | Copy of the home page | |
| `/privacy-policy` | Privacy policy | |
| `/contact` | Contact form | No email or phone shown |
| `/newsletter-signup` | "Notify Me" email signup | Showed a storage-configuration message on 2026-10-31, so it may not be saving signups |
| `/cart` | Cart | Squarespace checkout |

There's no shipping policy page and no social links. The header links out to
**Shop on TCGplayer** (`https://poke6s.tcgplayerpro.com/`) and **Market**
(`https://market.poke6s.com`). Footer: FAQs, Cancellations, Contact Us, Terms & Conditions,
Privacy Policy, newsletter signup, "©2025 Poke6s LLC".

## Saved copy of the site (2026-10-31)

All public page text, the 10 blog posts and every image (pages, posts and the 238 product photo
addresses in the export) were saved from the live site on 2026-10-31 by
`storefront/tools/archive_squarespace_site.py`. The copy is kept with the owner's business
records, outside this repo (it holds the products, orders and customer exports alongside). Each
page and post is in Markdown and as served, plus the original images. Re-run the script just before
2026-11-14 to pick up any late edits. One image was already broken on Squarespace (the Ninja Spinner
banner in the 2026-02-09 post) and couldn't be saved.

## Catalog (export of 2026-10-09)

- 183 product/variant rows: 102 visible and 78 hidden. 130 rows have stock above 0.
- **Tags:** Singles (89), Japanese (32), English (32), plus product traits (Booster Box, Elite,
  Costco, set names). Tag rules: `docs/store_tag_policy.md`.
- **Categories:** `/booster-box/japanese`, `/booster-pack/english`, `/mega-evolution/<set>`,
  `/other/collection-boxes`, `/other/tins`, `/other/other`.
- **SKU convention:** singles use the **TCGplayer product ID** as the SKU (holofoil variants add
  `-holofoil`). Sealed products use their own SKUs, mapped to TCGplayer IDs in
  `data/squarespace_tcgplayer_mapping.csv` (one store SKU can map to several IDs).
- **Export columns:** product and variant IDs, type, page, URL slug, title, description, SKU, GTIN,
  MPN, up to 6 option pairs, price, sale price, on-sale flag, stock, categories, tags,
  weight and dimensions, visible, hosted image URLs.

## How this repo works with it today

| What | Where | What happens when the plan ends |
|---|---|---|
| Daily 09:15 price sync (writes prices to the live store through the Squarespace API) | `scripts/squarespace_price_sync.py`, cron `scripts/run_squarespace_price_sync.sh` | Remove from cron and stop, or it fails every day |
| Listing intake, drafts, creation, retiring | `scripts/*squarespace*`, `scripts/build_standardized_sealed_inventory_import.py`, `scripts/convert_collection_csv_to_single_listing_intake.py` | Re-point the intake and pricing parts at the new store; retire the API-writing parts |
| Store price targets and rules | `scripts/build_store_price_targets.py`, `data/store_price_rules.csv`, `scripts/common/selling_costs.py` | Kept: the new store's pricing |
| "Poke6s" buy button on market.poke6s.com (shows when an item is visible and in stock in the latest export) | `load_squarespace_listing_by_sku()` and the mapping loaders in `scripts/dashboards/api.py`; runbook "Storefront button" section | Switch to the new store's stock |
| Product images cached from Squarespace | `scripts/cache_squarespace_single_listing_images.py` | Copy images into the new store before the plan ends |
| Export upload page | `/pricing-upload` | Replaced by the new store's own database |
| Earlier storefront thinking | `docs/retail_site_blueprint.md`, `docs/retail_page_review_worksheet.md`, `docs/store_category_tree_proposal.md`, `docs/store_tag_policy.md` | Inputs to the new design |

## Before 2026-11-14 (owner)

1. Export **products** (Commerce → Inventory → Export) and save it in
   `data/archive/squarespace_exports/`. Data files aren't committed.
2. Export **customers** and **email subscribers** (Contacts / Email Campaigns audience). These hold
   personal data: keep them outside the repo, e.g. in the private books folder.
3. Save copies of the About, FAQ, Cancellation, Terms and Privacy page text.
4. Download every product image, or run the image-cache script.
5. Turn off the website plan's auto-renew. **Keep the domain.**
