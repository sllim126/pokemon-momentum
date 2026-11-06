# KPDH Binder Planner: Series 2 Plan

Last reviewed: 2026-11-06

This document records the source policy, implementation and deployment checks used to add
Kayou KPop Demon Hunters Energy Edition Series 2 and Classic Edition Series 2 to the
standalone planner at `/kpdh/`. Series 2 went live on 2026-10-25. The owner subsequently
confirmed that collection import/export works as expected. The previously isolated KPDH
branch was reconciled onto the current repository history on 2026-11-05 without importing
its unrelated dashboard and operations commits.

## Deployment status

- Live catalog: 142 Energy entries plus 111 Classic entries.
- Shared-card verification: 75 cross-edition images are byte-identical, leaving 178 unique
  Series 2 master-binder cards and 397 unique cards across the full planner.
- Compatibility: existing Series 1 IDs and the `kpdh-master-binder-v2` browser key are
  unchanged. Catalog v4 migrates the nine provisional Energy CR IDs from `CR-001`–`009`
  to the confirmed continuous range `CR-007`–`015` without losing quantities.
- Promo evidence: all eight known Series 1 promos now have owner-supplied 600 DPI front and
  back scans. Catalog v5 corrects their printed suffixes from the provisional three-digit
  form to `-01`–`-03`, corrects PR2-02 from Rumi to Zoey, and migrates existing quantities.
  The published crops retain the sealed sleeves; the archival PDFs remain ignored under
  `data/kpdh_scan_intake/`. Detail views can switch between each card's front and back.
  Deployed 2026-11-06; every front and back was verified through the public `/kpdh/` route.
- Verification: the full test suite passed (241 tests and 33 subtests); the local and public
  planner, catalog, representative Series 1/2 assets, redirects, noindex/security headers,
  root market page and container logs were checked after deployment.
- Rollback: the prior local Docker image is tagged
  `kpdh-binder-planner:rollback-20261025-pre-series2`. The image immediately before the
  2026-10-28 CR renumbering is tagged
  `kpdh-binder-planner:rollback-20261028-pre-cr-renumber`; the image immediately before
  the 2026-10-29 lime evidence markers is tagged
  `kpdh-binder-planner:rollback-20261029-pre-evidence-markers`. The image immediately before
  the physical promo scans is tagged `kpdh-binder-planner:rollback-20261106-pre-promo-scans`.

## Confirmed release information

Kayou has published complete product and card-list pages for both sets:

- Energy Edition Series 2: <https://www.kayouofficial.com/en-US/series/series-41jmupca>
  - Product: `KPop Demon Hunters-Collectible Trading Cards-Energy Edition-002-NA`
  - Model: `KPDH-KP-NL-002A-NA`
  - Official release date: October 2026
  - 142 cards in 11 rarity tiers: SE 6, BP 8, CR 9, SP 6, LR 6, UR 8,
    TR 20, QR 19, HR 24, SSR 16 and SR 20
- Classic Edition Series 2: <https://www.kayouofficial.com/en-US/series/series-udf0eb3v>
  - Product: `KPop Demon Hunters-Collectible Trading Cards-Classic Edition-002-NA`
  - Model: `KPDH-KP-JD-002-NA`
  - Official release date: October 2026
  - 111 cards in 8 rarity tiers: CR 6, ZR 6, UR 8, TR 20, QR 19, SSR 13,
    SR 15 and R 24

Kayou gives only a month, not a day. US retailer dates are useful for availability
monitoring but must not replace the official release field. As of 2026-10-25, retailer
listings range from November 6 to a general November 2026 estimate. The planner should
therefore display `October 2026 (official release month)` and, only if useful, a separately
labelled `US availability observed ...` note with its source and observation date.

Kayou customer support confirmed on 2026-10-26 that Series 2 CR numbering continues
between products: Classic uses `CR-001`–`006`, then Energy uses `CR-007`–`015`. This is
not a continuation from Series 1. The confirmation was published with a screenshot at
<https://www.reddit.com/r/KayouKpopDemonhunters/comments/1xfs1r1/>. It corrects the
generator's earlier assumption that every rarity restarted at 001. Physical cards remain
the final check after release, so the live planner carries a visible provisional-evidence
note and preserves collection data through explicit catalog migrations.
The nine affected Energy CR cards use a lime outline and `Verify` marker in the planner;
their detail views say that physical confirmation is pending. Remove that treatment only
after the printed cards have been checked and any necessary catalog migration is complete.

## Source policy

Use evidence in this order:

1. **Printed card in Kayou's official image** for card number, rarity and visible title.
2. **Kayou's official set page** for product identity, release month, rarity order, card
   count, display order and official image URL.
3. **Physical card or packaging owned by the owner** when the image is unclear or Kayou's
   text and image disagree.
4. **Retailer listings** only for regional preorder/availability notes.
5. Collector sites, social posts and marketplace listings may identify a question, but
   they are never imported as card data without confirmation from sources 1-3.

The Series 2 pages contain a known internal-data error: some website metadata identifies
Series 2 cards as `KPDH01`. Official Series 2 card images and physical cards print
`KPDH02`. Every Series 2 standard-size card must therefore use the printed `KPDH02`
prefix. Do not derive the prefix from page metadata, DOM IDs, alt text or a third-party
checklist.

Names in Kayou's page text also need visual review. For example, labels such as `Abbey`,
`Doctor`, `Grandma`, `your idol` and combined-character abbreviations may be descriptions
rather than the best collector-facing name. Keep the official page label in provenance,
but use a normalized display name only after reviewing the corresponding image.

## Data and provenance design

Move the inline card arrays out of `kpdh-binder/my_card_planner.html` into a versioned data
file, preferably `kpdh-binder/cards/catalog.js` so the static, CSP-restricted site needs no
fetch request. Each record should contain:

- stable ID, such as `energy-2:KPDH02-SE-001`
- printed card code
- normalized display name
- Kayou's page label verbatim
- series number and edition
- rarity code and official rarity name
- position within its rarity
- orientation
- official set-page URL
- official full-image URL
- local full-image and thumbnail paths
- image SHA-256, verification date and verification status
- optional discrepancy note

Keep set metadata separately: product name, model, release month, official URL, rarity
counts and last verification date. This makes release information auditable without
duplicating it on every card.

Do not overwrite Series 1 records. Series 1 uses `KPDH01`, Mini Series 1 uses `KPDHM01`,
and Series 2 uses `KPDH02`. The eight existing promo records remain a separate group. Their
printed codes, character names, orientations and fronts/backs were physically verified from
owner-owned cards on 2026-11-06. The reproducible crop map is in
`kpdh-binder/scripts/build_promo_scans.py`; raw scanner PDFs stay outside Git under `data/`.

Energy and Classic share some Series 2 cards. Confirm sharing by comparing printed code and
official artwork; do not assume that matching rarity/index means the cards are identical.
If both match, store one collection identity with both editions and retain both official
image URLs in its provenance. Edition-exclusive cards with the same-looking index remain
separate records.

## Collection compatibility

The browser database currently uses `kpdh-master-binder-v2`, with IDs containing edition
and card code. Preserve all existing IDs and quantities.

- Increment the catalog/export schema version, not the local-storage key, unless migration
  testing proves a new key is necessary.
- Add Series 2 records without changing any Series 1 ID.
- Continue accepting older exports and ignore unknown fields safely.
- Include catalog version and exported-at time in new exports.
- During import, retain recognized Series 1 and Series 2 IDs, report unknown IDs, and never
  silently translate `KPDH01` into `KPDH02`.
- Test an actual Series 1 export before deployment and confirm its counts and binder size are
  unchanged after loading the Series 2 build.

## Planner changes

- Add Series and Edition controls that clearly distinguish Energy 1, Classic 1, Mini 1,
  Energy 2, Classic 2 and Promos.
- Keep fixed binder positions within each section; adding Series 2 must not rearrange the
  user's Series 1 pages.
- Show official release month and a source link in set/card details.
- Show a visible `Source discrepancy` note for records affected by Kayou metadata errors.
- Update totals only from the validated catalog, not handwritten marketing totals.
- Continue serving all images locally; source links are provenance, not runtime dependencies.
- Preserve the phone layout and horizontal page navigation added on 2026-10-21.

Expected counts before any new promos are discovered:

- Series 1 master planner: 219 current records, including eight promos
- Series 2 Energy: 142 product cards
- Series 2 Classic: 111 product cards
- Series 2 unique-master total: calculate only after image/code comparison identifies the
  genuinely shared cards; do not use `142 + 111` as the public total

## Asset preparation

1. Save snapshots of the two official page URLs and record the retrieval date.
2. Extract official image URLs in Kayou's displayed rarity/card order.
3. Download each image once with a descriptive user agent and modest pacing; stop on 403 or
   429. This is a one-time publishing task, not a browser-side scraper.
4. Store originals under `cards/energy-2/` and `cards/classic-2/` using the verified printed
   `KPDH02` code.
5. Create WebP thumbnails using the same dimensions/quality policy as Series 1.
6. Compute hashes, detect duplicate artwork, and flag shared-code images whose pixels differ.
7. Visually inspect every code and all questionable names. A generated contact sheet can
   make this review practical, but it is not itself a source.
8. Generate the catalog and provenance manifest from the reviewed records.

The downloader/generator should be committed under `kpdh-binder/scripts/`; downloaded page
snapshots are review artifacts and should be kept only if their size and copyright status
make them appropriate for the public repository. Never scrape on each Docker build.

## Validation gates

Before deployment:

- Schema tests reject duplicate stable IDs and duplicate edition/code pairs.
- Counts by set and rarity exactly match Kayou's published totals.
- Every Series 2 standard card code begins `KPDH02-`; a test must fail on `KPDH01-`.
- Every local image exists, has a source URL and matches its stored hash.
- Shared-card decisions have matching printed codes and reviewed artwork.
- No third-party domains appear in catalog provenance except explicitly labelled retailer
  availability notes.
- Import/export round trips preserve Series 1 and Series 2 quantities.
- An old Series 1 export imports without loss.
- The planner has no missing-image requests or console errors.
- Desktop checks cover 1920x1080 and 1366x768; phone checks cover portrait and landscape.
- The public build remains noindex, read-only and functional under the `/kpdh/` prefix.

## Deployment and rollback

1. Export the owner's live collection JSON and retain it outside the deployment folder.
2. Build and test the image locally, including `docker compose config` and the local 8091
   endpoint.
3. Record the current image ID and create a recoverable copy/tag before rebuilding.
4. Run `docker compose up -d --build` in `/opt/pokemon-momentum/kpdh-binder`.
5. Verify the local endpoint, then `https://market.poke6s.com/kpdh/`, one Series 1 image,
   one card from every Series 2 rarity, headers, redirects and compression.
6. Import a copy of the owner's export and verify representative quantities. Do not replace
   the owner's only browser copy during testing.
7. Watch the container and Caddy logs for missing assets or upstream errors.

No Caddy route change should be necessary because `/kpdh/` already proxies to port 8091.
If validation fails, restore the prior Docker image/build and verify the existing Series 1
planner. Browser collection data should survive rollback because existing IDs and the
storage key remain unchanged.

## Implementation stages

1. Build the source manifest and verification script; do not change the UI yet.
2. Review codes, names, shared cards and official-image coverage.
3. Refactor Series 1 into the versioned catalog with regression tests.
4. Add the verified Series 2 records and assets.
5. Add Series/Edition UI and source/release details.
6. Run compatibility, responsive and container tests.
7. Obtain owner approval for the live deployment, then deploy and verify.
