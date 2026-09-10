Retail Page Review Worksheet
============================

Updated: August 10, 2026

Purpose
-------

This is the working sheet for the `poke6s.com` retail-site cleanup pass.

It turns the strategy in [retail_site_blueprint.md](/opt/pokemon-momentum/docs/retail_site_blueprint.md)
into page-level decisions.

Status note
-----------

This repo does **not** currently contain a full export of all live Squarespace
pages.

So this worksheet is split into two layers:

- pages and collection lanes we can confirm from repo data
- likely trust/support pages that should exist, but still need a live
  Squarespace check

Evidence used
-------------

This worksheet is based on:

- [retail_site_blueprint.md](/opt/pokemon-momentum/docs/retail_site_blueprint.md)
- [store_category_tree_proposal.md](/opt/pokemon-momentum/docs/store_category_tree_proposal.md)
- [store_tag_policy.md](/opt/pokemon-momentum/docs/store_tag_policy.md)
- [output/squarespace_created_single_listings.csv](/opt/pokemon-momentum/output/squarespace_created_single_listings.csv)
- [output/squarespace_created_sealed_listings.csv](/opt/pokemon-momentum/output/squarespace_created_sealed_listings.csv)
- [output/squarespace_sealed_import_update.csv](/opt/pokemon-momentum/output/squarespace_sealed_import_update.csv)

Current inventory signals from repo data
----------------------------------------

Confirmed listing counts:

- singles listings created: `79`
- sealed listings created: `25`
- current created listings are entirely `English` in the local ledgers

Confirmed active storefront lanes from repo data:

- `Singles`
- `Sealed`
- `Shop on TCGplayer` as a clear off-site path

Important caution:

- the strategy calls for a visible `Japanese` lane
- the current local created-listing ledgers do not yet show active Japanese
  listings
- that means `Japanese` is still strategically important, but may need to be
  lighter in nav or presentation until live inventory depth improves

Decision legend
---------------

Use one of these decisions for each page:

- `Keep Top Nav`
- `Keep Homepage Path`
- `Move to Resources`
- `Move to Footer`
- `Hide`
- `Remove`

Section A: Primary Retail Paths
-------------------------------

These are the most important retail-domain paths to confirm and shape first.

| Page / Lane | Current Evidence | Recommended Decision | Why |
| --- | --- | --- | --- |
| Homepage | strategic requirement | `Keep Homepage Path` | Must act like a storefront, show inventory, and establish trust quickly. |
| `Singles` landing | `79` created singles listings in local ledger | `Keep Top Nav` | Singles is the clearest long-term differentiation lane and already has meaningful local inventory work behind it. |
| `Sealed` landing | `25` created sealed listings in local ledger | `Keep Top Nav` | Sealed is active, understandable to buyers, and already maps cleanly to category lanes. |
| `Japanese` landing | strategy requirement, but no confirmed active Japanese created listings in current ledgers | `Keep Top Nav` | Still belongs in the retail structure, but should stay curated and simple rather than sprawling. |
| `Resources` landing | strategy requirement | `Keep Top Nav` | Good container for buying guides, condition help, and trust-building support pages without crowding the store path. |
| `Shop on TCGplayer` | repo already routes TCGplayer links and treats it as a valid off-site path | `Keep Top Nav` | Clear outbound option for shoppers who want broader marketplace inventory. |

Section B: Trust And Support Pages
----------------------------------

These probably belong on the retail site, but not in prime navigation.

| Page / Lane | Current Evidence | Recommended Decision | Why |
| --- | --- | --- | --- |
| FAQ | expected support page, not yet confirmed from repo as a live Squarespace page | `Move to Footer` | Useful for reducing hesitation, but not part of the main browse path. |
| Shipping policy | expected support page, not yet confirmed | `Move to Footer` | Important for trust and purchase confidence. |
| Returns policy | expected support page, not yet confirmed | `Move to Footer` | Useful trust/support content, but secondary to shopping flow. |
| Contact | expected support page, not yet confirmed | `Move to Footer` | Helps legitimacy, but should not occupy top-nav space. |
| About / store story | expected support page, not yet confirmed | `Move to Footer` | Helps establish legitimacy if concise and buyer-relevant. |
| Singles condition guide | recommended by strategy, not yet confirmed | `Move to Resources` | Good support page if it directly answers condition uncertainty for card buyers. |
| Buying guide pages | not yet confirmed | `Move to Resources` | Worth keeping only if they help a shopper understand products and buy confidently. |

Section C: Confirmed Singles Collection Patterns
------------------------------------------------

The current singles ledger suggests the site already supports a deep singles
taxonomy.

That is useful operationally, but not every collection page should be promoted.

Confirmed singles base patterns from the created listing ledger:

- base lane: `/singles/english`
- repeated taxonomy dimensions:
  - set-specific paths
  - rarity paths
  - printing paths
  - condition paths

Examples from current data:

- `/singles/sv/prismatic-evolutions`
- `/singles/sv/scarlet-violet-promo-cards`
- `/singles/swsh/crown-zenith`
- `/singles/me03/perfect-order`
- `/singles/condition/near-mint`
- `/singles/printing/holofoil`

Recommended review rule for singles collection pages:

- keep the main `Singles` landing page obvious
- keep selected set pages only when inventory is strong enough to feel alive
- do **not** promote printing, condition, rarity, or thin set pages in top nav
- keep deeper taxonomy pages indexable or usable for browse/search only if they
  actually support shopping

Recommended singles page decisions:

| Page / Lane | Current Evidence | Recommended Decision | Why |
| --- | --- | --- | --- |
| Main `Singles` landing | confirmed | `Keep Top Nav` | Primary browse lane. |
| `English Singles` landing | confirmed through current ledger | `Keep Homepage Path` | Useful because the current local singles inventory is English-heavy. |
| `Japanese Singles` landing | not confirmed in current created ledger | `Hide` | Keep available when inventory exists, but do not force it into the structure until the lane feels stocked. |
| High-signal set pages | confirmed via set-based category paths | `Keep Homepage Path` | Useful only for the small number of sets with enough inventory or demand. |
| Rarity pages | confirmed in taxonomy only | `Hide` | Too granular for primary retail navigation. |
| Printing pages | confirmed in taxonomy only | `Hide` | Operationally useful, but not top-level browse structure. |
| Condition pages | confirmed in taxonomy only | `Hide` | Better as support or filtering structure than public navigation. |
| Thin set pages | confirmed as possible by taxonomy | `Remove` | If a set page has only a few cards, merge it into broader singles browse. |

Section D: Confirmed Sealed Collection Patterns
-----------------------------------------------

The current sealed ledger is much simpler and lines up well with the retail
strategy.

Confirmed sealed base patterns from the created listing ledger:

- base lane: `/sealed/english`
- repeated product-type paths:
  - `/sealed/type/booster-box`
  - `/sealed/type/booster-bundle`
  - `/sealed/type/pokemon-center-etb`
  - `/sealed/type/tin`
  - `/sealed/type/premium-collection`
  - `/sealed/type/two-pack-blister`

Confirmed active sealed set/style paths include:

- `/sealed/english/me03-perfect-order`
- `/sealed/english/me04-chaos-rising`
- `/sealed/english/me05-pitch-black`
- `/sealed/english/me-ascended-heroes`
- `/sealed/english/sv-black-bolt`
- `/sealed/english/sv-scarlet-violet-151`
- `/sealed/english/swsh-crown-zenith`

Recommended sealed page decisions:

| Page / Lane | Current Evidence | Recommended Decision | Why |
| --- | --- | --- | --- |
| Main `Sealed` landing | confirmed | `Keep Top Nav` | Core retail lane. |
| `English Sealed` landing | confirmed | `Keep Homepage Path` | Current local sealed inventory is English-heavy. |
| `Japanese Sealed` landing | not confirmed in current created ledger | `Hide` | Keep ready for use, but do not oversell the lane if inventory is thin. |
| `Booster Box` collections | confirmed | `Keep Homepage Path` | Clear buyer mental model and good browse lane. |
| `Booster Bundle` collections | confirmed | `Keep Homepage Path` | Clear browse lane with current inventory support. |
| `ETB` / `Pokemon Center ETB` collections | confirmed | `Keep Homepage Path` | Strong product-type browse path when inventory is active. |
| `Tin` collections | confirmed | `Keep Homepage Path` | Active inventory lane with clear shopper intent. |
| `Premium Collection` / `Collection Box` pages | confirmed | `Keep Homepage Path` | Useful as secondary browse lanes. |
| `Bundle Combo` page | confirmed as a taxonomy path | `Hide` | Too narrow for prime navigation unless inventory grows. |
| `2-Pack Blister` page | confirmed as a taxonomy path | `Hide` | Valid taxonomy, but not a strong top-level browse lane. |
| Thin set-specific sealed pages | confirmed as possible | `Hide` | Keep only if the set has enough inventory to support a real collection page. |

Section E: Pages To Watch For Removal
-------------------------------------

These are the first types of pages to cut during the Squarespace review:

- thin pages with little or no live inventory
- duplicate collection pages that say the same thing in different taxonomy
  language
- pages that mostly exist because of import/category mechanics
- research-heavy pages that belong on `market.poke6s.com`
- broad brand or story pages that do not help trust or buying

Section F: Live Squarespace Review Checklist
--------------------------------------------

When reviewing the actual site in Squarespace, fill in this table with the
real page list.

| Live Page Title | URL | Type | Current Role | Recommended Decision | Notes |
| --- | --- | --- | --- | --- | --- |
| Homepage | `/` | page | retail entry | `Keep Homepage Path` | Confirm it shows inventory first, not biography first. |
| Singles | `TBD` | collection / page | browse | `Keep Top Nav` | Confirm whether this is a landing page, category page, or both. |
| Sealed | `TBD` | collection / page | browse | `Keep Top Nav` | Confirm whether this is a landing page, category page, or both. |
| Japanese | `TBD` | collection / page | browse | `Keep Top Nav` | Confirm whether live inventory depth justifies equal nav weight today. |
| Resources | `TBD` | page / folder | support | `Keep Top Nav` | Keep lean. |
| Shop on TCGplayer | `TBD` | external link | outbound shop path | `Keep Top Nav` | Confirm link placement and wording. |
| FAQ | `TBD` | page | support | `Move to Footer` | Confirm if it is still useful or needs rewriting. |
| Shipping | `TBD` | page | support | `Move to Footer` | Confirm content is current. |
| Returns | `TBD` | page | support | `Move to Footer` | Confirm policy clarity. |
| Contact | `TBD` | page | support | `Move to Footer` | Confirm trust usefulness. |
| About | `TBD` | page | trust | `Move to Footer` | Keep concise. |

Recommended next execution order
--------------------------------

1. Pull the live Squarespace page list.
2. Fill the `TBD` rows with actual titles and URLs.
3. Mark every current page as `keep`, `move`, `hide`, or `remove`.
4. Confirm whether `Japanese` deserves equal top-nav weight right now or should
   stay in top nav but receive lighter homepage emphasis until inventory grows.
5. Collapse thin singles and sealed collection pages that do not feel alive.
