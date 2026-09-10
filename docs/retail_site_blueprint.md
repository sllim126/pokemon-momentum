Retail Site Blueprint
=====================

Updated: August 9, 2026

Goal
----

Turn `poke6s.com` into a clearer retail storefront while keeping
`market.poke6s.com` as the research and discovery product.

This document translates the current strategy into practical website decisions:

- what `poke6s.com` is for
- what belongs in top navigation
- what pages should stay, move, hide, or go away
- what collection pages should exist in Squarespace

Core positioning
----------------

The two domains should do different jobs.

- `poke6s.com`
  - the retail storefront
  - curated inventory
  - trust
  - simple navigation
  - direct purchase conversion

- `market.poke6s.com`
  - the research and discovery tool
  - market screening
  - set exploration
  - sealed deal comparison
  - future collector and research workflows

The handoff between them should be intentional:

- discovery can begin on `market.poke6s.com`
- buying should finish on `poke6s.com`
- only send people to the retail site when an item is actually in stock and purchasable

Retail-site job
---------------

`poke6s.com` should help a visitor do four things quickly:

1. confirm the store is legitimate
2. understand what is actually sold here
3. browse available inventory without friction
4. purchase or click through with confidence

If a page does not help with buying, trust, or understanding the inventory, it
should not get prime navigation space.

Retail-site tone
----------------

The site should feel like:

- "here is the inventory I stand behind"
- curated, not bloated
- knowledgeable, not hype-heavy
- clear and current, not resume-like

Recommended top navigation
--------------------------

Keep top navigation lean.

Recommended top-level nav:

- `Singles`
- `Sealed`
- `Japanese`
- `Resources`
- `Shop on TCGplayer`

Notes:

- `Singles` should earn a permanent slot because that is likely the strongest
  long-term differentiation lane.
- `Sealed` should stay because it is inventory people already understand and
  buy easily.
- `Japanese` should stay as a curated specialty lane, not because it is the
  whole business identity.
- `Resources` should hold only pages that help visitors buy better or trust the
  store more.
- `Shop on TCGplayer` is a useful off-site path and belongs as a clear outbound
  option.

Do not give top-level nav slots to lanes that are too thin to stay active.

Especially avoid:

- `Graded` as a permanent top-level item unless inventory depth supports it
- broad catch-all pages that exist only to sound bigger than the inventory
- market-research pages on the retail domain

Footer / secondary navigation
-----------------------------

Use footer or secondary navigation for lower-priority pages.

Good footer candidates:

- shipping / policies
- contact
- FAQ
- about / trust story
- returns
- condition guide
- links to selected collector resources

These pages matter, but they should not crowd the primary shopping path.

Page review rules
-----------------

Use this keep / cut / hide test for every Squarespace page:

- Keep
  - directly helps a visitor browse inventory, trust the store, or buy

- Move to footer or resources
  - useful, but not part of the main shopping path

- Hide
  - needed for operations, campaigns, or occasional linking, but not general nav

- Remove
  - thin, repetitive, outdated, or not clearly helpful

Page-by-page recommendation framework
-------------------------------------

Apply these decisions during the retail-site cleanup pass.

Keep in top-nav or obvious homepage paths:

- homepage
- singles landing / collection pages
- sealed landing / collection pages
- japanese landing / collection pages
- tcgplayer outbound shop link

Keep, but likely outside top nav:

- FAQ
- shipping / return policies
- contact
- about / store story
- condition explanation for singles if it answers buyer hesitation

Move into `Resources` if they help a buyer understand products:

- language explainer pages
- set or product-type explainers
- buying guides
- collector reference pages that support trust and understanding

Hide from nav but keep live if operationally useful:

- low-stock or temporary campaign pages
- one-off landing pages
- hidden product pages created for intake workflows
- thin category pages that only exist to support filters or imports

Remove or merge:

- sparse pages with little or no inventory behind them
- duplicate pages that say the same thing
- fluff pages that read like branding exercises instead of store help
- pages whose real purpose belongs on `market.poke6s.com`

Homepage recommendation
-----------------------

The homepage should behave like a storefront, not a profile.

Recommended homepage priorities:

1. clear statement of what the store sells
2. visible featured inventory or featured lanes
3. easy path into `Singles`, `Sealed`, and `Japanese`
4. trust signals
5. optional link to the market/research tool as a secondary action

What to avoid on the homepage:

- too much biography
- overexplaining the business before showing inventory
- dead-end brand copy
- too many category choices before the visitor sees products

Recommended collection structure
--------------------------------

The existing store-category and tag docs already point in the right direction.

The retail site should rely on:

- categories for the main browse structure
- tags for language, special traits, and selected set names

Primary browse lanes for the retail site:

- `Singles`
- `Sealed`
- `Japanese`

Within those lanes, category pages should do most of the browse work.

Sealed collection structure
---------------------------

The current proposal in [store_category_tree_proposal.md](/opt/pokemon-momentum/docs/store_category_tree_proposal.md)
fits the retail-site strategy well.

Main sealed browse categories:

- `Booster Pack`
- `Booster Bundle`
- `Elite Trainer Box`
- `Booster Box`
- `Other`

Useful sealed subcategories:

- `English Booster Packs`
- `Japanese Booster Packs`
- `English Booster Boxes`
- `Japanese Booster Boxes`
- `Collection Boxes`
- `Tins`
- `Other`

Set subcategories should stay selective, not exhaustive.

Keep only set lanes that are already active enough to browse, such as:

- `Ascended Heroes`
- `Perfect Order`
- `Chaos Rising`
- `Pitch Black`
- `SV 151`
- `Black Bolt`
- `Crown Zenith`

Do not create or promote extra set collections just to look comprehensive.

Singles collection structure
----------------------------

Singles should be simple at the storefront level.

Recommended first-pass storefront structure:

- a main `Singles` landing page
- a visible `English Singles` lane if inventory depth supports it
- a visible `Japanese Singles` lane if inventory depth supports it
- selected set, trait, or promo pages only when enough inventory exists to make
  the page feel alive

Singles should not start with a cluttered taxonomy. The goal is to help people
reach available cards, not force them through too many filters.

Japanese lane guidance
----------------------

`Japanese` should remain a specialty lane, but not the entire brand identity.

Use it when it helps shoppers quickly find:

- Japanese singles
- Japanese booster boxes
- Japanese booster packs
- selected Japanese set-driven inventory

Do not let `Japanese` become a substitute for a better singles/sealed structure.

Tag guidance
------------

Follow [store_tag_policy.md](/opt/pokemon-momentum/docs/store_tag_policy.md).

In practice:

- categories should do the heavy browse work
- tags should stay operational and searchable
- tags should mostly describe:
  - language
  - special product traits
  - selected set names

Avoid using tags as:

- hype copy
- duplicated product type labels
- a substitute for fixing category structure

What belongs on `market.poke6s.com` instead
-------------------------------------------

Keep these experiences off the retail storefront unless they directly support a
purchase:

- market screeners
- set explorer workflows
- sealed-deal comparison tooling
- index pages
- collection/research dashboards
- future deep hobby analysis tools

Those are valuable, but they belong on `market.poke6s.com`, not in the primary
retail-site navigation.

Decision checklist for the cleanup pass
---------------------------------------

When reviewing each current Squarespace page, ask:

1. Does this page help someone buy now?
2. Does it build trust in a way the homepage or footer does not already cover?
3. Does it clarify what inventory exists?
4. Is there enough inventory or substance here to justify a standalone page?
5. Would this be better as a footer page, resource page, or market-domain page?

If the answer is mostly no, remove it or hide it.

Concrete next pass
------------------

Use this order for the actual retail-site review:

1. inventory all current Squarespace pages
2. mark each as `keep`, `move`, `hide`, or `remove`
3. confirm the final top nav
4. confirm footer/resource pages
5. confirm the collection pages that should exist for:
   - `Singles`
   - `Sealed`
   - `Japanese`
6. clean up category pages with too little inventory to stand alone
7. only after structure is settled, refine homepage copy and visuals

Current recommendation summary
------------------------------

If a fast decision is needed right now, the recommended retail-site shape is:

- top nav:
  - `Singles`
  - `Sealed`
  - `Japanese`
  - `Resources`
  - `Shop on TCGplayer`

- footer:
  - FAQ
  - shipping / returns
  - contact
  - about
  - condition guide

- hide or remove:
  - thin pages
  - duplicate pages
  - research-heavy pages
  - top-level lanes without enough live inventory

- keep `market.poke6s.com` separate as the research product

Related docs
------------

- [store_category_tree_proposal.md](/opt/pokemon-momentum/docs/store_category_tree_proposal.md)
- [store_tag_policy.md](/opt/pokemon-momentum/docs/store_tag_policy.md)
- [todo.txt](/opt/pokemon-momentum/docs/todo.txt)
