Store Category Tree Proposal
============================

Updated: August 7, 2026

Goal
----

Keep the store category tree simpler and more Squarespace-friendly:

- use `categories` for the main browse structure
- use fewer categories per product
- use `tags` for extra detail like `English`, `Pokemon Center`, `Costco`, and `Random Design`

Recommended rule
----------------

For sealed products, default to:

- one product-type category
- optionally one language subcategory where it improves browsing
- optionally one set subcategory when that set is important and already useful to browse

That means most sealed products should land in **2-3 categories max**, not 4-5.

Squarespace import constraint
----------------------------

For Squarespace version 7.1 product CSV imports, the `Categories` field must
match categories and subcategories that already exist on the store page.

If a category path does not already exist, Squarespace may leave the product
with **Categories not assigned** instead of creating the missing category.

So:

1. create the category tree in Squarespace first
2. then import `output/squarespace_sealed_import_update.csv`

Proposed store tree
-------------------

Top-level product categories:

- `Booster Pack`
  - slug: `/booster-pack`
  - child: `English Booster Packs`
    - slug: `/booster-pack/english`
  - child: `Japanese Booster Packs`
    - slug: `/booster-pack/japanese`

- `Booster Bundle`
  - slug: `/booster-bundle`

- `Elite Trainer Box`
  - slug: `/elite-trainer-box`

- `Booster Box`
  - slug: `/booster-box`
  - child: `English Booster Boxes`
    - slug: `/booster-box/english`
  - child: `Japanese Booster Boxes`
    - slug: `/booster-box/japanese`

- `Other`
  - slug: `/other`
  - child: `Collection Boxes`
    - slug: `/other/collection-boxes`
  - child: `Tins`
    - slug: `/other/tins`
  - child: `Other`
    - slug: `/other/other`

Set categories that are worth keeping
-------------------------------------

These are the set-level categories currently used by the sealed import CSV.

- `Mega Evolution`
  - slug: `/mega-evolution`
  - child: `Ascended Heroes`
    - slug: `/mega-evolution/ascended-heroes`
  - child: `Perfect Order`
    - slug: `/mega-evolution/perfect-order`
  - child: `Chaos Rising`
    - slug: `/mega-evolution/chaos-rising`
  - child: `Pitch Black`
    - slug: `/mega-evolution/pitch-black`

- `Scarlet Violet`
  - slug: `/scarlet-violet`
  - child: `SV 151`
    - slug: `/scarlet-violet/sv-151`
  - child: `Black Bolt`
    - slug: `/scarlet-violet/black-bolt`

- `Sword Shield`
  - slug: `/sword-shield`
  - child: `Crown Zenith`
    - slug: `/sword-shield/crown-zenith`

Suggested future additions only if needed
-----------------------------------------

Do not build these unless you actually plan to browse or assign products there
soon.

- `/scarlet-violet/prismatic-evolutions`
- `/scarlet-violet/surging-sparks`
- `/scarlet-violet/white-flare`
- `/mega-evolution/mega-evolution-base`
- `/mega-evolution/phantasmal-flames`
- `/sword-shield/evolving-skies`
- `/sword-shield/vivid-voltage`

Tags
----

Use tags more freely than categories. Good sealed tags include:

- `English`
- `Japanese`
- `Pokemon Center`
- `Costco`
- `Random Design`
- `Crown Zenith`
- `Perfect Order`
- `Chaos Rising`
- `Pitch Black`
- `Ascended Heroes`

Keep tags short, consistent, and searchable.

Avoid using tags that simply duplicate the exact category unless they are useful
for store search or later maintenance.

Categories required by the current sealed import CSV
----------------------------------------------------

The current `output/squarespace_sealed_import_update.csv` uses these category
paths:

- `/booster-box`
- `/booster-box/english`
- `/booster-bundle`
- `/booster-pack`
- `/booster-pack/english`
- `/elite-trainer-box`
- `/other/collection-boxes`
- `/other/tins`
- `/other/other`
- `/mega-evolution/perfect-order`
- `/mega-evolution/ascended-heroes`
- `/mega-evolution/chaos-rising`
- `/mega-evolution/pitch-black`
- `/scarlet-violet/sv-151`
- `/scarlet-violet/black-bolt`
- `/sword-shield/crown-zenith`

Because these are nested paths, Squarespace should also already have the parent
categories:

- `/other`
- `/mega-evolution`
- `/scarlet-violet`
- `/sword-shield`

How the current sealed products map
-----------------------------------

Examples:

- booster boxes:
  - `/booster-box`
  - `/booster-box/english`
  - plus set when relevant, like `/mega-evolution/perfect-order`

- booster bundles:
  - `/booster-bundle`
  - plus set when relevant

- ETBs:
  - `/elite-trainer-box`
  - plus set when relevant

- collection boxes / premium collections:
  - `/other/collection-boxes`
  - plus set when relevant

- tins:
  - `/other/tins`

- Costco combo listings:
  - `/other/other`

Import file
-----------

After the categories above exist in Squarespace, import:

- `output/squarespace_sealed_import_update.csv`
