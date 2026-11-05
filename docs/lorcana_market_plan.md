# Lorcana Market Plan (working name "Lorcana6s")

Last reviewed: 2026-11-05

A Disney Lorcana version of the Pokemon market research site: prices, movers, set and card
history, indexes and saved lists. **Research only.** No store, listings, buying or
reselling tools. Start with `AGENTS.md` and `docs/todo.txt`; the work items for this plan
are under "Lorcana Market" in the todo list.

## Decisions so far (owner, 2026-11-05)

- **Internal first.** It runs on the LAN only until the owner decides to publish, like the
  storefront did. A public address comes later: a subdomain such as `lorcana.poke6s.com`,
  or a game-neutral domain if the owner gets one. Code must not hard-code the host name.
- **Scope.** Whatever TCGplayer (through TCGCSV) carries, which in practice means English.
  No Squarespace, storefront, supplier, listing or graded features.
- **Lorcana rules** (rarity tiers, indexes, sealed pack counts): the owner asked for best
  judgement. Those choices are in "Lorcana-specific choices" below and can be changed.
- **Build approach** (agent's recommendation, not objected to): the same codebase handles
  more than one game, rather than a copy of the repo. The pipeline already keys
  everything off a TCGplayer category through `scripts/common/category_config.py` (3 =
  Pokemon English, 85 = Pokemon Japanese). Lorcana is category 71.

## What already exists

| Piece | Where | State |
| --- | --- | --- |
| Price history 2024-02-08 to 2026-11 | `data/raw/prices-*.ppmd.7z` (the TCGCSV daily archive holds every game; folder `<date>/71/`) | On disk, not loaded |
| Daily Lorcana prices since 2026-10-11 | `data/tcgcsv_archive/<date>/tcgcsv-<date>-71.tar.gz` (collect-only job) | Running nightly |
| Set and product lists | `data/tcgcsv_catalogue/71/` | 21 sets, ~4,000 products |
| Card images | TCGplayer `imageUrl` in the product lists, as the Pokemon pages use; higher-resolution images from Lorcast | Free |

Lorcana sets with release dates come from the TCGCSV group list: The First Chapter
(2023-08-18) through Hyperia City (2026-10-16), Into the Inkdark (listed for 2027-02-05),
three Illumineer's Quest sets (Q1-Q3) and three promo groups (D23, Disney100, Promo Cards).

Fields TCGplayer gives for each card: Rarity, InkType (Amber, Amethyst, Emerald, Ruby,
Sapphire, Steel, or two inks joined by `;`), Property (the Disney film, for example
"Frozen"), CardType (Character, Action, Item, Location), Number, Cost Ink, Lore, Strength,
Willpower, Classification and PromoType. Printings (`subTypeName`) are Normal, Cold Foil
and, on a few cards, Holofoil.

## Data sources and their rules

Checked 2026-11-05. **None of the choices below adds a cost.**

| Source | Use | Rules that apply | Decision |
| --- | --- | --- | --- |
| TCGCSV (TCGplayer data) | Prices, products, images | Same etiquette as now: one fetch per file per day, at least 100 ms between requests (code uses 150 ms), stop on 403/429. The FAQ asks people not to build "self-serving" rent-seeking products and treats a free informational site as fine. Attribution isn't required; a thank-you link is appreciated. | **Use.** Lorcana adds about 21 set files a day. Add the TCGCSV thank-you link in the footer. |
| Lorcast (lorcast.com) | Higher-resolution card images; optional extras TCGplayer lacks (keywords, format legality) | Free, no key; asks for 50-100 ms between API requests and bans IPs that flood it. Images are served from `*.lorcast.io`, which those API limits don't cover. States it is not endorsed by Disney or Ravensburger and that it may not charge for Lorcana content. | **Use for sharper card images** (the card page and zoom). Look up image links once per set, weekly at most, and cache the mapping. TCGplayer images stay the default for lists. Data extras aren't needed for phase 1. |
| Scrydex | Card images, prices | The API is paid only: Starter $29/month, Growth $99, Professional $399; no free plan. It does cover Lorcana. | **Don't use the API** (new cost). The Pokemon listing tool loads images only from `images.scrydex.com`, with no account. For Lorcana, use Lorcast's published image links instead. |
| PriceCharting | Graded prices | Same subscription, no extra cost, but the written permission (2026-10-09) was for graded prices on market.poke6s.com. The download script fetches only Pokemon (`CATEGORY_IDS = (3, 85)`). | **Not in scope.** Showing Lorcana graded prices would need a new OK from PriceCharting first. Lorcana grading volume is thin anyway. |
| Ravensburger / Disney | Names, card art | Ravensburger's Community Code (in effect since 2023-05-10) has no fan-site section. It forbids "counterfeit or unauthorized Ravensburger names or products", tied to a Marketing Materials Policy that isn't public. Fan sites such as Lorcast and Lorcana Player use a disclaimer: not published, endorsed or approved by Disney or Ravensburger. | Don't use the Lorcana logo or Ravensburger branding. Put the disclaimer in the footer. Before going public, prefer a brand that doesn't start with the trademark (for example "<brand> — Lorcana prices" on a neutral domain) over "Lorcana6s". |

## Lorcana-specific choices (best judgement; change freely)

**Rarity tiers** used by screeners, filters and indexes:

| Tier | Rarities | Pokemon equivalent |
| --- | --- | --- |
| Chase | Enchanted, Iconic, Epic | Special illustration / hyper rares |
| High | Legendary, Super Rare | Ultra rares |
| Base | Rare, Uncommon, Common | Bulk and near-bulk |
| Promo | Promo (split by PromoType where it matters) | Promos |
| Quest | Quest (Illumineer's Quest cards) | None; shown separately |

Each printing (Normal, Cold Foil, Holofoil) is its own tracked item, as Pokemon reverse
holos are. The Cold Foil printing is often worth much more than the Normal one.

**Research views** replace Pokemon species with:

- Set: same as now.
- Franchise (the `Property` field): "all Frozen cards", like "all Charizard cards".
- Ink colour: six inks plus dual-ink cards.

**Indexes** use the same method as the Pokemon indexes, including leaving out cards whose
price flip-flops:

- **Lorcana 100:** the 100 most valuable English singles.
- **Lorcana Chase:** Enchanted, Iconic and Epic cards only.
- **Lorcana Sealed:** booster boxes, one per set.
- Lorcana has no eras like Pokemon's, so for now indexes go by release year (2023, 2024,
  2025, 2026) rather than era.

**Sealed pack counts**, for price-per-pack. TCGplayer descriptions for Lorcana sealed
products are empty, so these come from general product knowledge. **Each must be checked
against the product page before Sealed Deals goes live for Lorcana**, and kept in a
checklist like `data/sealed_pack_checklist.csv`:

| Product | Packs (to verify) |
| --- | --- |
| Booster Pack / Sleeved Booster Pack | 1 (12 cards) |
| Booster Box | 24 |
| Booster Box Case | 4 boxes = 96 |
| Illumineer's Trove | 8 |
| Starter Deck | 2 (plus a 60-card deck) |
| Sleeved Booster Pack Art Bundle [Set of 3] | 3 |
| Gift Box / Gift Set, Curator's Collection, Wonder/Portfolio Bundle, collector sets | Unknown; leave out until checked |
| Cases of anything (Trove Case, Gift Box Case, Starter Deck Display) | Count the units in the case |

## Phases

**Phase 0: data foundation (no pages).**
1. Add category 71 to `category_config.py` (slug `lorcana`, label "Disney Lorcana").
2. Load Lorcana history from the `data/raw` daily archives into the `lorcana_*` tables and
   Parquet history, with the same loader the Pokemon backfill uses.
3. Add category 71 to the nightly per-set TCGCSV fetch and snapshot builds, inside the
   DuckDB memory caps (`scripts/common/duckdb_settings.py`). Once that's running, take 71
   out of the collect-only job so the same file isn't fetched twice a day.
4. Add the Lorcana rarity tiers and printing handling where the pipeline uses Pokemon
   rarity names.
5. Add Lorcana pass/fail to the nightly Discord summary.

**Phase 1: internal research site (LAN only).**
- The game is chosen by host name, or by a `game` setting for LAN testing, rather than
  separate copies of each page. Pokemon stays the default, and `market.poke6s.com` must
  stay exactly as it is.
- Discover (movers, pullbacks, breakouts), Research (sets, franchises, inks), the card
  page with price history, and Track (saved lists, using the same Google accounts).
- The desktop app first (check at 1920×1080 and 1366×768), then the mobile app.
- Footer: TCGplayer and TCGCSV credit, plus the "not endorsed by Disney or Ravensburger"
  disclaimer.

**Phase 2: indexes and sealed.**
- Lorcana 100, Chase and Sealed indexes.
- Sealed Deals (price-per-pack research only, no buy links) once the pack counts are checked.

**Phase 3: going public (owner decision).**
- Choose the domain and brand, the DNS and proxy route, and whether to add Lorcana to the
  Discord alerts and the Learn pages.

**Out of scope:** storefront, Squarespace, supplier pricing, listings, the budget builders
(they recommend purchases), graded prices, and Japanese or other languages.

## Open questions for the owner

- The brand and domain at Phase 3: subdomain of poke6s.com, or a game-neutral domain.
- Whether to ask Ravensburger directly before going public (their Organized Play contact
  is LorcanaOP@ravensburger.com).
