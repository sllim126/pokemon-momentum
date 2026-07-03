# TCG Placeholder Builder

This folder treats `placeholders.csv` as the source of truth. The helper script
does not edit the source file; it reads the CSV, validates expected fields, and
writes generated files to `output/`.

## Current Source Shape

The source CSV has these columns:

- `Release Set`
- `Release Date`
- `Product`
- `Product Type`
- `Card Name`
- `Card Number`
- `Variant`
- `Region`
- `Notes`

## Run

```powershell
python tools/build_placeholders.py
```

To run against the old Excel workbook instead:

```powershell
python tools/build_placeholders.py --input Placeholders.xlsx
```

Generated files:

- `output/placeholders.normalized.csv` - exact row export for spreadsheet review.
- `output/placeholders.json` - structured data for scripts or future apps.
- `output/placeholders_print.html` - standard 2.5in x 3.5in cards, 9 per Letter page.
- `output/index.html` - clickable print dashboard.
- `output/by_release_block/*.html` - print sheets grouped by the `Release Set` column.
- `output/by_card_code/*.html` - print sheets grouped by card-number prefix such as `JTG`, `DRI`, or `MEG`.
- `output/validation_report.md` - row count, columns, duplicates, and missing fields.

Reference files:

- `data/set_release_dates.csv` - set code and release date lookup.
- `docs/set_release_dates.md` - human-readable notes for set dates and special releases.

## Data Safety Rules

- Keep `placeholders.csv` as the only hand-edited source until the workflow is
  stable. `Placeholders.xlsx` can stay as a historical reference.
- Regenerate `output/` whenever the CSV changes.
- Review `validation_report.md` before printing.
- Prefer adding fields as columns instead of mixing extra meaning into one cell.

## Current Print Logic

Each placeholder card is rendered from workbook fields only:

- First line: `Card Name`
- Second line: `Card Number` when present.
- Third line: `Variant`, except `Black Star Promo` is hidden because it is often
  metadata rather than useful placeholder text.
- Product sources are kept in the printable CSV and HTML tooltips, but are not
  printed on the card face. When one card appears in multiple products, the
  print card is collapsed to one placeholder.
- `Region` is shown when it is not the default `US/UK`.

This can be adjusted once the desired placeholder wording is locked in.

## Print Dashboard

Open `output/index.html` in a browser to choose a print mode:

- `Print Everything` opens the full source as one printable run.
- `By Release Block` groups rows by the product/release section, such as
  `Journey Together` or `Black Bolt & White Flare`.
- `By Card Set Code` groups rows by the card number prefix, such as `JTG`,
  `DRI`, `SVP`, or `MEG`. This is better for master-set binder work when a card
  appeared in a later product block.

The same builder works for other CSV files:

```powershell
python tools/build_placeholders.py --input mega_placeholders.csv --extra-input prize_pack_series8.csv --output-dir output_mega
python tools/build_placeholders.py --input swsh_placeholders.csv --output-dir output_swsh
python tools/build_placeholders.py --input prize_pack_series7.csv --output-dir output_prize_pack_series7
```

To build one combined print dashboard that includes SV, Mega-era carryover cards,
and Prize Pack rows grouped back into their original card set codes:

```powershell
python tools/build_placeholders.py --input placeholders.csv --extra-input mega_placeholders.csv --extra-input prize_pack_series7.csv --extra-input prize_pack_series8.csv --output-dir output_combined
```

## Scarlet & Violet Checklists

The checklist builder creates static, hostable pages with browser-saved
checkboxes, printable tables, and CSV downloads. It reads the SV placeholder
data, Mega-era carryover rows, and Prize Pack Series 7/8 by default. Release
block promos such as `SVP` prerelease/staff cards are grouped into the set page
where they were released, while off-set reprints and Prize Pack cards are
grouped back into their printed card-number set code.

```powershell
python tools/build_checklists.py
```

To build the Mega Evolution checklist site, including `MEP` promos and Prize
Pack Series 8 rows:

```powershell
python tools/build_checklists.py --era "Mega Evolution" --input mega_placeholders.csv --input prize_pack_series8.csv --output-dir checklists_mega --overrides data\checklist_mega_overrides.csv --additions data\checklist_mega_additions.csv
```

Generated files:

- `checklists_sv/index.html` - online checklist dashboard.
- `checklists_sv/by_set/*.html` - one interactive and printable checklist per set code.
- `checklists_sv/csv/*.csv` - per-set checklist downloads.
- `checklists_sv/scarlet_violet_checklist_all.csv` - all SV checklist rows in one file.

Mega checklist equivalents are written under `checklists_mega/`, including
`checklists_mega/mega_evolution_checklist_all.csv`.

The current checklist data covers the special rows we have captured so far:
promos, stamped cards, product-exclusive holo variants, prerelease/staff cards,
and Prize Pack stamped cards. Full base-set card lists can be added later as
another CSV source if desired.

### Review And Corrections

Use the generated review queue as the easiest audit worksheet:

- `checklists_sv/review.html` - browser-based review tool for filtering cards,
  preparing corrections, adding missing rows, and downloading updated CSV files.
- `checklists_sv/review_queue.csv` - every current checklist candidate with its
  generated set page and printed card-number set code.
- `data/checklist_overrides.csv` - hand-edited corrections for existing rows.
- `data/checklist_additions.csv` - hand-edited missing rows that are not in the
  source data yet.

The review page is static and can be hosted with the rest of `checklists_sv/`.
It does not write to the server directly; use its download buttons, then replace
the matching CSV in `data/` and rerun `python tools/build_checklists.py`.

To move an existing row, copy its `Card Name`, `Card Number`, `Variant`, and
`Region` into `data/checklist_overrides.csv`, then fill one or more correction
columns:

- `Override Set Code` - moves the card to another set page, such as `PRE`.
- `Override Category` - forces the section, such as `MCAP Cards` or
  `Stamped Cards`.
- `Exclude` - use `yes` to remove a row from checklist pages.
- `Review Notes` - adds an audit note to the generated checklist CSV.

To add a missing card, add it to `data/checklist_additions.csv` using the normal
source columns. If the card number is blank or belongs to another printed set,
use `Override Set Code` so the checklist knows where to place it.

Checklist pages and printable set-code pages use this collector sort order when
the matching rows exist:

1. Illustration Rare
2. Ultra Rare
3. Special Illustration Rare
4. Hyper Rare
5. Stamped Cards
6. Staff Cards
7. Promo Cards
8. MCAP Cards
9. Prize Pack Cards
10. League and Championship Cards
11. Japanese Promos
12. Other Cards

Community shorthand currently maps to the app this way:

- `Master Set` - cards available through normal pack opening, including the
  numbered secret/rare slots when full base-set lists are added.
- `Set Symbol / Set Logo Grandmaster` - cards tied to the set by printed set
  code, expansion logo stamp, retailer stamp, prerelease stamp, Play Pokemon
  stamp, World/Championship stamp, or set-symbol holo treatment.
- `MCAP / Product Variant Layer` - product-exclusive treatments and
  product-packaged variants. This includes holo type variants such as cosmos
  holo and cracked ice, plus non-holo deck exclusives, checklane/blister
  variants, and similar packaged cards.
- `Personal Extended Layer` - league/championship cards and Japanese counterpart
  promos that a collector wants tracked alongside the English set.
