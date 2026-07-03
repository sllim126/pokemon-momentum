# Publishing Guide

This project can be published as a static resource. No server-side code is
required for the public checklist and printout pages.

## Recommended Public Files

Upload these files and folders to a public path such as `/tcg-master-set/`:

- `index.html`
- `checklists_sv/`
- `checklists_mega/`
- `output_combined/`
- `output_mega/`
- `docs/set_release_dates.md`

The public experience gives collectors:

- set-by-set printable checklists
- downloadable checklist CSVs
- printable placeholder cards
- promo-only pages such as `SVP` and `MEP`
- comparison reports for promo-list audits

## Recommended Private Files

Keep these private unless you intentionally want to share the source workflow:

- `data/`
- `tools/`
- source CSV files such as `placeholders.csv` and `mega_placeholders.csv`
- PDFs and raw pasted text files

The generated review pages are static and safe to view, but they are better
treated as admin tools because they help produce correction CSVs.

## Update Workflow

1. Edit source CSVs or correction files in `data/`.
2. Regenerate outputs:

```powershell
python tools\build_checklists.py
python tools\build_checklists.py --era "Mega Evolution" --input mega_placeholders.csv --input prize_pack_series8.csv --output-dir checklists_mega --overrides data\checklist_mega_overrides.csv --additions data\checklist_mega_additions.csv
python tools\build_placeholders.py --input placeholders.csv --extra-input mega_placeholders.csv --extra-input prize_pack_series7.csv --extra-input prize_pack_series8.csv --output-dir output_combined
python tools\build_placeholders.py --input mega_placeholders.csv --extra-input prize_pack_series8.csv --output-dir output_mega
```

3. Upload the public files/folders again.

## Suggested Navigation

Use `index.html` as the public landing page. Link to it from your main website
as a collector resource, for example:

- Master set checklist resources
- Promo and variant checklist printouts
- Printable binder placeholder cards

This gives visitors something useful while also keeping the maintenance workflow
separate.
