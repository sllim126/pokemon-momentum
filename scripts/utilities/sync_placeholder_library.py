"""Keep the Placeholder Library up to date with the TCGplayer catalog.

The library (`TCG Placeholders/`, served at /placeholders and in the Collector Hub) is
built from hand-edited CSVs. This job finds what the catalog knows that those CSVs don't,
writes it to generated files next to them, and rebuilds the library:

- **New sets** of the era (for example ME: 30th Celebration, ME06: Delta Reign) are
  appended to `data/set_release_dates.csv` with their code and release date.
- **New promos** (catalog group "ME: Mega Evolution Promo", numbered 001, 002, ...) that are
  missing from `mega_placeholders.csv` are written to `auto_mega_promos.csv`, in the same
  columns. The catalog name usually says where the card came from ("Zapdos - 098 (30th
  Celebration)", "Slowbro - 083 (Pitch Black Stamped)"), which gives the release set and
  the variant. When it doesn't ("(Prerelease)"), the set is taken from the neighbouring
  promo numbers. The catalog never says which product a promo came in (blister, ETB, box),
  so those rows say "Not confirmed yet" until someone checks.
- The library pages are rebuilt with the generated rows included (the same commands as
  `TCG Placeholders/docs/publishing.md`).

To confirm a generated promo: copy its row into `mega_placeholders.csv` with the right
Product and Product Type. The next run sees the number there and drops it from
`auto_mega_promos.csv`. Prize Pack series are still added by hand (their card lists come
as PDFs), as are corrections in `data/checklist_mega_overrides.csv`.

Runs in the site container, which can read the catalog while the site is up:

    docker-compose exec -T pokemon-momentum python scripts/utilities/sync_placeholder_library.py

`--dry-run` reports what would change without writing or rebuilding. `--no-discord` skips
the Discord message, which is otherwise posted only when something new was added.
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LIBRARY = ROOT / "TCG Placeholders"
DB_PATH = ROOT / "data" / "processed" / "prices_db.duckdb"
SET_CATALOG = LIBRARY / "data" / "set_release_dates.csv"

COLUMNS = ["Release Set", "Release Date", "Product", "Product Type", "Card Name", "Card Number", "Variant", "Region", "Notes"]
UNCONFIRMED_PRODUCT = "Not confirmed yet"
UNCONFIRMED_TYPE = "Needs review"

# One entry per era the library covers. Scarlet & Violet is finished, so only Mega is synced.
ERAS = [
    {
        "era": "Mega Evolution",
        "set_prefix": re.compile(r"^ME(\d+|E)?: "),   # "ME05: Pitch Black", "ME: 30th Celebration"
        "promo_group": "ME: Mega Evolution Promo",
        "promo_code": "MEP",
        "skip_sets": {"Mega Evolution Promo", "Mega Evolution Energies"},
        "hand_file": LIBRARY / "mega_placeholders.csv",
        "auto_file": LIBRARY / "auto_mega_promos.csv",
    },
]

# Library rebuild, as in TCG Placeholders/docs/publishing.md plus the generated promo file.
BUILDS = [
    ["tools/build_checklists.py", "--era", "Mega Evolution", "--input", "mega_placeholders.csv",
     "--input", "auto_mega_promos.csv", "--input", "prize_pack_series8.csv", "--output-dir", "checklists_mega",
     "--overrides", "data/checklist_mega_overrides.csv", "--additions", "data/checklist_mega_additions.csv"],
    ["tools/build_placeholders.py", "--input", "placeholders.csv", "--extra-input", "mega_placeholders.csv",
     "--extra-input", "auto_mega_promos.csv", "--extra-input", "prize_pack_series7.csv",
     "--extra-input", "prize_pack_series8.csv", "--output-dir", "output_combined"],
    ["tools/build_placeholders.py", "--input", "mega_placeholders.csv", "--extra-input", "auto_mega_promos.csv",
     "--extra-input", "prize_pack_series8.csv", "--output-dir", "output_mega"],
]


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def catalog(era: dict) -> tuple[dict[str, dict], list[dict]]:
    """The era's sets {short name: {code, release}} and its promo products from the catalog."""
    import duckdb  # only in the site container

    con = duckdb.connect(str(DB_PATH), read_only=True)
    sets = {}
    for name, code, published in con.execute(
        "SELECT name, abbreviation, publishedOn FROM pokemon_groups ORDER BY publishedOn"
    ).fetchall():
        if not era["set_prefix"].match(name or ""):
            continue
        short = era["set_prefix"].sub("", name)
        if short in era["skip_sets"]:
            continue
        sets[short] = {"code": code or "", "release": str(published)[:10]}
    promos = [
        {"name": name, "number": number}
        for name, number in con.execute(
            """
            SELECT p.name, p.number FROM pokemon_products p JOIN pokemon_groups g USING (groupId)
            WHERE g.name = ? AND regexp_matches(coalesce(p.number, ''), '^[0-9]+$')
            ORDER BY CAST(p.number AS INTEGER), p.name
            """,
            [era["promo_group"]],
        ).fetchall()
    ]
    con.close()
    return sets, promos


def promo_number(code: str, value: str) -> int | None:
    match = re.fullmatch(rf"(?:{code})?0*(\d+)", (value or "").strip())
    return int(match.group(1)) if match else None


def describe_promo(name: str, sets: dict[str, dict]) -> tuple[str, str | None, str]:
    """Card name, release set (None if the name doesn't say) and variant from a catalog name.

    "Zapdos - 098 (30th Celebration)"           -> Zapdos, 30th Celebration, Black Star Promo
    "Slowbro - 083 (Pitch Black Stamped)"       -> Slowbro, Pitch Black, Pitch Black Stamp
    "Nidorina - 101 (30th Celebration) (Pokemon Center Exclusive)"
                                                -> Nidorina, 30th Celebration, Pokemon Center Stamp
    "Celebratory Fanfare (Ace Trainer)"         -> Celebratory Fanfare, None, Black Star Promo
    "Miraidon - 082 [Staff]"                    -> Miraidon, None, Staff Stamp
    """
    notes = re.findall(r"\(([^)]*)\)", name)
    card = re.sub(r"\s*(\([^)]*\)|\[[^\]]*\])", "", name)
    card = re.sub(r"\s+-\s+\d+\s*$", "", card).strip()
    release_set, variant = None, "Black Star Promo"
    if re.search(r"\[Staff\]", name, re.I):
        variant = "Staff Stamp"
    for note in notes:
        base = re.sub(r"\s+Stamped$", "", note).strip()
        if base in sets and release_set is None:
            release_set = base
            if note.endswith("Stamped") and variant == "Black Star Promo":
                variant = f"{base} Stamp"
        elif "Pokemon Center" in note and variant == "Black Star Promo":
            variant = "Pokemon Center Stamp"
    return card, release_set, variant


def nearest_set(number: int, known: dict[int, str]) -> str | None:
    """Release set of the closest promo number below (else above) that has one."""
    lower = [n for n in known if n < number]
    if lower:
        return known[max(lower)]
    higher = [n for n in known if n > number]
    return known[min(higher)] if higher else None


def sync_era(era: dict, dry_run: bool) -> dict:
    sets, promos = catalog(era)
    hand_rows = read_csv(era["hand_file"])

    # New sets for the release-date catalog.
    catalog_rows = read_csv(SET_CATALOG)
    known_sets = {row["Set"] for row in catalog_rows}
    new_sets = [
        {"Era": era["era"], "Set": short, "Code": info["code"], "Release Date": info["release"],
         "Notes": f"Added automatically from the TCGplayer catalog on {date.today().isoformat()}"}
        for short, info in sets.items() if short not in known_sets
    ]

    # Promos the hand-edited file doesn't have yet.
    known_numbers = {n for n in (promo_number(era["promo_code"], r.get("Card Number", "")) for r in hand_rows) if n}
    set_by_number = {}
    for row in hand_rows:
        n = promo_number(era["promo_code"], row.get("Card Number", ""))
        if n and row.get("Release Set"):
            set_by_number.setdefault(n, row["Release Set"])
    described = []
    for promo in promos:
        number = int(promo["number"])
        if number in known_numbers:
            continue
        card, release_set, variant = describe_promo(promo["name"], sets)
        described.append((number, promo["name"], card, release_set, variant))
        if release_set:
            set_by_number.setdefault(number, release_set)
    auto_rows, seen = [], set()
    for number, catalog_name, card, release_set, variant in described:
        if (number, variant) in seen:  # the catalog can list the same card twice (e.g. a bundle copy)
            continue
        seen.add((number, variant))
        guessed = release_set is None and number not in set_by_number
        # Another printing of the same number (e.g. the [Staff] copy) shares its set.
        release_set = release_set or set_by_number.get(number) or nearest_set(number, set_by_number) or ""
        note = f"Added automatically from the TCGplayer catalog (\"{catalog_name}\")"
        if guessed:
            note += "; release set guessed from the neighbouring promo numbers"
        auto_rows.append({
            "Release Set": release_set,
            "Release Date": sets.get(release_set, {}).get("release", ""),
            "Product": UNCONFIRMED_PRODUCT,
            "Product Type": UNCONFIRMED_TYPE,
            "Card Name": card,
            "Card Number": f"{era['promo_code']}{number:03d}",
            "Variant": variant,
            "Region": "US/UK",
            "Notes": note + ". Check the product, then move this row to the hand-edited file.",
        })

    previous = {(r["Card Number"], r["Variant"]) for r in read_csv(era["auto_file"])}
    added = [r for r in auto_rows if (r["Card Number"], r["Variant"]) not in previous]
    if not dry_run:
        with era["auto_file"].open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=COLUMNS)
            writer.writeheader()
            writer.writerows(auto_rows)
        if new_sets:
            with SET_CATALOG.open("a", newline="", encoding="utf-8") as handle:
                csv.DictWriter(handle, fieldnames=["Era", "Set", "Code", "Release Date", "Notes"]).writerows(new_sets)
    return {"era": era["era"], "new_sets": new_sets, "auto_rows": auto_rows, "added": added}


def rebuild() -> None:
    for command in BUILDS:
        subprocess.run([sys.executable, *command], cwd=LIBRARY, check=True, stdout=subprocess.DEVNULL)


def discord_message(results: list[dict]) -> str | None:
    """Short note for the owner's channel, or None when nothing new turned up."""
    lines = []
    for result in results:
        for s in result["new_sets"]:
            lines.append(f"New set: {s['Set']} ({s['Code']}, {s['Release Date']})")
        if result["added"]:
            cards = ", ".join(f"{r['Card Number']} {r['Card Name']}" for r in result["added"][:12])
            more = f" and {len(result['added']) - 12} more" if len(result["added"]) > 12 else ""
            lines.append(f"{len(result['added'])} new promo placeholder(s): {cards}{more}")
    if not lines:
        return None
    waiting = sum(len(r["auto_rows"]) for r in results)
    lines.append(f"{waiting} promo(s) still need their product confirmed (TCG Placeholders/auto_mega_promos.csv).")
    return "**Placeholder Library updated**\n" + "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="Report only; write and rebuild nothing.")
    parser.add_argument("--no-discord", action="store_true", help="Don't post the summary to Discord.")
    args = parser.parse_args()

    results = [sync_era(era, args.dry_run) for era in ERAS]
    for result in results:
        print(f"{result['era']}: {len(result['new_sets'])} new set(s), {len(result['auto_rows'])} generated promo row(s), "
              f"{len(result['added'])} new since the last run")
        for s in result["new_sets"]:
            print(f"  set   {s['Set']} ({s['Code']}, {s['Release Date']})")
        for r in result["auto_rows"]:
            print(f"  promo {r['Card Number']} {r['Card Name']} | {r['Release Set']} | {r['Variant']}")
    if args.dry_run:
        return 0
    rebuild()
    print("Library rebuilt.")
    message = discord_message(results)
    if message and not args.no_discord:
        sys.path.insert(0, str(ROOT / "scripts" / "pipeline"))
        from notify_discord import post, setting  # stdlib helpers shared with the nightly report

        url = setting("POKE6S_DISCORD_WEBHOOK_URL")
        if url:
            try:
                post(url, {"content": message})
            except Exception as error:  # a Discord outage must not fail the sync
                print(f"Discord post failed: {error}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
