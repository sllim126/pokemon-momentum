import csv
import argparse
import sys
import requests
import duckdb
from pathlib import Path

DATA_DIR = "/app/data/extracted"
PROCESSED_DIR = Path("/app/data/processed")
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.common.category_config import get_category_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export TCG category groups into DuckDB and CSV.")
    parser.add_argument("--category-id", type=int, default=3, help="TCGplayer categoryId to export.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    category = get_category_config(args.category_id)
    out_csv = f"{DATA_DIR}/{category.groups_csv}"
    table_name = category.groups_table

    url = f"https://tcgcsv.com/tcgplayer/{category.category_id}/groups"
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    groups = r.json()["results"]
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["groupId", "name", "abbreviation", "isSupplemental", "publishedOn", "modifiedOn"])
        for g in groups:
            w.writerow([
                g.get("groupId"),
                g.get("name"),
                g.get("abbreviation"),
                g.get("isSupplemental"),
                g.get("publishedOn"),
                g.get("modifiedOn"),
            ])

    con = duckdb.connect(str(DB_PATH))
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            groupId BIGINT,
            name VARCHAR,
            abbreviation VARCHAR,
            isSupplemental BOOLEAN,
            publishedOn VARCHAR,
            modifiedOn VARCHAR
        )
        """
    )
    con.execute(f"DELETE FROM {table_name}")
    con.executemany(
        f"""
        INSERT INTO {table_name}
        (groupId, name, abbreviation, isSupplemental, publishedOn, modifiedOn)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                g.get("groupId"),
                g.get("name"),
                g.get("abbreviation"),
                g.get("isSupplemental"),
                g.get("publishedOn"),
                g.get("modifiedOn"),
            )
            for g in groups
        ],
    )
    con.close()

    print("Category:", category.label, f"({category.category_id})")
    print("Wrote:", out_csv)
    print("Groups:", len(groups))
    print("DuckDB table:", table_name)
    print("Database:", DB_PATH)
    print("Example:", groups[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
