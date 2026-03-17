import csv
import requests
import duckdb
from pathlib import Path

CATEGORY_ID = 3
DATA_DIR = "/app/data/extracted"
OUT_CSV = f"{DATA_DIR}/pokemon_groups.csv"
PROCESSED_DIR = Path("/app/data/processed")
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"
TABLE_NAME = "pokemon_groups"

url = f"https://tcgcsv.com/tcgplayer/{CATEGORY_ID}/groups"
r = requests.get(url, timeout=60)
r.raise_for_status()

groups = r.json()["results"]
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
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
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        groupId BIGINT,
        name VARCHAR,
        abbreviation VARCHAR,
        isSupplemental BOOLEAN,
        publishedOn VARCHAR,
        modifiedOn VARCHAR
    )
    """
)
con.execute(f"DELETE FROM {TABLE_NAME}")
con.executemany(
    f"""
    INSERT INTO {TABLE_NAME}
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

print("Wrote:", OUT_CSV)
print("Groups:", len(groups))
print("DuckDB table:", TABLE_NAME)
print("Database:", DB_PATH)
print("Example:", groups[0])
