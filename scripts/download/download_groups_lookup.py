import csv
import requests
from pathlib import Path

CATEGORY_ID = 3
DATA_DIR = Path("/app/data/extracted")
OUT_CSV = DATA_DIR / "pokemon_groups_lookup.csv"

url = f"https://tcgcsv.com/tcgplayer/{CATEGORY_ID}/groups"
r = requests.get(url, timeout=60)
r.raise_for_status()

groups = r.json()["results"]
DATA_DIR.mkdir(parents=True, exist_ok=True)

with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["groupId", "groupName", "abbreviation", "publishedOn", "modifiedOn", "isSupplemental"])
    for g in groups:
        w.writerow([
            g.get("groupId"),
            g.get("name"),
            g.get("abbreviation"),
            g.get("publishedOn"),
            g.get("modifiedOn"),
            g.get("isSupplemental"),
        ])

print("Wrote:", OUT_CSV)
print("Total groups:", len(groups))
