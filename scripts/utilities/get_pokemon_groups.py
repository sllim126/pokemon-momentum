import json
import requests
import csv

CATEGORY_ID = 3  # Pokemon
URL = f"https://tcgcsv.com/tcgplayer/{CATEGORY_ID}/groups"

r = requests.get(URL, timeout=60)
r.raise_for_status()
data = r.json()["results"]

out_csv = r"F:\Pokemon historical data extracted\pokemon_groups.csv"

with open(out_csv, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["groupId", "name", "abbreviation", "publishedOn", "modifiedOn", "isSupplemental"])
    for g in data:
        w.writerow([
            g.get("groupId"),
            g.get("name"),
            g.get("abbreviation"),
            g.get("publishedOn"),
            g.get("modifiedOn"),
            g.get("isSupplemental"),
        ])

print("Wrote:", out_csv)
print("Total groups:", len(data))
print("Example:", data[0])
