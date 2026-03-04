import csv
import requests

PRICES_CSV = r"F:\Pokemon historical data extracted\pokemon_prices_all_days.csv"
OUT_CSV = r"F:\Pokemon historical data extracted\pokemon_products.csv"
CATEGORY_ID = 3

def get_unique_group_ids(prices_csv_path: str) -> list[int]:
    group_ids = set()
    with open(prices_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = row.get("groupId")
            if gid:
                group_ids.add(int(gid))
    return sorted(group_ids)

def fetch_products_for_group(group_id: int) -> list[dict]:
    url = f"https://tcgcsv.com/tcgplayer/{CATEGORY_ID}/{group_id}/products"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json().get("results", [])

group_ids = get_unique_group_ids(PRICES_CSV)
print("Unique groups in your price history:", len(group_ids))

with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["groupId", "productId", "name", "cleanName", "imageUrl", "rarity", "number"])

    for i, gid in enumerate(group_ids, start=1):
        print(f"[{i}/{len(group_ids)}] Downloading products for groupId {gid} ...")
        products = fetch_products_for_group(gid)

        for p in products:
            # extendedData often includes Number and Rarity but format varies by product
            rarity = ""
            number = ""
            for ed in p.get("extendedData", []) or []:
                n = (ed.get("name") or "").lower()
                if n == "rarity":
                    rarity = ed.get("value", "") or ""
                if n in ["number", "collector number", "collectors number", "collector_number"]:
                    number = ed.get("value", "") or ""

            w.writerow([
                gid,
                p.get("productId"),
                p.get("name"),
                p.get("cleanName"),
                p.get("imageUrl"),
                rarity,
                number
            ])

print("Wrote:", OUT_CSV)
