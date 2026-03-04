import csv

PRICES_CSV = r"F:\Pokemon historical data extracted\pokemon_prices_all_days.csv"
GROUPS_CSV = r"F:\Pokemon historical data extracted\pokemon_groups.csv"
PRODUCTS_CSV = r"F:\Pokemon historical data extracted\pokemon_products.csv"
OUT_CSV = r"F:\Pokemon historical data extracted\pokemon_prices_named.csv"

# groupId -> groupName
groups = {}
with open(GROUPS_CSV, "r", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        groups[int(row["groupId"])] = row["name"]

# productId -> productName
products = {}
with open(PRODUCTS_CSV, "r", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        products[int(row["productId"])] = row["name"]

with open(PRICES_CSV, "r", encoding="utf-8") as fin, open(OUT_CSV, "w", newline="", encoding="utf-8") as fout:
    reader = csv.DictReader(fin)
    fieldnames = reader.fieldnames + ["groupName", "productName"]
    writer = csv.DictWriter(fout, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        gid = int(row["groupId"])
        pid = int(row["productId"])
        row["groupName"] = groups.get(gid, "")
        row["productName"] = products.get(pid, "")
        writer.writerow(row)

print("Wrote:", OUT_CSV)
