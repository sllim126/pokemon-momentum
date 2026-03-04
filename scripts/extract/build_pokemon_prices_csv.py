import json
import csv
from pathlib import Path

# 1) CHANGE THIS to the folder where you extracted the archive.
# It should contain many date folders like 2024-02-08, 2024-02-09, etc.
ARCHIVE_ROOT = Path(r"C:\Users\ISI\OneDrive - isislc.com\Poke6s\Historical Pricing Data")

CATEGORY_ID = "3"  # Pokemon
OUT_CSV = Path(r"C:\Users\ISI\OneDrive - isislc.com\poke6s\historical pricing data\pokemon_prices_all.csv")

def to_float(x):
    try:
        if x is None or x == "":
            return ""
        return float(x)
    except Exception:
        return ""

rows_written = 0
files_read = 0

with OUT_CSV.open("w", newline="", encoding="utf-8") as f_out:
    writer = csv.writer(f_out)
    writer.writerow([
        "date", "categoryId", "groupId", "productId", "subTypeName",
        "lowPrice", "midPrice", "highPrice", "marketPrice", "directLowPrice"
    ])

    # Each top folder is a date (YYYY-MM-DD)
    for date_dir in sorted([p for p in ARCHIVE_ROOT.iterdir() if p.is_dir()]):
        date_str = date_dir.name

        cat_dir = date_dir / CATEGORY_ID
        if not cat_dir.exists():
            continue

        # Each folder inside /3/ is a groupId
        for group_dir in sorted([p for p in cat_dir.iterdir() if p.is_dir()]):
            group_id = group_dir.name
            prices_file = group_dir / "prices"
            if not prices_file.exists():
                continue

            try:
                data = json.loads(prices_file.read_text(encoding="utf-8", errors="ignore"))
                results = data.get("results", [])
            except Exception as e:
                print(f"FAILED parsing: {prices_file}  Error: {e}")
                continue

            files_read += 1

            for item in results:
                writer.writerow([
                    date_str,
                    CATEGORY_ID,
                    group_id,
                    item.get("productId", ""),
                    item.get("subTypeName", ""),
                    to_float(item.get("lowPrice")),
                    to_float(item.get("midPrice")),
                    to_float(item.get("highPrice")),
                    to_float(item.get("marketPrice")),
                    to_float(item.get("directLowPrice")),
                ])
                rows_written += 1

print("DONE")
print("Price files read:", files_read)
print("Rows written:", rows_written)
print("Output CSV:", OUT_CSV)
