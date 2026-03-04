import json
import csv
from pathlib import Path

# Folder that contains prices-2024-02-08.ppmd, prices-2024-02-09.ppmd, etc.
EXTRACTED_ROOT = Path(r"F:\Pokemon historical data extracted")

CATEGORY_ID = "3"  # Pokemon
OUT_CSV = Path(r"F:\Pokemon historical data extracted\pokemon_prices_all_days.csv")

def to_float(x):
    try:
        if x is None or x == "":
            return ""
        return float(x)
    except Exception:
        return ""

rows_written = 0
files_read = 0
days_found = 0

with OUT_CSV.open("w", newline="", encoding="utf-8") as f_out:
    writer = csv.writer(f_out)
    writer.writerow([
        "date", "categoryId", "groupId", "productId", "subTypeName",
        "lowPrice", "midPrice", "highPrice", "marketPrice", "directLowPrice"
    ])

    # Each folder is like prices-2024-02-08.ppmd
    for day_pkg in sorted([p for p in EXTRACTED_ROOT.iterdir() if p.is_dir()]):
        # Inside is a date folder like 2024-02-08
        date_folders = [p for p in day_pkg.iterdir() if p.is_dir() and p.name[:4].isdigit()]
        if not date_folders:
            continue

        date_dir = date_folders[0]
        date_str = date_dir.name
        days_found += 1

        cat_dir = date_dir / CATEGORY_ID
        if not cat_dir.exists():
            continue

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
print("Days found:", days_found)
print("Price files read:", files_read)
print("Rows written:", rows_written)
print("Output CSV:", OUT_CSV)
