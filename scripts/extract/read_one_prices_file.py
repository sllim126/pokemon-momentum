import json
import pandas as pd

OUT_CSV = r"C:\Users\ISI\OneDrive - isislc.com\ADAM\Desktop\one_prices_file.csv"

with open(PRICES_FILE, "r", encoding="utf-8", errors="ignore") as f:
    data = json.loads(f.read())

df = pd.DataFrame(data["results"])
df.to_csv(OUT_CSV, index=False)

print("Wrote CSV to:", OUT_CSV)
print("Rows:", len(df))
print("Columns:", list(df.columns))
print(df.head(5))
