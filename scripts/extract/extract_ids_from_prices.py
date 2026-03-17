import pandas as pd

DATA_DIR = "/app/data/extracted"
PRICES_CSV = f"{DATA_DIR}/pokemon_prices_all_days.csv"
OUT_GROUP_IDS = f"{DATA_DIR}/group_ids_in_archive.txt"

df = pd.read_csv(PRICES_CSV, usecols=["groupId"])
group_ids = sorted(df["groupId"].dropna().astype(str).unique())

with open(OUT_GROUP_IDS, "w", encoding="utf-8") as f:
    for gid in group_ids:
        f.write(gid + "\n")

print("Groups found:", len(group_ids))
print("Wrote:", OUT_GROUP_IDS)
print("Example groupIds:", group_ids[:10])
