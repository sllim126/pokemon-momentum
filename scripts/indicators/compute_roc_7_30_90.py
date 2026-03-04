import pandas as pd

# ---------------- CONFIG ----------------
INPUT_FILE = r"F:\Pokemon historical data extracted\pokemon_prices_all_days.csv"
OUTPUT_FILE = r"F:\Pokemon historical data extracted\roc_snapshot_7_30_90.csv"

PRICE_COL = "marketPrice"     # change if needed
DATE_COL = "date"
KEY_COLS = ["productId", "subTypeName"]
# ----------------------------------------

def to_num(s):
    return pd.to_numeric(s, errors="coerce")

print("Loading:", INPUT_FILE)
df = pd.read_csv(INPUT_FILE)

df[DATE_COL] = pd.to_datetime(df[DATE_COL])
df[PRICE_COL] = to_num(df[PRICE_COL])
df = df.dropna(subset=[PRICE_COL])

latest_date = df[DATE_COL].max()
print("Latest date:", latest_date.date())

def snapshot_at(date_value: pd.Timestamp) -> pd.DataFrame:
    return df[df[DATE_COL] == date_value][KEY_COLS + [PRICE_COL]].copy()

def roc(cur: pd.Series, past: pd.Series) -> pd.Series:
    return (cur / past - 1.0) * 100.0

def build_roc(days: int) -> pd.DataFrame:
    target_date = latest_date - pd.Timedelta(days=days)
    cur = snapshot_at(latest_date).rename(columns={PRICE_COL: "price_now"})
    past = snapshot_at(target_date).rename(columns={PRICE_COL: f"price_{days}d"})
    out = cur.merge(past[KEY_COLS + [f"price_{days}d"]], on=KEY_COLS, how="left")
    out[f"roc_{days}d_pct"] = roc(out["price_now"], out[f"price_{days}d"])
    out["target_date"] = target_date.date().isoformat()
    return out

roc7 = build_roc(7)
roc30 = build_roc(30)
roc90 = build_roc(90)

# Merge into one snapshot
snap = roc7.merge(roc30[KEY_COLS + ["price_30d", "roc_30d_pct"]], on=KEY_COLS, how="left")
snap = snap.merge(roc90[KEY_COLS + ["price_90d", "roc_90d_pct"]], on=KEY_COLS, how="left")

snap["latest_date"] = latest_date.date().isoformat()

# Nice ordering and sorting
cols = [
    "latest_date",
    "productId", "subTypeName",
    "price_now",
    "price_7d", "roc_7d_pct",
    "price_30d", "roc_30d_pct",
    "price_90d", "roc_90d_pct",
]
snap = snap[cols].sort_values("roc_30d_pct", ascending=False)

print("Writing:", OUTPUT_FILE)
snap.to_csv(OUTPUT_FILE, index=False)
print("Done. Rows:", len(snap))