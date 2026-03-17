import pandas as pd

# ---------- CONFIG ----------
DATA_DIR = "/app/data/extracted"
PRICES_FILE = f"{DATA_DIR}/pokemon_prices_all_days.csv"
UNIVERSE_FILE = f"{DATA_DIR}/top200_universe.csv"
OUT_FILE = f"{DATA_DIR}/top200_timeseries.csv"

PRICE_COL = "marketPrice"
DATE_COL = "date"
KEY_COLS = ["productId", "subTypeName"]
# ----------------------------

print("Loading prices...")
df = pd.read_csv(PRICES_FILE)

print("Loading top200 universe...")
uni = pd.read_csv(UNIVERSE_FILE)

# Keep only productIds in universe
df = df[df["productId"].isin(uni["productId"])]

df[DATE_COL] = pd.to_datetime(df[DATE_COL])
df[PRICE_COL] = pd.to_numeric(df[PRICE_COL], errors="coerce")

df = df.dropna(subset=[PRICE_COL])

# Sort properly
df = df.sort_values(KEY_COLS + [DATE_COL])

# Compute rolling SMAs
df["sma_7"] = (
    df.groupby(KEY_COLS)[PRICE_COL]
      .transform(lambda x: x.rolling(7).mean())
)

df["sma_30"] = (
    df.groupby(KEY_COLS)[PRICE_COL]
      .transform(lambda x: x.rolling(30).mean())
)

# Rename price column to match dashboard
df = df.rename(columns={PRICE_COL: "price"})

# Keep only required columns
df = df[KEY_COLS + [DATE_COL, "price", "sma_7", "sma_30"]]

print("Saving:", OUT_FILE)
df.to_csv(OUT_FILE, index=False)

print("Done. Rows:", len(df))