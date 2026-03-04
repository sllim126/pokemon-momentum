import pandas as pd

# ---- CONFIG ----
INPUT_FILE = r"F:\Pokemon historical data extracted\pokemon_prices_all_days.csv"
OUTPUT_FILE = r"F:\Pokemon historical data extracted\roc_30d.csv"
PRICE_COLUMN = "marketPrice"   # change if needed
# ----------------

print("Loading data...")
df = pd.read_csv(INPUT_FILE)

# Ensure correct dtypes
df["date"] = pd.to_datetime(df["date"])
df[PRICE_COLUMN] = pd.to_numeric(df[PRICE_COLUMN], errors="coerce")

# Drop missing prices
df = df.dropna(subset=[PRICE_COLUMN])

# Identify latest date
latest_date = df["date"].max()
target_date = latest_date - pd.Timedelta(days=30)

print(f"Latest date: {latest_date}")
print(f"Target 30d date: {target_date}")

# Split into current and historical
df_latest = df[df["date"] == latest_date].copy()
df_30d = df[df["date"] == target_date].copy()

# Rename price columns for merge clarity
df_latest = df_latest.rename(columns={PRICE_COLUMN: "current_price"})
df_30d = df_30d.rename(columns={PRICE_COLUMN: "price_30d_ago"})

# Merge on productId + subTypeName
merged = pd.merge(
    df_latest,
    df_30d[["productId", "subTypeName", "price_30d_ago"]],
    on=["productId", "subTypeName"],
    how="left"
)

# Compute ROC
merged["roc_30d_pct"] = (
    (merged["current_price"] / merged["price_30d_ago"] - 1) * 100
)

# Clean up infinite values
merged = merged.replace([float("inf"), float("-inf")], pd.NA)

# Select useful columns
output = merged[[
    "productId",
    "subTypeName",
    "current_price",
    "price_30d_ago",
    "roc_30d_pct"
]]

# Sort by ROC descending
output = output.sort_values(by="roc_30d_pct", ascending=False)

print("Saving output...")
output.to_csv(OUTPUT_FILE, index=False)

print("Done.")