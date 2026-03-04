import pandas as pd
from pathlib import Path

UNIVERSE = Path(r"F:\Pokemon historical data extracted\top200_universe.csv")
PRODUCTS = Path(r"F:\Pokemon historical data extracted\pokemon_products.csv")
OUT = Path(r"F:\Pokemon historical data extracted\top200_lookup.csv")

u = pd.read_csv(UNIVERSE)
p = pd.read_csv(PRODUCTS)

# normalize column names
if "name" not in p.columns:
    raise SystemExit(f"pokemon_products.csv missing 'name'. Columns: {p.columns.tolist()}")

u["productId"] = u["productId"].astype("int64")
p["productId"] = p["productId"].astype("int64")

lk = u[["productId"]].drop_duplicates().merge(
    p[["productId", "groupId", "name", "cleanName", "imageUrl", "rarity", "number"]],
    on="productId",
    how="left"
)

# standardize to productName for the dashboards
lk = lk.rename(columns={"name": "productName"})

OUT.parent.mkdir(parents=True, exist_ok=True)
lk.to_csv(OUT, index=False)
print("Wrote:", OUT)
print("Lookup rows:", len(lk))
print("Missing names:", (lk["productName"].isna() | (lk["productName"].astype(str).str.strip()=="")).sum())