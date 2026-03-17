import duckdb
import pandas as pd

EXTRACTED_DIR = "/app/data/extracted"
PROCESSED_DIR = "/app/data/processed"
DB_PATH = f"{PROCESSED_DIR}/prices_db.duckdb"
OUT_PATH = f"{EXTRACTED_DIR}/top200_indicators.csv"
TABLE_NAME = "top200_indicators"

con = duckdb.connect(DB_PATH)

df = con.execute(f"""
WITH universe AS (
    SELECT productId, subTypeName
    FROM top200_universe
),

filtered AS (
    SELECT
        date AS d,
        productId,
        subTypeName,
        marketPrice AS price
    FROM pokemon_prices
    WHERE marketPrice IS NOT NULL
)

SELECT *
FROM filtered
JOIN universe USING(productId, subTypeName)
ORDER BY productId, subTypeName, d
""").fetchdf()

# Compute moving averages grouped
df["sma_7"] = df.groupby(["productId","subTypeName"])["price"].transform(lambda x: x.rolling(7).mean())
df["sma_30"] = df.groupby(["productId","subTypeName"])["price"].transform(lambda x: x.rolling(30).mean())

# Add trend state
def trend_state(row):
    if row["price"] > row["sma_30"] and row["sma_7"] > row["sma_30"]:
        return "Breakout"
    if row["price"] > row["sma_30"]:
        return "Trending"
    if row["price"] < row["sma_7"]:
        return "Cooling"
    return "Weak"

df["trend_state"] = df.apply(trend_state, axis=1)

con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
con.register("top200_indicators_df", df)
con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM top200_indicators_df")
con.unregister("top200_indicators_df")
df.to_csv(OUT_PATH, index=False)
con.close()

print("Wrote:", OUT_PATH)
print("DuckDB table:", TABLE_NAME)
print("Database:", DB_PATH)
