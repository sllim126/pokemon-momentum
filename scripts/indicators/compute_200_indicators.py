import duckdb
import pandas as pd

DATA_PATH = r"F:\Pokemon historical data extracted\pokemon_prices_all_days.csv"
UNIVERSE_PATH = r"F:\Pokemon historical data extracted\top200_universe.csv"
OUT_PATH = r"F:\Pokemon historical data extracted\top200_indicators.csv"

con = duckdb.connect()

df = con.execute(f"""
WITH universe AS (
    SELECT productId, subTypeName
    FROM read_csv_auto('{UNIVERSE_PATH}')
),

filtered AS (
    SELECT
        CAST(date AS DATE) AS d,
        productId,
        subTypeName,
        CAST(marketPrice AS DOUBLE) AS price
    FROM read_csv_auto('{DATA_PATH}', ignore_errors=true)
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

df.to_csv(OUT_PATH, index=False)
print("Wrote:", OUT_PATH)