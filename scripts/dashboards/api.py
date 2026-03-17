from datetime import timedelta
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

DATA_ROOT = Path("/app/data")
EXTRACTED_DIR = DATA_ROOT / "extracted"
PROCESSED_DIR = DATA_ROOT / "processed"
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"
SCRIPT_DIR = Path(__file__).resolve().parent
DASHBOARD_HTML = SCRIPT_DIR / "dashboard.html"
OUTPUT_DIR = Path("/app/output")
PRODUCTS_CSV = EXTRACTED_DIR / "pokemon_products.csv"
GROUPS_CSV = EXTRACTED_DIR / "pokemon_groups.csv"
OUTPUT_PRODUCTS_CSV = OUTPUT_DIR / "pokemon_products.csv"
OUTPUT_GROUPS_CSV = OUTPUT_DIR / "pokemon_groups.csv"
PARQUET_ROOT = DATA_ROOT / "parquet"
PARQUET_GLOB = str(PARQUET_ROOT / "**/*.parquet")


def has_parquet() -> bool:
    return PARQUET_ROOT.exists() and any(PARQUET_ROOT.rglob("*.parquet"))


def prices_from() -> str:
    if has_parquet():
        return f"read_parquet('{PARQUET_GLOB}')"
    return "pokemon_prices"


def db_has_table(name: str) -> bool:
    if not DB_PATH.exists():
        return False
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        return name in tables
    finally:
        con.close()


def first_existing_path(*paths: Path) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def products_from() -> str:
    if db_has_table("pokemon_products"):
        return "pokemon_products"
    csv_path = first_existing_path(PRODUCTS_CSV, OUTPUT_PRODUCTS_CSV)
    if csv_path is None:
        raise HTTPException(status_code=500, detail="pokemon_products metadata not found")
    return f"read_csv_auto('{csv_path}')"


def groups_from() -> str:
    if db_has_table("pokemon_groups"):
        return "pokemon_groups"
    csv_path = first_existing_path(GROUPS_CSV, OUTPUT_GROUPS_CSV)
    if csv_path is None:
        raise HTTPException(status_code=500, detail="pokemon_groups metadata not found")
    return f"read_csv_auto('{csv_path}')"


def get_con():
    if DB_PATH.exists():
        return duckdb.connect(str(DB_PATH), read_only=True)
    return duckdb.connect()


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def q(sql: str, params=None):
    con = get_con()
    try:
        if params is None:
            cur = con.execute(sql)
        else:
            cur = con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return cols, rows
    finally:
        con.close()


@app.get("/")
def dashboard():
    return FileResponse(DASHBOARD_HTML)


@app.get("/dashboard")
def dashboard_alias():
    return FileResponse(DASHBOARD_HTML)


@app.get("/health")
def health():
    cols, rows = q(f"SELECT COUNT(*) AS rows, MAX(date) AS latest FROM {prices_from()}")
    r = dict(zip(cols, rows[0]))
    r["latest"] = str(r["latest"])
    r["source"] = "parquet" if has_parquet() else "duckdb"
    return r


@app.get("/universe")
def universe(limit: int = 5000):
    limit = max(1, min(limit, 50000))

    sql = f"""
    WITH u AS (
      SELECT
        productId,
        subTypeName,
        any_value(groupId) AS groupId
      FROM {prices_from()}
      WHERE categoryId = 3
      GROUP BY productId, subTypeName
    )
    SELECT
      u.productId,
      u.subTypeName,
      u.groupId,
      g.name AS groupName,
      p.name AS productName,
      p.imageUrl,
      p.rarity,
      p.number
    FROM u
    LEFT JOIN {products_from()} p
      ON p.productId = u.productId
    LEFT JOIN {groups_from()} g
      ON g.groupId = u.groupId
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/series")
def series(productId: int, subTypeName: str, days: int = 365):
    days = max(7, min(days, 5000))
    st = subTypeName.replace("'", "''")

    latest_sql = f"""
        SELECT MAX(date) AS latest
        FROM {prices_from()}
        WHERE categoryId = 3
          AND productId = {productId}
          AND subTypeName = '{st}'
    """
    cols_l, rows_l = q(latest_sql)
    latest = rows_l[0][0] if rows_l and rows_l[0] else None
    if latest is None:
        raise HTTPException(status_code=404, detail="No rows for that productId/subTypeName")

    start = latest - timedelta(days=days - 1)

    data_sql = f"""
        WITH s AS (
          SELECT
            date,
            marketPrice AS price
          FROM {prices_from()}
          WHERE categoryId = 3
            AND productId = {productId}
            AND subTypeName = '{st}'
            AND date >= DATE '{start}'
            AND marketPrice IS NOT NULL
          ORDER BY date
        )
        SELECT
          date,
          price,
          AVG(price) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sma7,
          AVG(price) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sma30
        FROM s
        ORDER BY date
    """
    cols, rows = q(data_sql)
    return {"columns": cols, "rows": rows, "latest": str(latest), "start": str(start)}


@app.get("/top_movers")
def top_movers(days: int = 30, limit: int = 200, min_prior: float = 5.0):
    sql = f"""
    WITH d AS (
        SELECT MAX(date) AS max_date
        FROM {prices_from()}
        WHERE categoryId = 3
          AND marketPrice IS NOT NULL
    ),
    base AS (
        SELECT
            productId,
            subTypeName,
            MAX(CASE WHEN date = (SELECT max_date FROM d) THEN marketPrice END) AS p_now,
            MAX(CASE WHEN date <= (SELECT max_date FROM d) - INTERVAL {days} DAY THEN marketPrice END) AS p_prior
        FROM {prices_from()}
        WHERE categoryId = 3
          AND marketPrice IS NOT NULL
        GROUP BY productId, subTypeName
    )
    SELECT
        productId,
        subTypeName,
        p_now,
        p_prior,
        (p_now / p_prior - 1) * 100 AS roc_pct
    FROM base
    WHERE p_now IS NOT NULL
      AND p_prior IS NOT NULL
      AND p_prior >= {min_prior}
    ORDER BY roc_pct DESC
    LIMIT {limit}
    """

    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/breakouts")
def breakouts(days: int = 90, limit: int = 200, min_price: float = 5.0):
    sql = f"""
    WITH base AS (
        SELECT
            productId,
            subTypeName,
            date,
            marketPrice
        FROM {prices_from()}
        WHERE categoryId = 3
          AND marketPrice IS NOT NULL
          AND marketPrice >= {min_price}
    ),
    latest AS (
        SELECT
            productId,
            subTypeName,
            MAX(date) AS latest_date
        FROM base
        GROUP BY productId, subTypeName
    ),
    win AS (
        SELECT
            b.productId,
            b.subTypeName,
            l.latest_date,
            b.marketPrice
        FROM base b
        JOIN latest l
          ON b.productId = l.productId
         AND b.subTypeName = l.subTypeName
         AND b.date = l.latest_date
    ),
    hi AS (
        SELECT
            b.productId,
            b.subTypeName,
            MAX(b.marketPrice) AS high_n
        FROM base b
        JOIN latest l
          ON b.productId = l.productId
         AND b.subTypeName = l.subTypeName
        WHERE b.date >= l.latest_date - INTERVAL {days} DAY
        GROUP BY b.productId, b.subTypeName
    )
    SELECT
        w.productId,
        w.subTypeName,
        w.marketPrice AS latest_price,
        h.high_n AS high_window
    FROM win w
    JOIN hi h
      ON w.productId = h.productId
     AND w.subTypeName = h.subTypeName
    WHERE w.marketPrice >= h.high_n
    ORDER BY w.marketPrice / h.high_n DESC
    LIMIT {limit}
    """

    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/sma30_holds")
def sma30_holds(days_required: int = 7, limit: int = 200, min_price: float = 5.0):
    days_required = max(1, min(days_required, 30))
    limit = max(1, min(limit, 1000))

    sql = f"""
    WITH base AS (
        SELECT
            productId,
            subTypeName,
            date,
            marketPrice AS price
        FROM {prices_from()}
        WHERE categoryId = 3
          AND marketPrice IS NOT NULL
          AND marketPrice >= {min_price}
    ),
    ma AS (
        SELECT
            productId,
            subTypeName,
            date,
            price,
            AVG(price) OVER (
                PARTITION BY productId, subTypeName
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS sma30
        FROM base
    ),
    flagged AS (
        SELECT
            productId,
            subTypeName,
            date,
            price,
            sma30,
            CASE
                WHEN sma30 IS NOT NULL AND price > sma30 THEN 1
                ELSE 0
            END AS above30
        FROM ma
    ),
    recent AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY productId, subTypeName
                ORDER BY date DESC
            ) AS rn
        FROM flagged
    ),
    latest_rows AS (
        SELECT
            productId,
            subTypeName,
            MAX(CASE WHEN rn = 1 THEN price END) AS latest_price,
            MAX(CASE WHEN rn = 1 THEN sma30 END) AS latest_sma30
        FROM recent
        GROUP BY productId, subTypeName
    ),
    streaks AS (
        SELECT
            productId,
            subTypeName,
            COUNT(*) AS hold_days
        FROM recent
        WHERE rn <= {days_required}
          AND above30 = 1
        GROUP BY productId, subTypeName
    ),
    last_cross AS (
        SELECT
            productId,
            subTypeName,
            MAX(date) AS cross_date
        FROM (
            SELECT
                productId,
                subTypeName,
                date,
                above30,
                LAG(above30) OVER (
                    PARTITION BY productId, subTypeName
                    ORDER BY date
                ) AS prev_above30
            FROM flagged
        )
        WHERE above30 = 1
          AND COALESCE(prev_above30, 0) = 0
        GROUP BY productId, subTypeName
    )
    SELECT
        s.productId,
        s.subTypeName,
        c.cross_date,
        s.hold_days,
        l.latest_price,
        l.latest_sma30,
        ((l.latest_price / NULLIF(l.latest_sma30, 0)) - 1) * 100 AS pct_vs_sma30
    FROM streaks s
    JOIN latest_rows l USING (productId, subTypeName)
    LEFT JOIN last_cross c USING (productId, subTypeName)
    WHERE s.hold_days >= {days_required}
      AND l.latest_sma30 IS NOT NULL
    ORDER BY s.hold_days DESC, pct_vs_sma30 DESC
    LIMIT {limit}
    """

    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/confirmed_uptrends")
def confirmed_uptrends(days_required: int = 5, limit: int = 200, min_price: float = 5.0):
    days_required = max(1, min(days_required, 30))
    limit = max(1, min(limit, 1000))

    sql = f"""
    WITH base AS (
        SELECT
            productId,
            subTypeName,
            date,
            marketPrice AS price
        FROM {prices_from()}
        WHERE categoryId = 3
          AND marketPrice IS NOT NULL
          AND marketPrice >= {min_price}
    ),
    ma AS (
        SELECT
            productId,
            subTypeName,
            date,
            price,
            AVG(price) OVER (
                PARTITION BY productId, subTypeName
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS sma7,
            AVG(price) OVER (
                PARTITION BY productId, subTypeName
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS sma30
        FROM base
    ),
    flagged AS (
        SELECT
            productId,
            subTypeName,
            date,
            price,
            sma7,
            sma30,
            CASE
                WHEN sma30 IS NOT NULL
                 AND price > sma30
                 AND sma7 > sma30 THEN 1
                ELSE 0
            END AS bullish_day
        FROM ma
    ),
    recent AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY productId, subTypeName
                ORDER BY date DESC
            ) AS rn
        FROM flagged
    ),
    latest_rows AS (
        SELECT
            productId,
            subTypeName,
            MAX(CASE WHEN rn = 1 THEN price END) AS latest_price,
            MAX(CASE WHEN rn = 1 THEN sma7 END) AS latest_sma7,
            MAX(CASE WHEN rn = 1 THEN sma30 END) AS latest_sma30
        FROM recent
        GROUP BY productId, subTypeName
    ),
    streaks AS (
        SELECT
            productId,
            subTypeName,
            COUNT(*) AS bullish_streak
        FROM recent
        WHERE rn <= {days_required}
          AND bullish_day = 1
        GROUP BY productId, subTypeName
    )
    SELECT
        s.productId,
        s.subTypeName,
        s.bullish_streak,
        l.latest_price,
        l.latest_sma7,
        l.latest_sma30,
        ((l.latest_price / NULLIF(l.latest_sma30, 0)) - 1) * 100 AS pct_vs_sma30
    FROM streaks s
    JOIN latest_rows l USING (productId, subTypeName)
    WHERE s.bullish_streak >= {days_required}
      AND l.latest_sma30 IS NOT NULL
    ORDER BY s.bullish_streak DESC, pct_vs_sma30 DESC
    LIMIT {limit}
    """

    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}
