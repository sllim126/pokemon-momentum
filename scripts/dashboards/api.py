from datetime import timedelta
from pathlib import Path
import sys
import urllib.error

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

DATA_ROOT = Path("/app/data")
EXTRACTED_DIR = DATA_ROOT / "extracted"
PROCESSED_DIR = DATA_ROOT / "processed"
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"
SCRIPT_DIR = Path(__file__).resolve().parent
DASHBOARD_HTML = SCRIPT_DIR / "dashboard.html"
EOD_DASHBOARD_HTML = SCRIPT_DIR / "eod_dashboard.html"
EMBED_DASHBOARD_HTML = SCRIPT_DIR / "embed_dashboard.html"
OUTPUT_DIR = Path("/app/output")
PRODUCTS_CSV = EXTRACTED_DIR / "pokemon_products.csv"
GROUPS_CSV = EXTRACTED_DIR / "pokemon_groups.csv"
OUTPUT_PRODUCTS_CSV = OUTPUT_DIR / "pokemon_products.csv"
OUTPUT_GROUPS_CSV = OUTPUT_DIR / "pokemon_groups.csv"
PARQUET_ROOT = DATA_ROOT / "parquet"
PARQUET_GLOB = str(PARQUET_ROOT / "**/*.parquet")
MS_SCRIPTS_ROOT = Path("/app/MS_Scripts")
if str(MS_SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(MS_SCRIPTS_ROOT))

from processor.utilities.pokemon_eodhistoricaldata_api import EodApi as PokemonEodApi

EOD_API = PokemonEodApi("POKEMON")

PRODUCT_CLASS_SQL = """
CASE
  WHEN COALESCE(NULLIF(p.number, ''), '') <> ''
    OR COALESCE(NULLIF(p.rarity, ''), '') <> ''
    THEN 'card'
  WHEN lower(COALESCE(p.name, '')) LIKE '%ultra-premium collection%'
    OR lower(COALESCE(p.name, '')) LIKE '%ultra premium collection%'
    THEN 'mcap'
  WHEN lower(COALESCE(p.name, '')) LIKE '%booster box%'
    OR lower(COALESCE(p.name, '')) LIKE '%elite trainer box%'
    OR lower(COALESCE(p.name, '')) LIKE '% etb%'
    OR lower(COALESCE(p.name, '')) LIKE 'etb%'
    THEN 'sealed_booster_box'
  WHEN lower(COALESCE(p.name, '')) LIKE '%booster pack%'
    OR lower(COALESCE(p.name, '')) LIKE '%booster bundle%'
    OR lower(COALESCE(p.name, '')) LIKE '%bundle%'
    OR lower(COALESCE(p.name, '')) LIKE '%mini tin%'
    OR lower(COALESCE(p.name, '')) LIKE '% tin%'
    OR lower(COALESCE(p.name, '')) LIKE 'tin%'
    OR lower(COALESCE(p.name, '')) LIKE '%blister case%'
    OR lower(COALESCE(p.name, '')) LIKE '%blister%'
    OR lower(COALESCE(p.name, '')) LIKE '%premium figure collection%'
    OR lower(COALESCE(p.name, '')) LIKE '%figure collection%'
    OR lower(COALESCE(p.name, '')) LIKE '%premium figure set%'
    OR lower(COALESCE(p.name, '')) LIKE '%sleeved%'
    THEN 'sealed_booster_pack'
  ELSE 'other'
END
"""

PRODUCT_KIND_SQL = """
CASE
  WHEN COALESCE(NULLIF(p.number, ''), '') <> ''
    OR COALESCE(NULLIF(p.rarity, ''), '') <> ''
    THEN 'card'
  WHEN lower(COALESCE(p.name, '')) LIKE '%booster box%'
    OR lower(COALESCE(p.name, '')) LIKE '%elite trainer box%'
    OR lower(COALESCE(p.name, '')) LIKE '% etb%'
    OR lower(COALESCE(p.name, '')) LIKE 'etb%'
    OR lower(COALESCE(p.name, '')) LIKE '%booster pack%'
    OR lower(COALESCE(p.name, '')) LIKE '%booster bundle%'
    OR lower(COALESCE(p.name, '')) LIKE '%bundle%'
    OR lower(COALESCE(p.name, '')) LIKE '%mini tin%'
    OR lower(COALESCE(p.name, '')) LIKE '% tin%'
    OR lower(COALESCE(p.name, '')) LIKE 'tin%'
    OR lower(COALESCE(p.name, '')) LIKE '%blister case%'
    OR lower(COALESCE(p.name, '')) LIKE '%blister%'
    OR lower(COALESCE(p.name, '')) LIKE '%premium figure collection%'
    OR lower(COALESCE(p.name, '')) LIKE '%figure collection%'
    OR lower(COALESCE(p.name, '')) LIKE '%premium figure set%'
    OR lower(COALESCE(p.name, '')) LIKE '%sleeved%'
    THEN 'sealed'
  WHEN lower(COALESCE(p.name, '')) LIKE '%ultra-premium collection%'
    OR lower(COALESCE(p.name, '')) LIKE '%ultra premium collection%'
    THEN 'mcap'
  ELSE 'other'
END
"""


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


def to_jsonable(value):
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [to_jsonable(v) for v in value]
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


@app.get("/")
def dashboard():
    return FileResponse(DASHBOARD_HTML)


@app.get("/dashboard")
def dashboard_alias():
    return FileResponse(DASHBOARD_HTML)


@app.get("/dashboard-dev")
def dashboard_dev():
    return FileResponse(DASHBOARD_HTML)


@app.get("/eod-dashboard")
def eod_dashboard():
    return FileResponse(EOD_DASHBOARD_HTML)


@app.get("/embed")
def embed_dashboard():
    return FileResponse(EMBED_DASHBOARD_HTML)


@app.head("/embed")
def embed_dashboard_head():
    return FileResponse(EMBED_DASHBOARD_HTML)


@app.get("/health")
def health():
    cols, rows = q(f"SELECT COUNT(*) AS rows, MAX(date) AS latest FROM {prices_from()}")
    r = dict(zip(cols, rows[0]))
    r["latest"] = str(r["latest"])
    r["source"] = "parquet" if has_parquet() else "duckdb"
    return r


@app.get("/eod/market_details")
def eod_market_details():
    details = EOD_API.get_market_details()
    return details.to_dict()


@app.get("/eod/index_list")
def eod_index_list():
    cols, rows = q(
        f"""
        WITH active_groups AS (
          SELECT DISTINCT groupId
          FROM {prices_from()}
          WHERE categoryId = 3
        )
        SELECT
          CAST(g.groupId AS VARCHAR) AS value,
          COALESCE(g.name, CAST(g.groupId AS VARCHAR)) AS label,
          g.abbreviation
        FROM active_groups a
        JOIN {groups_from()} g
          ON g.groupId = a.groupId
        ORDER BY lower(COALESCE(g.name, CAST(g.groupId AS VARCHAR)))
        """
    )
    items = [dict(zip(cols, row)) for row in rows]
    return {"indexes": items}


@app.get("/eod/index_components")
def eod_index_components(index: str):
    try:
        general, components = EOD_API.get_index_components(index)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except duckdb.Error as exc:
        raise HTTPException(status_code=500, detail=f"EOD index_components failed for {index}: {exc}") from exc
    except urllib.error.HTTPError as exc:
        raise HTTPException(status_code=exc.code or 404, detail=f"Set not found: {index}") from exc
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=f"EOD index_components failed for {index}: {exc}") from exc

    general_rows = general.to_dict(orient="records")
    components_reset = components.reset_index()
    component_rows = components_reset.to_dict(orient="records")
    return {
        "general": general_rows[0] if general_rows else {},
        "components": component_rows,
    }


@app.get("/eod/series")
def eod_series(code: str, days: int = 365):
    days = max(7, min(days, 5000))
    try:
        product = EOD_API.resolve_product(code)
        if product is None:
            raise HTTPException(status_code=404, detail=f"No product found for {code}")
        latest_date = product.get("latest_date")
        if pd.notna(latest_date) and hasattr(latest_date, "date"):
            latest_date = latest_date.date()
        padded_from = None
        if latest_date is not None:
            padded_from = latest_date - timedelta(days=days + 35)
        live = EOD_API.code_live(code, product=product)
        series = EOD_API.code_eod(code, from_date=padded_from, product=product)
        fundamentals = EOD_API.code_fundamentals(code, product=product)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        if isinstance(exc, HTTPException):
            raise
        raise HTTPException(status_code=500, detail=f"EOD series failed for {code}: {exc}") from exc

    if series is None or series.empty:
        raise HTTPException(status_code=404, detail=f"No series found for {code}")

    if days:
        series = series.tail(days)

    rows = []
    prices = pd.to_numeric(series["close"], errors="coerce")
    sma7 = prices.rolling(7).mean()
    sma30 = prices.rolling(30).mean()
    normalized = series.reset_index(drop=True).copy()
    normalized["close"] = prices.reset_index(drop=True)

    for idx, row in normalized.iterrows():
        raw_date = row.get("date")
        if pd.isna(raw_date):
            date_str = ""
        elif hasattr(raw_date, "date"):
            date_str = str(raw_date.date())
        else:
            date_str = str(raw_date)
        rows.append(
            [
                date_str,
                None if pd.isna(row["close"]) else float(row["close"]),
                None if pd.isna(sma7.iloc[idx]) else float(sma7.iloc[idx]),
                None if pd.isna(sma30.iloc[idx]) else float(sma30.iloc[idx]),
            ]
        )

    return {
        "columns": ["date", "close", "sma7", "sma30"],
        "rows": rows,
        "live": to_jsonable(live.to_dict()) if live is not None else {},
        "fundamentals": to_jsonable(fundamentals.to_dict()) if fundamentals is not None else {},
    }


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
      COALESCE(g.name, 'Unknown Group') AS groupName,
      COALESCE(p.name, p.cleanName, 'Product ' || CAST(u.productId AS VARCHAR)) AS productName,
      p.imageUrl,
      p.rarity,
      p.number,
      {PRODUCT_CLASS_SQL} AS productClass,
      {PRODUCT_KIND_SQL} AS productKind
    FROM u
    LEFT JOIN {products_from()} p
      ON p.productId = u.productId
     AND p.groupId = u.groupId
    LEFT JOIN {groups_from()} g
      ON g.groupId = u.groupId
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/groups")
def groups(limit: int = 1000):
    limit = max(1, min(limit, 5000))

    sql = f"""
    WITH active_groups AS (
      SELECT
        groupId,
        COUNT(DISTINCT productId) AS productCount,
        MAX(date) AS latestDate
      FROM {prices_from()}
      WHERE categoryId = 3
      GROUP BY groupId
    )
    SELECT
      ag.groupId,
      COALESCE(g.name, 'Unknown Group') AS groupName,
      g.abbreviation,
      ag.productCount,
      ag.latestDate
    FROM active_groups ag
    LEFT JOIN {groups_from()} g
      ON g.groupId = ag.groupId
    ORDER BY groupName
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/group_products")
def group_products(groupId: int, limit: int = 2000):
    limit = max(1, min(limit, 10000))

    sql = f"""
    WITH latest_date AS (
      SELECT MAX(date) AS latestDate
      FROM {prices_from()}
      WHERE categoryId = 3
        AND groupId = {groupId}
    ),
    latest_prices AS (
      SELECT
        productId,
        groupId,
        subTypeName,
        marketPrice AS latest_price,
        date AS latest_date
      FROM {prices_from()}
      WHERE categoryId = 3
        AND groupId = {groupId}
        AND date = (SELECT latestDate FROM latest_date)
        AND marketPrice IS NOT NULL
    )
    SELECT
      lp.productId,
      lp.groupId,
      COALESCE(g.name, 'Unknown Group') AS groupName,
      COALESCE(p.name, p.cleanName, 'Product ' || CAST(lp.productId AS VARCHAR)) AS productName,
      p.imageUrl,
      p.rarity,
      p.number,
      {PRODUCT_CLASS_SQL} AS productClass,
      {PRODUCT_KIND_SQL} AS productKind,
      lp.subTypeName,
      lp.latest_price,
      lp.latest_date
    FROM latest_prices lp
    LEFT JOIN {products_from()} p
      ON p.productId = lp.productId
     AND p.groupId = lp.groupId
    LEFT JOIN {groups_from()} g
      ON g.groupId = lp.groupId
    ORDER BY
      CASE WHEN p.number IS NULL OR p.number = '' THEN 1 ELSE 0 END,
      p.number,
      productName,
      lp.subTypeName
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/product_signals")
def product_signals(limit: int = 500, min_price: float = 0.0):
    limit = max(1, min(limit, 5000))

    sql = f"""
    SELECT
      latest_date,
      groupId,
      groupName,
      productId,
      productName,
      imageUrl,
      rarity,
      number,
      productClass,
      productKind,
      subTypeName,
      latest_price,
      roc_7d_pct,
      roc_30d_pct,
      roc_90d_pct,
      roc_365d_pct,
      price_vs_sma30_pct,
      price_vs_sma90_pct,
      breakout_90d_flag,
      acceleration_7d_vs_30d,
      trend_score
    FROM product_signal_snapshot
    WHERE latest_price >= {min_price}
    ORDER BY trend_score DESC, roc_30d_pct DESC, latest_price DESC
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/group_signals")
def group_signals(limit: int = 500, min_items: int = 5):
    limit = max(1, min(limit, 5000))
    min_items = max(1, min(min_items, 1000))

    sql = f"""
    SELECT
      latest_date,
      groupId,
      groupName,
      item_count,
      card_count,
      sealed_count,
      avg_30d_pct,
      avg_90d_pct,
      pct_above_sma30,
      pct_above_sma90,
      pct_at_90d_high,
      avg_acceleration_7d_vs_30d,
      sealed_vs_cards_30d_divergence,
      sealed_vs_cards_90d_divergence,
      breadth_score
    FROM group_signal_snapshot
    WHERE item_count >= {min_items}
    ORDER BY breadth_score DESC, avg_30d_pct DESC, pct_above_sma30 DESC, groupName
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
def top_movers(
    days: int = 30,
    limit: int = 200,
    min_prior: float = 5.0,
    min_signal_days: int = 3,
    min_daily_move_pct: float = 1.0,
    require_recent_change: bool = True,
    recent_change_within_days: int = 5,
    product_kind: str | None = None,
):
    product_kind_filter = ""
    if product_kind in {"card", "sealed"}:
        product_kind_filter = f"AND m.productKind = '{product_kind}'"

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
            groupId,
            MAX(CASE WHEN date = (SELECT max_date FROM d) THEN marketPrice END) AS p_now,
            MAX(CASE WHEN date <= (SELECT max_date FROM d) - INTERVAL {days} DAY THEN marketPrice END) AS p_prior
        FROM {prices_from()}
        WHERE categoryId = 3
          AND marketPrice IS NOT NULL
        GROUP BY groupId, productId, subTypeName
    ),
    recent_changes AS (
        SELECT
            groupId,
            productId,
            subTypeName,
            date,
            marketPrice,
            LAG(marketPrice) OVER (
                PARTITION BY groupId, productId, subTypeName
                ORDER BY date
            ) AS prev_price
        FROM {prices_from()}
        WHERE categoryId = 3
          AND marketPrice IS NOT NULL
          AND date >= (SELECT max_date FROM d) - INTERVAL {days + 7} DAY
    ),
    activity AS (
        SELECT
            groupId,
            productId,
            subTypeName,
            COUNT(*) FILTER (
                WHERE prev_price IS NOT NULL
                  AND prev_price > 0
                  AND ((marketPrice / prev_price) - 1) * 100 >= {min_daily_move_pct}
            ) AS signal_days,
            COUNT(*) FILTER (WHERE prev_price IS NOT NULL) AS observed_changes
        FROM recent_changes
        GROUP BY groupId, productId, subTypeName
    ),
    latest_rows AS (
        SELECT
            groupId,
            productId,
            subTypeName,
            date,
            marketPrice,
            ROW_NUMBER() OVER (
                PARTITION BY groupId, productId, subTypeName
                ORDER BY date DESC
            ) AS rn
        FROM {prices_from()}
        WHERE categoryId = 3
          AND marketPrice IS NOT NULL
    ),
    recent_window AS (
        SELECT
            groupId,
            productId,
            subTypeName,
            COUNT(*) FILTER (WHERE rn <= 3) AS recent_points,
            COUNT(DISTINCT marketPrice) FILTER (WHERE rn <= 3) AS recent_distinct_prices
        FROM latest_rows
        GROUP BY groupId, productId, subTypeName
    ),
    recent_activity AS (
        SELECT
            groupId,
            productId,
            subTypeName,
            MAX(date) FILTER (
                WHERE prev_price IS NOT NULL
                  AND marketPrice IS NOT NULL
                  AND prev_price IS NOT NULL
                  AND marketPrice <> prev_price
            ) AS last_change_date
        FROM recent_changes
        GROUP BY groupId, productId, subTypeName
    ),
    metadata AS (
        SELECT
            p.productId,
            p.groupId,
            COALESCE(p.name, p.cleanName, 'Product ' || CAST(p.productId AS VARCHAR)) AS productName,
            p.imageUrl,
            p.rarity,
            p.number,
            {PRODUCT_CLASS_SQL} AS productClass,
            {PRODUCT_KIND_SQL} AS productKind,
            COALESCE(g.name, 'Unknown Group') AS groupName
        FROM {products_from()} p
        LEFT JOIN {groups_from()} g
          ON g.groupId = p.groupId
    )
    SELECT
        b.productId,
        b.subTypeName,
        b.groupId,
        m.groupName,
        m.productName,
        m.imageUrl,
        m.rarity,
        m.number,
        m.productClass,
        m.productKind,
        b.p_now,
        b.p_prior,
        (b.p_now / b.p_prior - 1) * 100 AS roc_pct,
        a.signal_days,
        rw.recent_points,
        rw.recent_distinct_prices,
        ra.last_change_date
    FROM base b
    LEFT JOIN activity a
      ON a.productId = b.productId
     AND a.subTypeName = b.subTypeName
     AND a.groupId = b.groupId
    LEFT JOIN recent_window rw
      ON rw.productId = b.productId
     AND rw.subTypeName = b.subTypeName
     AND rw.groupId = b.groupId
    LEFT JOIN recent_activity ra
      ON ra.productId = b.productId
     AND ra.subTypeName = b.subTypeName
     AND ra.groupId = b.groupId
    LEFT JOIN metadata m
      ON m.productId = b.productId
     AND m.groupId = b.groupId
    WHERE b.p_now IS NOT NULL
      AND b.p_prior IS NOT NULL
      AND b.p_prior >= {min_prior}
      AND COALESCE(a.signal_days, 0) >= {min_signal_days}
      {product_kind_filter}
      AND (
        NOT {1 if require_recent_change else 0}
        OR (
          ra.last_change_date IS NOT NULL
          AND ra.last_change_date >= (SELECT max_date FROM d) - INTERVAL {recent_change_within_days} DAY
        )
      )
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
            groupId,
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
            b.groupId,
            b.subTypeName,
            MAX(b.marketPrice) AS high_n
        FROM base b
        JOIN latest l
          ON b.productId = l.productId
         AND b.groupId = l.groupId
         AND b.subTypeName = l.subTypeName
        WHERE b.date >= l.latest_date - INTERVAL {days} DAY
        GROUP BY b.productId, b.groupId, b.subTypeName
    ),
    metadata AS (
        SELECT
            p.productId,
            p.groupId,
            COALESCE(p.name, p.cleanName, 'Product ' || CAST(p.productId AS VARCHAR)) AS productName,
            p.imageUrl,
            p.rarity,
            p.number,
            COALESCE(g.name, 'Unknown Group') AS groupName
        FROM {products_from()} p
        LEFT JOIN {groups_from()} g
          ON g.groupId = p.groupId
    )
    SELECT
        w.productId,
        w.groupId,
        w.subTypeName,
        m.groupName,
        m.productName,
        m.imageUrl,
        m.rarity,
        m.number,
        w.marketPrice AS latest_price,
        h.high_n AS high_window
    FROM win w
    JOIN hi h
      ON w.productId = h.productId
     AND w.groupId = h.groupId
     AND w.subTypeName = h.subTypeName
    LEFT JOIN metadata m
      ON m.productId = w.productId
     AND m.groupId = w.groupId
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
            groupId,
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
            groupId,
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
            groupId,
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
                PARTITION BY productId, groupId, subTypeName
                ORDER BY date DESC
            ) AS rn
        FROM flagged
    ),
    latest_rows AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            MAX(CASE WHEN rn = 1 THEN price END) AS latest_price,
            MAX(CASE WHEN rn = 1 THEN sma30 END) AS latest_sma30
        FROM recent
        GROUP BY productId, groupId, subTypeName
    ),
    streaks AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            COUNT(*) AS hold_days
        FROM recent
        WHERE rn <= {days_required}
          AND above30 = 1
        GROUP BY productId, groupId, subTypeName
    ),
    last_cross AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            MAX(date) AS cross_date
        FROM (
            SELECT
                productId,
                groupId,
                subTypeName,
                date,
                above30,
                LAG(above30) OVER (
                    PARTITION BY productId, groupId, subTypeName
                    ORDER BY date
                ) AS prev_above30
            FROM flagged
        )
        WHERE above30 = 1
          AND COALESCE(prev_above30, 0) = 0
        GROUP BY productId, groupId, subTypeName
    ),
    metadata AS (
        SELECT
            p.productId,
            p.groupId,
            COALESCE(p.name, p.cleanName, 'Product ' || CAST(p.productId AS VARCHAR)) AS productName,
            p.imageUrl,
            p.rarity,
            p.number,
            COALESCE(g.name, 'Unknown Group') AS groupName
        FROM {products_from()} p
        LEFT JOIN {groups_from()} g
          ON g.groupId = p.groupId
    )
    SELECT
        s.productId,
        s.groupId,
        s.subTypeName,
        m.groupName,
        m.productName,
        m.imageUrl,
        m.rarity,
        m.number,
        c.cross_date,
        s.hold_days,
        l.latest_price,
        l.latest_sma30,
        ((l.latest_price / NULLIF(l.latest_sma30, 0)) - 1) * 100 AS pct_vs_sma30
    FROM streaks s
    JOIN latest_rows l USING (productId, groupId, subTypeName)
    LEFT JOIN last_cross c USING (productId, groupId, subTypeName)
    LEFT JOIN metadata m
      ON m.productId = s.productId
     AND m.groupId = s.groupId
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
            groupId,
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
            groupId,
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
            groupId,
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
                PARTITION BY productId, groupId, subTypeName
                ORDER BY date DESC
            ) AS rn
        FROM flagged
    ),
    latest_rows AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            MAX(CASE WHEN rn = 1 THEN price END) AS latest_price,
            MAX(CASE WHEN rn = 1 THEN sma7 END) AS latest_sma7,
            MAX(CASE WHEN rn = 1 THEN sma30 END) AS latest_sma30
        FROM recent
        GROUP BY productId, groupId, subTypeName
    ),
    streaks AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            COUNT(*) AS bullish_streak
        FROM recent
        WHERE rn <= {days_required}
          AND bullish_day = 1
        GROUP BY productId, groupId, subTypeName
    ),
    metadata AS (
        SELECT
            p.productId,
            p.groupId,
            COALESCE(p.name, p.cleanName, 'Product ' || CAST(p.productId AS VARCHAR)) AS productName,
            p.imageUrl,
            p.rarity,
            p.number,
            COALESCE(g.name, 'Unknown Group') AS groupName
        FROM {products_from()} p
        LEFT JOIN {groups_from()} g
          ON g.groupId = p.groupId
    )
    SELECT
        s.productId,
        s.groupId,
        s.subTypeName,
        m.groupName,
        m.productName,
        m.imageUrl,
        m.rarity,
        m.number,
        s.bullish_streak,
        l.latest_price,
        l.latest_sma7,
        l.latest_sma30,
        ((l.latest_price / NULLIF(l.latest_sma30, 0)) - 1) * 100 AS pct_vs_sma30
    FROM streaks s
    JOIN latest_rows l USING (productId, groupId, subTypeName)
    LEFT JOIN metadata m
      ON m.productId = s.productId
     AND m.groupId = s.groupId
    WHERE s.bullish_streak >= {days_required}
      AND l.latest_sma30 IS NOT NULL
    ORDER BY s.bullish_streak DESC, pct_vs_sma30 DESC
    LIMIT {limit}
    """

    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/early_uptrends")
def early_uptrends(
    days_required: int = 3,
    limit: int = 200,
    min_price: float = 5.0,
    max_price_vs_sma30_pct: float = 15.0,
    min_recent_observations: int = 3,
    recent_change_within_days: int = 5,
):
    days_required = max(1, min(days_required, 15))
    limit = max(1, min(limit, 1000))
    min_recent_observations = max(2, min(min_recent_observations, 10))
    recent_change_within_days = max(1, min(recent_change_within_days, 15))

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
            groupId,
            subTypeName,
            date,
            marketPrice AS price
        FROM {prices_from()}
        WHERE categoryId = 3
          AND marketPrice IS NOT NULL
          AND marketPrice >= {min_price}
    ),
    recent_activity AS (
        SELECT
            groupId,
            productId,
            subTypeName,
            COUNT(*) FILTER (
                WHERE date >= (SELECT max_date FROM d) - INTERVAL 7 DAY
            ) AS recent_observations,
            COUNT(DISTINCT marketPrice) FILTER (
                WHERE date >= (SELECT max_date FROM d) - INTERVAL 7 DAY
            ) AS recent_distinct_prices,
            MAX(CASE
                WHEN prev_price IS NOT NULL AND marketPrice <> prev_price
                THEN date
            END) AS last_change_date
        FROM (
            SELECT
                groupId,
                productId,
                subTypeName,
                date,
                marketPrice,
                LAG(marketPrice) OVER (
                    PARTITION BY groupId, productId, subTypeName
                    ORDER BY date
                ) AS prev_price
            FROM {prices_from()}
            WHERE categoryId = 3
              AND marketPrice IS NOT NULL
              AND marketPrice >= {min_price}
              AND date >= (SELECT max_date FROM d) - INTERVAL 14 DAY
        ) activity
        GROUP BY groupId, productId, subTypeName
    ),
    ma AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            date,
            price,
            AVG(price) OVER (
                PARTITION BY productId, groupId, subTypeName
                ORDER BY date
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ) AS sma3,
            AVG(price) OVER (
                PARTITION BY productId, groupId, subTypeName
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS sma7,
            AVG(price) OVER (
                PARTITION BY productId, groupId, subTypeName
                ORDER BY date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS sma30
        FROM base
    ),
    flagged AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            date,
            price,
            sma3,
            sma7,
            sma30,
            CASE
                WHEN sma30 IS NOT NULL
                 AND sma7 IS NOT NULL
                 AND sma3 IS NOT NULL
                 AND price > sma30
                 AND sma3 > sma7
                 AND sma7 > sma30
                THEN 1 ELSE 0
            END AS early_bullish_day
        FROM ma
    ),
    recent AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY productId, groupId, subTypeName
                ORDER BY date DESC
            ) AS rn
        FROM flagged
    ),
    latest_rows AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            MAX(CASE WHEN rn = 1 THEN price END) AS latest_price,
            MAX(CASE WHEN rn = 1 THEN sma3 END) AS latest_sma3,
            MAX(CASE WHEN rn = 1 THEN sma7 END) AS latest_sma7,
            MAX(CASE WHEN rn = 1 THEN sma30 END) AS latest_sma30
        FROM recent
        GROUP BY productId, groupId, subTypeName
    ),
    streaks AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            COUNT(*) AS early_streak
        FROM recent
        WHERE rn <= {days_required}
          AND early_bullish_day = 1
        GROUP BY productId, groupId, subTypeName
    ),
    metadata AS (
        SELECT
            p.productId,
            p.groupId,
            COALESCE(p.name, p.cleanName, 'Product ' || CAST(p.productId AS VARCHAR)) AS productName,
            p.imageUrl,
            p.rarity,
            p.number,
            COALESCE(g.name, 'Unknown Group') AS groupName
        FROM {products_from()} p
        LEFT JOIN {groups_from()} g
          ON g.groupId = p.groupId
    )
    SELECT
        s.productId,
        s.groupId,
        s.subTypeName,
        m.groupName,
        m.productName,
        m.imageUrl,
        m.rarity,
        m.number,
        s.early_streak,
        ra.recent_observations,
        ra.recent_distinct_prices,
        ra.last_change_date,
        l.latest_price,
        l.latest_sma3,
        l.latest_sma7,
        l.latest_sma30,
        ((l.latest_price / NULLIF(l.latest_sma30, 0)) - 1) * 100 AS pct_vs_sma30
    FROM streaks s
    JOIN latest_rows l USING (productId, groupId, subTypeName)
    JOIN recent_activity ra USING (productId, groupId, subTypeName)
    LEFT JOIN metadata m
      ON m.productId = s.productId
     AND m.groupId = s.groupId
    WHERE s.early_streak >= {days_required}
      AND l.latest_sma30 IS NOT NULL
      AND ra.recent_observations >= {min_recent_observations}
      AND ra.recent_distinct_prices >= 2
      AND ra.last_change_date >= (SELECT max_date FROM d) - INTERVAL {recent_change_within_days} DAY
      AND ((l.latest_price / NULLIF(l.latest_sma30, 0)) - 1) * 100 <= {max_price_vs_sma30_pct}
    ORDER BY s.early_streak DESC, pct_vs_sma30 ASC, l.latest_price DESC
    LIMIT {limit}
    """

    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}
