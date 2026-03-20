from datetime import timedelta
from pathlib import Path
import sys
import urllib.error

import duckdb
import pandas as pd
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from scripts.dashboards.query_support import (
    DB_PATH,
    PRODUCT_CLASS_SQL,
    PRODUCT_KIND_SQL,
    build_generation_case,
    build_metadata_cte,
    build_premium_rarity_filter,
    category_config,
    group_signal_from,
    groups_from,
    prices_from,
    product_signal_from,
    products_from,
    q,
    to_jsonable,
)
from scripts.dashboards.tracking_store import (
    create_session,
    create_user,
    delete_session,
    ensure_tracking_schema,
    get_session_user,
    get_tags_for_user,
    get_user_by_username,
    merge_tags,
    set_tag,
    verify_user,
)

SCRIPT_DIR = Path(__file__).resolve().parent
DASHBOARD_HTML = SCRIPT_DIR / "dashboard.html"
ALT_DASHBOARD_HTML = SCRIPT_DIR / "dashboard_lab.html"
SET_EXPLORER_HTML = SCRIPT_DIR / "set_explorer.html"
EOD_DASHBOARD_HTML = SCRIPT_DIR / "eod_dashboard.html"
EMBED_DASHBOARD_HTML = SCRIPT_DIR / "embed_dashboard.html"
DASHBOARD_COMMON_JS = SCRIPT_DIR / "dashboard_common.js"
IMAGE_DIR_CANDIDATES = [
    SCRIPT_DIR.parents[2] / "images",
    Path("/app/images"),
    Path("/opt/pokemon-momentum/images"),
    Path.cwd() / "images",
]
MS_SCRIPTS_ROOT = Path("/app/MS_Scripts")
if str(MS_SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(MS_SCRIPTS_ROOT))

from processor.utilities.pokemon_eodhistoricaldata_api import EodApi as PokemonEodApi

EOD_API = PokemonEodApi("POKEMON")


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

ensure_tracking_schema()


@app.get("/")
def dashboard():
    return FileResponse(DASHBOARD_HTML)


@app.get("/dashboard")
def dashboard_alias():
    return FileResponse(DASHBOARD_HTML)


@app.get("/dashboard-lab")
def dashboard_lab():
    return FileResponse(ALT_DASHBOARD_HTML)


@app.get("/set-explorer")
def set_explorer():
    """Serve the lighter-weight set explorer page used for basket and concentration browsing."""
    return FileResponse(SET_EXPLORER_HTML)


@app.get("/dashboard-dev")
def dashboard_dev():
    return FileResponse(DASHBOARD_HTML)


@app.get("/eod-dashboard")
def eod_dashboard():
    return FileResponse(EOD_DASHBOARD_HTML)


@app.get("/embed")
def embed_dashboard():
    return FileResponse(EMBED_DASHBOARD_HTML)


@app.get("/dashboard-common.js")
def dashboard_common_js():
    return FileResponse(DASHBOARD_COMMON_JS, media_type="application/javascript")


def resolve_image_path(filename: str) -> Path | None:
    for directory in IMAGE_DIR_CANDIDATES:
        path = directory / filename
        if path.exists() and path.is_file():
            return path
    return None


@app.get("/images/{filename}")
def image_asset(filename: str):
    path = resolve_image_path(filename)
    if path is None:
        raise HTTPException(status_code=404, detail="Image not found")
    return FileResponse(path)


@app.head("/images/{filename}")
def image_asset_head(filename: str):
    path = resolve_image_path(filename)
    if path is None:
        raise HTTPException(status_code=404, detail="Image not found")
    return FileResponse(path)


@app.head("/embed")
def embed_dashboard_head():
    return FileResponse(EMBED_DASHBOARD_HTML)


@app.get("/health")
def health(category_id: int = 3):
    """Report the active history source and latest available date for a category."""
    category = category_config(category_id)
    price_source = prices_from(category.category_id)
    cols, rows = q(
        f"""
        SELECT COUNT(*) AS rows, MAX(date) AS latest
        FROM {price_source}
        WHERE categoryId = {category.category_id}
        """
    )
    r = dict(zip(cols, rows[0]))
    r["latest"] = str(r["latest"])
    r["source"] = "parquet" if price_source.startswith("read_parquet(") else "duckdb"
    r["category_id"] = category.category_id
    r["category"] = category.label
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
        JOIN {groups_from(3)} g
          ON g.groupId = a.groupId
        ORDER BY lower(COALESCE(g.name, CAST(g.groupId AS VARCHAR)))
        """
    )
    items = [dict(zip(cols, row)) for row in rows]
    return {"indexes": items}


@app.get("/categories")
def categories():
    """Expose the market/category choices used by the dashboard selectors."""
    return {
        "items": [
            {"category_id": 3, "label": "Pokemon", "slug": "pokemon"},
            {"category_id": 85, "label": "Pokemon Japanese", "slug": "pokemon_jp"},
        ]
    }


def require_tracking_user(authorization: str | None):
    """Resolve a signed-in tracking user from the bearer token used by the dashboard UI."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing tracking session")
    token = authorization.split(" ", 1)[1].strip()
    session_user = get_session_user(token)
    if session_user is None:
        raise HTTPException(status_code=401, detail="Invalid tracking session")
    return session_user


@app.post("/tracking/session")
def tracking_session(payload: dict):
    """Create or resume a lightweight tracking account using username + PIN."""
    username = str(payload.get("username", "")).strip()
    pin = str(payload.get("pin", "")).strip()
    create_if_missing = bool(payload.get("create_if_missing", True))
    if len(username) < 3:
        raise HTTPException(status_code=400, detail="Username must be at least 3 characters")
    if len(pin) < 4:
        raise HTTPException(status_code=400, detail="PIN must be at least 4 characters")

    user = verify_user(username, pin)
    if user is None:
        existing = get_user_by_username(username)
        if existing is not None:
            raise HTTPException(status_code=401, detail="Incorrect PIN")
        if not create_if_missing:
            raise HTTPException(status_code=404, detail="Tracking account not found")
        user_id = create_user(username, pin)
        username_out = username.strip().lower()
    else:
        user_id = int(user["id"])
        username_out = user["username"]

    token = create_session(user_id)
    return {
        "token": token,
        "user": {
            "username": username_out,
        },
    }


@app.get("/tracking/session")
def tracking_session_status(authorization: str | None = Header(default=None)):
    session_user = require_tracking_user(authorization)
    return {"user": {"username": session_user.username}}


@app.delete("/tracking/session")
def tracking_session_delete(authorization: str | None = Header(default=None)):
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
        delete_session(token)
    return {"ok": True}


@app.get("/tracking/tags")
def tracking_tags(authorization: str | None = Header(default=None)):
    session_user = require_tracking_user(authorization)
    return {"items": get_tags_for_user(session_user.user_id)}


@app.put("/tracking/tags")
def tracking_tags_upsert(payload: dict, authorization: str | None = Header(default=None)):
    session_user = require_tracking_user(authorization)
    set_tag(
        user_id=session_user.user_id,
        category_id=int(payload.get("category_id", 3)),
        product_id=int(payload["product_id"]),
        sub_type_name=str(payload.get("sub_type_name", "")),
        tag=str(payload["tag"]),
        enabled=bool(payload.get("enabled", True)),
    )
    return {"ok": True}


@app.post("/tracking/tags/merge")
def tracking_tags_merge(payload: dict, authorization: str | None = Header(default=None)):
    session_user = require_tracking_user(authorization)
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="items must be a list")
    merge_tags(session_user.user_id, items)
    return {"ok": True, "count": len(items)}


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
def universe(limit: int = 5000, category_id: int = 3):
    limit = max(1, min(limit, 50000))
    category = category_config(category_id)
    price_source = prices_from(category.category_id)
    metadata_cte = build_metadata_cte(category.category_id, include_classification=True, cte_name="metadata")

    sql = f"""
    WITH u AS (
      SELECT
        productId,
        subTypeName,
        any_value(groupId) AS groupId
      FROM {price_source}
      WHERE categoryId = {category.category_id}
      GROUP BY productId, subTypeName
    ),
    {metadata_cte}
    SELECT
      u.productId,
      u.subTypeName,
      u.groupId,
      m.groupName,
      m.productName,
      m.imageUrl,
      m.rarity,
      m.number,
      m.productClass,
      m.productKind
    FROM u
    LEFT JOIN metadata m
      ON m.productId = u.productId
     AND m.groupId = u.groupId
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/groups")
def groups(limit: int = 1000, category_id: int = 3):
    limit = max(1, min(limit, 5000))
    category = category_config(category_id)
    price_source = prices_from(category.category_id)

    sql = f"""
    WITH active_groups AS (
      SELECT
        groupId,
        COUNT(DISTINCT productId) AS productCount,
        MAX(date) AS latestDate
      FROM {price_source}
      WHERE categoryId = {category.category_id}
      GROUP BY groupId
    )
    SELECT
      ag.groupId,
      COALESCE(g.name, 'Unknown Group') AS groupName,
      g.abbreviation,
      ag.productCount,
      ag.latestDate
    FROM active_groups ag
    LEFT JOIN {groups_from(category.category_id)} g
      ON g.groupId = ag.groupId
    ORDER BY groupName
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/group_products")
def group_products(groupId: int, limit: int = 2000, category_id: int = 3):
    limit = max(1, min(limit, 10000))
    category = category_config(category_id)
    price_source = prices_from(category.category_id)
    metadata_cte = build_metadata_cte(category.category_id, include_classification=True, cte_name="metadata")

    sql = f"""
    WITH latest_date AS (
      SELECT MAX(date) AS latestDate
      FROM {price_source}
      WHERE categoryId = {category.category_id}
        AND groupId = {groupId}
    ),
    latest_prices AS (
      SELECT
        productId,
        groupId,
        subTypeName,
        marketPrice AS latest_price,
        date AS latest_date
      FROM {price_source}
      WHERE categoryId = {category.category_id}
        AND groupId = {groupId}
        AND date = (SELECT latestDate FROM latest_date)
        AND marketPrice IS NOT NULL
    ),
    {metadata_cte}
    SELECT
      lp.productId,
      lp.groupId,
      m.groupName,
      m.productName,
      m.imageUrl,
      m.rarity,
      m.number,
      m.productClass,
      m.productKind,
      lp.subTypeName,
      lp.latest_price,
      lp.latest_date
    FROM latest_prices lp
    LEFT JOIN metadata m
      ON m.productId = lp.productId
     AND m.groupId = lp.groupId
    ORDER BY
      CASE WHEN m.number IS NULL OR m.number = '' THEN 1 ELSE 0 END,
      m.number,
      productName,
      lp.subTypeName
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/product_signals")
def product_signals(limit: int = 500, min_price: float = 0.0, category_id: int = 3):
    limit = max(1, min(limit, 5000))
    category = category_config(category_id)

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
    FROM {product_signal_from(category.category_id)}
    WHERE latest_price >= {min_price}
    ORDER BY trend_score DESC, roc_30d_pct DESC, latest_price DESC
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/good_buys")
def good_buys(
    limit: int = 250,
    min_price: float = 5.0,
    max_30d_pct: float = 0.0,
    max_7d_pct: float = 5.0,
    category_id: int = 3,
):
    limit = max(1, min(limit, 5000))
    category = category_config(category_id)
    premium_rarity_filter = build_premium_rarity_filter("rarity")

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
      price_vs_sma30_pct,
      trend_score
    FROM {product_signal_from(category.category_id)}
    WHERE latest_price >= {min_price}
      AND productKind = 'card'
      AND {premium_rarity_filter}
      AND COALESCE(roc_30d_pct, 0) <= {max_30d_pct}
      AND COALESCE(roc_7d_pct, 0) <= {max_7d_pct}
    ORDER BY roc_30d_pct ASC, price_vs_sma30_pct ASC, latest_price DESC
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/group_signals")
def group_signals(limit: int = 500, min_items: int = 5, generation: str | None = None, category_id: int = 3):
    limit = max(1, min(limit, 5000))
    min_items = max(1, min(min_items, 1000))
    category = category_config(category_id)
    generation_case = build_generation_case("g.name", "g.abbreviation", "g.publishedOn")
    generation_filter = ""
    if generation:
        safe_generation = generation.replace("'", "''")
        generation_filter = f"AND generation = '{safe_generation}'"

    sql = f"""
    WITH grouped AS (
      SELECT
        gs.latest_date,
        gs.groupId,
        gs.groupName,
        {generation_case} AS generation,
        gs.item_count,
        gs.card_count,
        gs.sealed_count,
        gs.avg_30d_pct,
        gs.avg_90d_pct,
        gs.pct_above_sma30,
        gs.pct_above_sma90,
        gs.pct_at_90d_high,
        gs.avg_acceleration_7d_vs_30d,
        gs.sealed_vs_cards_30d_divergence,
        gs.sealed_vs_cards_90d_divergence,
        gs.breadth_score
      FROM {group_signal_from(category.category_id)} gs
      LEFT JOIN {groups_from(category.category_id)} g
        ON g.groupId = gs.groupId
    )
    SELECT
      latest_date,
      generation,
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
    FROM grouped
    WHERE item_count >= {min_items}
      {generation_filter}
    ORDER BY generation, breadth_score DESC, avg_30d_pct DESC, pct_above_sma30 DESC, groupName
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/group_series")
def group_series(groupId: int, days: int = 365, category_id: int = 3):
    """Return a set-level time series so groups can be graphed like individual products."""
    days = max(30, min(days, 5000))
    category = category_config(category_id)
    price_source = prices_from(category.category_id)

    latest_sql = f"""
        SELECT MAX(date) AS latest
        FROM {price_source}
        WHERE categoryId = {category.category_id}
          AND groupId = {groupId}
          AND marketPrice IS NOT NULL
    """
    cols_l, rows_l = q(latest_sql)
    latest = rows_l[0][0] if rows_l and rows_l[0] else None
    if latest is None:
        raise HTTPException(status_code=404, detail="No rows for that groupId")

    start = latest - timedelta(days=days - 1)
    padded_start = start - timedelta(days=35)

    series_sql = f"""
        WITH raw AS (
            -- Pull the raw price history for every tracked product/subtype inside the set.
            SELECT
                productId,
                subTypeName,
                date,
                marketPrice AS price
            FROM {price_source}
            WHERE categoryId = {category.category_id}
              AND groupId = {groupId}
              AND marketPrice IS NOT NULL
              AND date >= DATE '{padded_start}'
              AND date <= DATE '{latest}'
        ),
        with_ma AS (
            -- Compute each item's SMA30 so daily set breadth can ask "how many products
            -- are above their own medium-term trend?"
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
            FROM raw
        ),
        baseline AS (
            -- Rebase each item to its first in-window price. This creates an equal-weight
            -- set index where a single expensive chase card does not dominate the whole set.
            SELECT
                productId,
                subTypeName,
                price AS base_price
            FROM (
                SELECT
                    productId,
                    subTypeName,
                    price,
                    ROW_NUMBER() OVER (
                        PARTITION BY productId, subTypeName
                        ORDER BY date
                    ) AS rn
                FROM with_ma
                WHERE date >= DATE '{start}'
            )
            WHERE rn = 1
        ),
        windowed AS (
            -- Carry both the rebasing baseline and SMA30 into the final daily aggregation.
            SELECT
                m.productId,
                m.subTypeName,
                m.date,
                m.price,
                m.sma30,
                b.base_price
            FROM with_ma m
            JOIN baseline b
              ON b.productId = m.productId
             AND b.subTypeName = m.subTypeName
            WHERE m.date >= DATE '{start}'
        )
        SELECT
            date,
            -- Equal-weight set index: 100 at the start of the selected window, then the
            -- average rebased move across all active items in the set.
            AVG((price / NULLIF(base_price, 0)) * 100.0) AS equal_weight_index,
            -- Daily breadth: what percent of tracked items were above their own SMA30.
            AVG(CASE WHEN sma30 IS NOT NULL AND price > sma30 THEN 1.0 ELSE 0.0 END) * 100.0 AS pct_above_sma30,
            COUNT(*) AS active_items,
            AVG(price) AS avg_price,
            MEDIAN(price) AS median_price
        FROM windowed
        GROUP BY date
        ORDER BY date
    """
    cols, rows = q(series_sql)
    return {
        "columns": cols,
        "rows": rows,
        "latest": str(latest),
        "start": str(start),
        "groupId": groupId,
        "category_id": category.category_id,
        "category": category.label,
    }


@app.get("/set_baskets")
def set_baskets(limit: int = 500, min_cards: int = 10, category_id: int = 3):
    """Return a lightweight set-completion and concentration view for the fun set explorer page."""
    limit = max(1, min(limit, 2000))
    min_cards = max(1, min(min_cards, 400))
    category = category_config(category_id)
    generation_case = build_generation_case("g.name", "g.abbreviation", "g.publishedOn")

    sql = f"""
    WITH base AS (
        -- Use the latest product snapshot so set explorer questions stay fast and easy to browse.
        SELECT
            s.groupId,
            s.groupName,
            s.productId,
            s.productName,
            s.imageUrl,
            s.rarity,
            s.number,
            s.subTypeName,
            s.latest_price
        FROM {product_signal_from(category.category_id)} s
        WHERE COALESCE(s.productKind, '') = 'card'
          AND s.latest_price IS NOT NULL
          AND s.latest_price > 0
    ),
    ranked AS (
        -- Rank cards inside each set by price so the explorer can show top-hit and top-3 concentration.
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY b.groupId
                ORDER BY b.latest_price DESC, lower(b.productName), lower(COALESCE(b.subTypeName, ''))
            ) AS rn
        FROM base b
    ),
    grouped AS (
        SELECT
            r.groupId,
            MAX(r.groupName) AS groupName,
            COUNT(*) AS card_count,
            SUM(r.latest_price) AS total_set_cost,
            AVG(r.latest_price) AS avg_card_price,
            MEDIAN(r.latest_price) AS median_card_price,
            MAX(CASE WHEN r.rn = 1 THEN r.productName END) AS top_hit_name,
            MAX(CASE WHEN r.rn = 1 THEN r.imageUrl END) AS top_hit_image,
            MAX(CASE WHEN r.rn = 1 THEN r.latest_price END) AS top_hit_price,
            SUM(CASE WHEN r.rn <= 3 THEN r.latest_price ELSE 0 END) AS top3_price
        FROM ranked r
        GROUP BY r.groupId
    )
    SELECT
        a.groupId,
        a.groupName,
        {generation_case} AS generation,
        a.card_count,
        a.total_set_cost,
        a.avg_card_price,
        a.median_card_price,
        a.top_hit_name,
        a.top_hit_image,
        a.top_hit_price,
        (a.top_hit_price / NULLIF(a.total_set_cost, 0)) * 100.0 AS top_hit_share_pct,
        (a.top3_price / NULLIF(a.total_set_cost, 0)) * 100.0 AS top3_share_pct,
        -- Lower concentration means the set's value is spread more broadly across the checklist.
        100.0 - ((a.top3_price / NULLIF(a.total_set_cost, 0)) * 100.0) AS depth_score
    FROM grouped a
    LEFT JOIN {groups_from(category.category_id)} g
      ON g.groupId = a.groupId
    WHERE a.card_count >= {min_cards}
    ORDER BY a.total_set_cost DESC, a.card_count DESC, a.groupName
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {
        "columns": cols,
        "rows": rows,
        "category_id": category.category_id,
        "category": category.label,
        "min_cards": min_cards,
        "limit": limit,
    }


@app.get("/series")
def series(productId: int, subTypeName: str, days: int = 365, category_id: int = 3):
    days = max(7, min(days, 5000))
    st = subTypeName.replace("'", "''")
    category = category_config(category_id)
    price_source = prices_from(category.category_id)

    latest_sql = f"""
        SELECT MAX(date) AS latest
        FROM {price_source}
        WHERE categoryId = {category.category_id}
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
          FROM {price_source}
          WHERE categoryId = {category.category_id}
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
    min_recent_observations: int = 4,
    min_recent_distinct_prices: int = 3,
    recent_variation_window_days: int = 14,
    require_recent_change: bool = True,
    recent_change_within_days: int = 4,
    product_kind: str | None = None,
    category_id: int = 3,
):
    category = category_config(category_id)
    price_source = prices_from(category.category_id)
    product_kind_filter = ""
    if product_kind in {"card", "sealed"}:
        product_kind_filter = f"AND m.productKind = '{product_kind}'"
    metadata_cte = build_metadata_cte(category.category_id, include_classification=True, cte_name="metadata")
    min_recent_observations = max(2, min(min_recent_observations, 30))
    min_recent_distinct_prices = max(2, min(min_recent_distinct_prices, 15))
    recent_variation_window_days = max(3, min(recent_variation_window_days, 30))

    sql = f"""
    WITH d AS (
        SELECT MAX(date) AS max_date
        FROM {price_source}
        WHERE categoryId = {category.category_id}
          AND marketPrice IS NOT NULL
    ),
    base AS (
        SELECT
            productId,
            subTypeName,
            groupId,
            MAX(CASE WHEN date = (SELECT max_date FROM d) THEN marketPrice END) AS p_now,
            MAX(CASE WHEN date <= (SELECT max_date FROM d) - INTERVAL {days} DAY THEN marketPrice END) AS p_prior
        FROM {price_source}
        WHERE categoryId = {category.category_id}
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
        FROM {price_source}
        WHERE categoryId = {category.category_id}
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
    recent_variation AS (
        SELECT
            groupId,
            productId,
            subTypeName,
            COUNT(*) FILTER (
                WHERE date >= (SELECT max_date FROM d) - INTERVAL {recent_variation_window_days} DAY
            ) AS recent_observations,
            COUNT(DISTINCT marketPrice) FILTER (
                WHERE date >= (SELECT max_date FROM d) - INTERVAL {recent_variation_window_days} DAY
            ) AS recent_distinct_prices
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
        FROM {price_source}
        WHERE categoryId = {category.category_id}
          AND marketPrice IS NOT NULL
    ),
    latest_window AS (
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
    {metadata_cte}
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
        rv.recent_observations,
        rv.recent_distinct_prices,
        lw.recent_points,
        ra.last_change_date
    FROM base b
    LEFT JOIN activity a
      ON a.productId = b.productId
     AND a.subTypeName = b.subTypeName
     AND a.groupId = b.groupId
    LEFT JOIN recent_variation rv
      ON rv.productId = b.productId
     AND rv.subTypeName = b.subTypeName
     AND rv.groupId = b.groupId
    LEFT JOIN latest_window lw
      ON lw.productId = b.productId
     AND lw.subTypeName = b.subTypeName
     AND lw.groupId = b.groupId
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
      AND COALESCE(rv.recent_observations, 0) >= {min_recent_observations}
      AND COALESCE(rv.recent_distinct_prices, 0) >= {min_recent_distinct_prices}
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
def breakouts(days: int = 90, limit: int = 200, min_price: float = 5.0, category_id: int = 3):
    category = category_config(category_id)
    price_source = prices_from(category.category_id)
    metadata_cte = build_metadata_cte(category.category_id, cte_name="metadata")
    sql = f"""
    WITH base AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            date,
            marketPrice
        FROM {price_source}
        WHERE categoryId = {category.category_id}
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
    {metadata_cte}
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
def sma30_holds(days_required: int = 7, limit: int = 200, min_price: float = 5.0, category_id: int = 3):
    days_required = max(1, min(days_required, 30))
    limit = max(1, min(limit, 1000))
    category = category_config(category_id)
    price_source = prices_from(category.category_id)
    metadata_cte = build_metadata_cte(category.category_id, cte_name="metadata")

    sql = f"""
    WITH base AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            date,
            marketPrice AS price
        FROM {price_source}
        WHERE categoryId = {category.category_id}
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
    {metadata_cte}
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
def confirmed_uptrends(days_required: int = 5, limit: int = 200, min_price: float = 5.0, category_id: int = 3):
    days_required = max(1, min(days_required, 30))
    limit = max(1, min(limit, 1000))
    category = category_config(category_id)
    price_source = prices_from(category.category_id)
    metadata_cte = build_metadata_cte(category.category_id, cte_name="metadata")

    sql = f"""
    WITH base AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            date,
            marketPrice AS price
        FROM {price_source}
        WHERE categoryId = {category.category_id}
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
    {metadata_cte}
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
    category_id: int = 3,
):
    days_required = max(1, min(days_required, 15))
    limit = max(1, min(limit, 1000))
    min_recent_observations = max(2, min(min_recent_observations, 10))
    recent_change_within_days = max(1, min(recent_change_within_days, 15))
    category = category_config(category_id)
    price_source = prices_from(category.category_id)
    metadata_cte = build_metadata_cte(category.category_id, cte_name="metadata")

    sql = f"""
    WITH d AS (
        SELECT MAX(date) AS max_date
        FROM {price_source}
        WHERE categoryId = {category.category_id}
          AND marketPrice IS NOT NULL
    ),
    base AS (
        SELECT
            productId,
            groupId,
            subTypeName,
            date,
            marketPrice AS price
        FROM {price_source}
        WHERE categoryId = {category.category_id}
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
            FROM {price_source}
            WHERE categoryId = {category.category_id}
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
    {metadata_cte}
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
