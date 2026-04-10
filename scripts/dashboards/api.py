import csv
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import unquote
import sys
import urllib.error

import duckdb
import pandas as pd
import requests
from fastapi import Cookie, FastAPI, File, Header, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from scripts.build_store_price_targets import build_target_rows

from scripts.dashboards.query_support import (
    DB_PATH,
    PRODUCT_CLASS_SQL,
    PRODUCT_KIND_SQL,
    build_generation_case,
    build_metadata_cte,
    build_premium_rarity_filter,
    build_set_basket_filter,
    category_config,
    group_signal_from,
    health_snapshot_from,
    groups_from,
    prices_from,
    product_signal_from,
    products_from,
    q,
    series_snapshot_from,
    sparkline_snapshot_from,
    to_jsonable,
)
from scripts.dashboards.tracking_store import (
    create_session,
    create_bug_report,
    create_google_user,
    create_user,
    delete_user,
    delete_session,
    ensure_tracking_schema,
    get_session_user,
    get_tags_for_user,
    get_user_by_username,
    list_bug_reports,
    merge_tags,
    set_tag,
    verify_user,
)

SCRIPT_DIR = Path(__file__).resolve().parent
DASHBOARD_HTML = SCRIPT_DIR / "dashboard.html"
ALT_DASHBOARD_HTML = SCRIPT_DIR / "dashboard_lab.html"
SET_EXPLORER_HTML = SCRIPT_DIR / "set_explorer.html"
SEALED_DEALS_HTML = SCRIPT_DIR / "sealed_deals.html"
ACCOUNT_SETTINGS_HTML = SCRIPT_DIR / "account_settings.html"
EOD_DASHBOARD_HTML = SCRIPT_DIR / "eod_dashboard.html"
EMBED_DASHBOARD_HTML = SCRIPT_DIR / "embed_dashboard.html"
BUG_REPORTS_HTML = SCRIPT_DIR / "bug_reports.html"
PRICING_UPLOAD_HTML = SCRIPT_DIR / "pricing_upload.html"
SUPPLIER_PRICING_HTML = SCRIPT_DIR / "supplier_pricing.html"
SUPPLIER_PROFITABILITY_HTML = SCRIPT_DIR / "supplier_profitability.html"
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
ADMIN_USERNAMES = {
    username.strip().lower()
    for username in os.getenv("POKEMON_MOMENTUM_ADMIN_USERS", "sllim126").split(",")
    if username.strip()
}
GOOGLE_CLIENT_ID = os.getenv("POKEMON_MOMENTUM_GOOGLE_CLIENT_ID", "").strip()


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


@app.get("/sealed-deals")
def sealed_deals_page():
    """Serve a standalone sealed-product value tracker page (not linked from the main dashboard)."""
    return FileResponse(SEALED_DEALS_HTML)


@app.get("/account-settings")
def account_settings():
    return FileResponse(ACCOUNT_SETTINGS_HTML)


@app.get("/dashboard-dev")
def dashboard_dev():
    return FileResponse(DASHBOARD_HTML)


@app.get("/eod-dashboard")
def eod_dashboard(authorization: str | None = Header(default=None), tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token")):
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
    return FileResponse(EOD_DASHBOARD_HTML)


@app.get("/embed")
def embed_dashboard(authorization: str | None = Header(default=None), tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token")):
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
    return FileResponse(EMBED_DASHBOARD_HTML)


@app.get("/bug-reports")
def bug_reports_page(authorization: str | None = Header(default=None), tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token")):
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
    return FileResponse(BUG_REPORTS_HTML)


@app.get("/pricing-upload")
def pricing_upload_page(authorization: str | None = Header(default=None), tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token")):
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
    return FileResponse(PRICING_UPLOAD_HTML)


@app.get("/supplier-pricing")
def supplier_pricing_page(authorization: str | None = Header(default=None), tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token")):
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
    return FileResponse(SUPPLIER_PRICING_HTML)


@app.get("/supplier-profitability")
def supplier_profitability_page(authorization: str | None = Header(default=None), tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token")):
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
    return FileResponse(SUPPLIER_PROFITABILITY_HTML)


@app.get("/dashboard-common.js")
def dashboard_common_js():
    return FileResponse(DASHBOARD_COMMON_JS, media_type="application/javascript")


def resolve_image_path(filename: str) -> Path | None:
    relative = Path(unquote(filename))
    if relative.is_absolute() or ".." in relative.parts:
        return None
    for directory in IMAGE_DIR_CANDIDATES:
        path = (directory / relative).resolve()
        try:
            path.relative_to(directory.resolve())
        except ValueError:
            continue
        if path.exists() and path.is_file():
            return path
    return None


@app.get("/images/{filename:path}")
def image_asset(filename: str):
    path = resolve_image_path(filename)
    if path is None:
        raise HTTPException(status_code=404, detail="Image not found")
    return FileResponse(path)


@app.head("/images/{filename:path}")
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
        SELECT rows, latest
        FROM {health_snapshot_from(category.category_id)}
        """
    )
    r = dict(zip(cols, rows[0]))
    r["latest"] = str(r["latest"])
    r["source"] = "parquet" if price_source.startswith("read_parquet(") else "duckdb"
    r["category_id"] = category.category_id
    r["category"] = category.label
    return r


@app.get("/eod/market_details")
def eod_market_details(authorization: str | None = Header(default=None), tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token")):
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
    details = EOD_API.get_market_details()
    return details.to_dict()


@app.get("/eod/index_list")
def eod_index_list(authorization: str | None = Header(default=None), tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token")):
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
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


def _product_signal_source_resilient(category_id: int) -> str:
    """Resolve product-signal source with a CSV fallback when DuckDB metadata locks occur."""
    try:
        return product_signal_from(category_id)
    except Exception as err:
        message = str(err).lower()
        if "could not set lock on file" not in message and "conflicting lock is held" not in message:
            raise
        category = category_config(category_id)
        csv_candidates = [
            Path("/app/data/extracted") / category.product_signal_csv,
            Path("/app/output") / category.product_signal_csv,
            Path("/opt/pokemon-momentum/data/extracted") / category.product_signal_csv,
            Path("/opt/pokemon-momentum/output") / category.product_signal_csv,
            SCRIPT_DIR.parents[2] / "data" / "extracted" / category.product_signal_csv,
            SCRIPT_DIR.parents[2] / "output" / category.product_signal_csv,
        ]
        for path in csv_candidates:
            if path.exists():
                return f"read_csv_auto('{path}')"
        raise


def _query_with_in_memory_duckdb(sql: str):
    """Execute a query without touching the on-disk DB file (used as lock fallback)."""
    con = duckdb.connect()
    try:
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return cols, rows
    finally:
        con.close()


_SEALED_EXCLUDE_TOKENS = (
    "code card",
    "digital bundle",
    "digital",
    "online",
    "ptcgo",
)

_SEALED_EXCLUDE_NAME_PATTERNS = (
    "battle deck",
    "league battle deck",
    "v battle deck",
    "ex battle deck",
    "mega battle deck",
    "battle academy",
)

_SEALED_PACK_COMPOSITION_OVERRIDES = [
    {
        "match": ["mega charizard x ex ultra premium collection"],
        "packs": [
            {"set": "Destined Rivals", "count": 4},
            {"set": "Journey Together", "count": 4},
            {"set": "Phantasmal Flames", "count": 4},
            {"set": "Mega Evolution", "count": 4},
            {"set": "Surging Sparks", "count": 2},
        ],
        "product_type": "Ultra Premium Collection",
    },
    {
        "match": ["team rocket's moltres ex ultra-premium collection"],
        "packs": [
            {"set": "Destined Rivals", "count": 2},
            {"set": "Journey Together", "count": 4},
            {"set": "Temporal Forces", "count": 2},
            {"set": "Paradox Rift", "count": 2},
            {"set": "Obsidian Flames", "count": 3},
            {"set": "Paldea Evolved", "count": 3},
            {"set": "Scarlet and Violet", "count": 2},
        ],
        "product_type": "Ultra Premium Collection",
    },
]

_SEALED_PACK_COUNT_OVERRIDES_BY_PRODUCT_ID = {
    # Booster box / ETB / bundle case counts provided by user.
    248124: {"pack_count": 216},
    190325: {"pack_count": 3},
    513409: {"pack_count": 360},
    635609: {"pack_count": 260},
    247655: {"pack_count": 216},
    506640: {"pack_count": 90},
    528030: {"pack_count": 44},
    530142: {"pack_count": 216},
    278793: {"pack_count": 216},
    243722: {"pack_count": 80},
    530105: {"pack_count": 150},
    609238: {"pack_count": 120},
    624678: {"pack_count": 216},
    256145: {"pack_count": 216},
    283391: {"pack_count": 216},
    236259: {"pack_count": 216},
    515970: {"pack_count": 64},
    530700: {"pack_count": 90},
    453471: {"pack_count": 100},
    256271: {"pack_count": 80},
    496905: {"pack_count": 216},
    496131: {"pack_count": 80},
    600697: {"pack_count": 44},
    655281: {"pack_count": 216},
    454377: {
        "pack_count": 64,
        "packs": [{"set": "SWSH Random", "count": 64}],
        "product_type": "Ultra Premium Collection Case",
    },
    614449: {"pack_count": 216},
    580708: {"pack_count": 216},
    628398: {"pack_count": 90},
    628396: {"pack_count": 1, "product_type": "Pack Blister"},
    628395: {"pack_count": 1, "product_type": "Pack Blister"},
    648588: {"pack_count": 9},
    646039: {"pack_count": 18, "product_type": "Half Booster Box"},
    649413: {"pack_count": 18, "product_type": "Half Booster Box"},
    649421: {"pack_count": 18, "product_type": "Half Booster Box"},
    541171: {"pack_count": 3, "product_type": "Tin"},
    587368: {"pack_count": 3, "product_type": "Tin"},
    558713: {"pack_count": 35, "product_type": "Mini Pack Bundle", "msrp_total": 14.99},
    591147: {"pack_count": 3, "product_type": "Tin"},
    280302: {"pack_count": 40, "product_type": "Mini Pack Bundle"},
    591145: {"pack_count": 3, "product_type": "Tin"},
    591146: {"pack_count": 3, "product_type": "Tin"},
    562354: {"pack_count": 5, "product_type": "Tin"},
    562357: {"pack_count": 5, "product_type": "Tin"},
    562356: {"pack_count": 5, "product_type": "Tin"},
    544241: {"pack_count": 5, "product_type": "Tin"},
    636740: {"pack_count": 8},
    475646: {"pack_count": 11},
    210566: {"pack_count": 4},
    616737: {"pack_count": 7},
    475647: {"pack_count": 11},
    644731: {"pack_count": 216},
    591211: {"pack_count": 120, "product_type": "Mini Pack Bundle"},
    566954: {"pack_count": 80, "product_type": "Mini Pack Bundle"},
    # Mixed sealed layouts supplied by user notes.
    518638: {
        "pack_count": 25,
        "packs": [
            {"set": "SWSH Random", "count": 8},
            {"set": "Celebrations (4-card packs)", "count": 17},
        ],
        "product_type": "Ultra Premium Collection",
    },
    251895: {
        "pack_count": 150,
        "packs": [
            {"set": "Celebrations (4-card packs)", "count": 100},
            {"set": "SWSH Random", "count": 50},
        ],
        "product_type": "Elite Trainer Box Case",
    },
}

_SEALED_PACK_COUNT_OVERRIDES_BY_NAME = [
    {
        "match": ["pokemon center", "elite trainer box", "scarlet", "violet"],
        "pack_count": 11,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["elite trainer box", "scarlet", "violet"],
        "pack_count": 9,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "elite trainer box", "mega"],
        "pack_count": 11,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["elite trainer box", "mega"],
        "pack_count": 9,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "elite trainer box", "crown zenith"],
        "pack_count": 12,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["elite trainer box", "crown zenith"],
        "pack_count": 10,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "elite trainer box", "celestial storm"],
        "pack_count": 8,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["elite trainer box", "celestial storm"],
        "pack_count": 6,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "elite trainer box", "brilliant stars"],
        "pack_count": 10,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["elite trainer box", "brilliant stars"],
        "pack_count": 8,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "elite trainer box", "fusion strike"],
        "pack_count": 8,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["elite trainer box", "fusion strike"],
        "pack_count": 6,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "elite trainer box", "pokemon go"],
        "pack_count": 12,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["elite trainer box", "pokemon go"],
        "pack_count": 10,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "elite trainer box", "hidden fates"],
        "pack_count": 10,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["elite trainer box", "hidden fates"],
        "pack_count": 8,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "elite trainer box", "cosmic eclipse"],
        "pack_count": 8,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["elite trainer box", "cosmic eclipse"],
        "pack_count": 6,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "elite trainer box", "lost origin"],
        "pack_count": 10,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["elite trainer box", "lost origin"],
        "pack_count": 8,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "elite trainer box", "astral radiance"],
        "pack_count": 10,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["elite trainer box", "astral radiance"],
        "pack_count": 8,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "elite trainer box", "silver tempest"],
        "pack_count": 10,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["elite trainer box", "silver tempest"],
        "pack_count": 8,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "sun", "moon", "elite trainer box", "lunala"],
        "pack_count": 8,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["sun", "moon", "elite trainer box", "lunala"],
        "pack_count": 6,
        "product_type": "Elite Trainer Box",
    },
    {
        "match": ["pokemon center", "sun", "moon", "elite trainer box", "solgaleo"],
        "pack_count": 8,
        "product_type": "Pokemon Center ETB",
    },
    {
        "match": ["sun", "moon", "elite trainer box", "solgaleo"],
        "pack_count": 6,
        "product_type": "Elite Trainer Box",
    },
]

# JP supplier list (box-level JPY) provided by user; MSRP baseline uses +25% uplift.
# JP MSRP tiers (box configuration + yen/pack):
# - Regular sets: 30 packs/box at ¥180 per pack
# - High class sets: 10 packs/box at ¥550 per pack
# - 151: 20 packs/box at ¥290 per pack
_JP_HIGH_CLASS_SET_KEYS = (
    "terastal festival",
    "vstar universe",
    "shiny treasures",
)
_JPY_TO_USD_RATE = 1.0 / 159.48


def _safe_float(value) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:  # NaN
        return None
    return parsed


def _should_exclude_sealed_name(name_raw: str) -> bool:
    lower = (name_raw or "").lower()
    if any(token in lower for token in _SEALED_EXCLUDE_TOKENS):
        return True
    if any(pattern in lower for pattern in _SEALED_EXCLUDE_NAME_PATTERNS):
        return True
    return False


def _is_trick_or_trade_product(name_raw: str) -> bool:
    lower = (name_raw or "").lower()
    return "trick or trade" in lower or "booster bundle trick or trade" in lower


def _parse_bundle_multiplier(name: str) -> int:
    patterns = (
        r"\[\s*set of\s*(\d+)\s*\]",
        r"\[\s*bundle of\s*(\d+)\s*\]",
        r"\bset of\s*(\d+)\b",
        r"\bbundle of\s*(\d+)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, name)
        if match:
            return max(1, int(match.group(1)))
    return 1


def _find_pack_composition_override(name_raw: str) -> dict | None:
    lower = (name_raw or "").lower()
    for override in _SEALED_PACK_COMPOSITION_OVERRIDES:
        tokens = [token.lower() for token in override.get("match", [])]
        if "case" in lower and "case" not in tokens:
            continue
        if tokens and all(token in lower for token in tokens):
            return override
    return None


def _find_pack_count_override(product_id: int | None) -> dict | None:
    if product_id is None:
        return None
    return _SEALED_PACK_COUNT_OVERRIDES_BY_PRODUCT_ID.get(int(product_id))


def _find_pack_count_override_by_name(name_raw: str) -> dict | None:
    lower = (name_raw or "").lower()
    for override in _SEALED_PACK_COUNT_OVERRIDES_BY_NAME:
        tokens = [token.lower() for token in override.get("match", [])]
        if tokens and all(token in lower for token in tokens):
            return override
    return None


def _normalize_match_text(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()
    return re.sub(r"\s+", " ", text)


def _jp_pricing_profile(name_raw: str, group_name_raw: str) -> tuple[int, float]:
    haystack = _normalize_match_text(f"{name_raw} {group_name_raw}")
    if re.search(r"\b151\b", haystack):
        return 20, 290.0
    if "high class" in haystack:
        return 10, 550.0
    if any(_normalize_match_text(key) in haystack for key in _JP_HIGH_CLASS_SET_KEYS):
        return 10, 550.0
    return 30, 180.0


def _pack_mix_text(packs: list[dict]) -> str:
    if not packs:
        return ""
    return ", ".join(
        f"{int(pack.get('count', 0))}x {str(pack.get('set') or 'Unknown Set')}"
        for pack in packs
        if int(pack.get("count", 0)) > 0
    )


def _looks_like_individual_card(name_raw: str, rarity: str | None, number: str | None) -> bool:
    name = (name_raw or "").lower()
    number_text = (number or "").strip()
    rarity_text = (rarity or "").strip()
    # Card-like numbering signatures.
    if re.search(r"\b\d{1,3}\s*/\s*\d{1,3}\b", name):
        return True
    if re.search(r"\bgg\d{1,3}\b", name) or re.search(r"\btg\d{1,3}\b", name):
        return True
    # Metadata fields present on cards should usually be absent on sealed products.
    if number_text:
        return True
    if rarity_text:
        return True
    return False


def _infer_pack_count(
    name_raw: str,
    product_class: str,
    category_id: int | None = None,
    group_name_raw: str = "",
) -> int | None:
    name = (name_raw or "").lower()
    multiplier = _parse_bundle_multiplier(name)
    is_japanese_market = int(category_id or 0) == 85
    jp_packs_per_box, _ = _jp_pricing_profile(name_raw, group_name_raw) if is_japanese_market else (36, 0.0)

    if _is_trick_or_trade_product(name_raw):
        return 35 * multiplier

    if "ultra premium collection case" in name:
        return 16 * 6 * multiplier
    if "booster box case" in name or ("booster box" in name and "case" in name):
        packs_per_box = jp_packs_per_box if is_japanese_market else 36
        return packs_per_box * 6 * multiplier
    if "half booster box" in name:
        return 18 * multiplier
    if "elite trainer box case" in name:
        return 9 * 10 * multiplier
    if "build & battle stadium" in name or "build and battle stadium" in name:
        return 12 * multiplier
    if "pokemon center elite trainer box" in name:
        return 11 * multiplier
    if "elite trainer box" in name or " etb" in name or name.startswith("etb "):
        return 9 * multiplier
    if "booster box" in name:
        packs_per_box = jp_packs_per_box if is_japanese_market else 36
        return packs_per_box * multiplier
    if "ultra premium collection" in name:
        return 16 * multiplier
    if "booster bundle" in name:
        return 6 * multiplier
    if "build & battle box" in name or "build and battle box" in name:
        return 4 * multiplier

    # Avoid treating set numbers like "151 Mini Tin" as tin-counts.
    # Count mini tins only when quantity is explicit as a pack/bundle/set indicator.
    mini_tin_count = re.search(r"\bmini\s*tins?\s*(\d{1,2})\s*[- ]?pack\b", name)
    if mini_tin_count:
        return max(1, int(mini_tin_count.group(1))) * 2 * multiplier
    mini_tin_count = re.search(r"\b(\d{1,2})\s*mini\s*tins?\s*(?:pack|bundle|set)?\b", name)
    if mini_tin_count and any(token in name for token in ("pack", "bundle", "set of", "set")):
        return max(1, int(mini_tin_count.group(1))) * 2 * multiplier
    if re.search(r"\bmini\s*tin[s]?\b", name):
        return 2 * multiplier

    pack_blister = re.search(r"(\d+)\s*pack\s*blister", name)
    if pack_blister:
        return max(1, int(pack_blister.group(1))) * multiplier
    if "checklane blister" in name:
        return 1 * multiplier
    if "single pack blister" in name:
        return 1 * multiplier
    if "sleeved booster pack" in name:
        return 1 * multiplier
    if "booster pack" in name:
        return 1 * multiplier

    if re.search(r"\bstacking\s*tin[s]?\b", name):
        return 3 * multiplier
    if re.search(r"\bpoke\s*ball\s*tin[s]?\b", name) or re.search(r"\bpokeball\s*tin[s]?\b", name):
        return 3 * multiplier
    if re.search(r"\btin[s]?\b", name):
        return 4 * multiplier
    if "collection" in name or "figure collection" in name:
        return 5 * multiplier

    if product_class == "sealed_booster_box":
        packs_per_box = jp_packs_per_box if is_japanese_market else 36
        return packs_per_box * multiplier
    if product_class == "sealed_booster_pack":
        return 1 * multiplier

    return None


def _infer_retail_per_pack(name_raw: str, product_class: str, product_type: str | None = None) -> float:
    name = (name_raw or "").lower()
    normalized_type = str(product_type or "").lower()

    if _is_trick_or_trade_product(name_raw):
        return 0.0

    if normalized_type == "mini pack bundle":
        return 0.0
    if "booster box" in name:
        return 4.49
    if "booster bundle" in name:
        return 4.49
    if "ultra premium collection" in name:
        return 6.25
    if "single pack blister" in name:
        return 4.99
    if re.search(r"\b\d+\s*pack\s*blister\b", name):
        return 4.99
    if "mini tin" in name:
        return 5.00
    if "pokemon center elite trainer box" in name:
        return 5.45
    if "elite trainer box" in name or " etb" in name or name.startswith("etb "):
        return 5.55
    if "tin" in name:
        return 6.25
    if "collection" in name:
        return 6.00

    if product_class == "sealed_booster_box":
        return 4.49
    if product_class == "sealed_booster_pack":
        return 4.99
    if product_class == "sealed_deck":
        return 0.0
    return 5.00


def _infer_product_type(name_raw: str, product_class: str) -> str:
    name = (name_raw or "").lower()
    if "ultra premium collection" in name:
        return "Ultra Premium Collection"
    if "booster box case" in name or ("booster box" in name and "case" in name):
        return "Booster Box Case"
    if "half booster box" in name:
        return "Half Booster Box"
    if "booster box" in name:
        return "Booster Box"
    if "pokemon center elite trainer box" in name:
        return "Pokemon Center ETB"
    if "elite trainer box" in name or " etb" in name or name.startswith("etb "):
        return "Elite Trainer Box"
    if "booster bundle" in name:
        return "Booster Bundle"
    if re.search(r"\bmini\s*tin[s]?\b", name):
        return "Mini Tin"
    if "checklane blister" in name:
        return "Pack Blister"
    if re.search(r"\bstacking\s*tin[s]?\b", name) or re.search(r"\bpoke\s*ball\s*tin[s]?\b", name) or re.search(r"\bpokeball\s*tin[s]?\b", name):
        return "Tin"
    if re.search(r"\btin[s]?\b", name):
        return "Tin"
    if "pack blister" in name:
        return "Pack Blister"
    if "sleeved booster pack" in name:
        return "Sleeved Pack"
    if "booster pack" in name:
        return "Booster Pack"
    if "collection" in name:
        return "Collection Box"
    if product_class == "sealed_booster_box":
        return "Booster Box"
    if product_class == "sealed_booster_pack":
        return "Sealed Product"
    if product_class == "sealed_deck":
        return "Deck"
    if product_class == "mcap":
        return "Premium Collection"
    return "Sealed Product"


def require_tracking_user(authorization: str | None):
    """Resolve a signed-in tracking user from the bearer token used by the dashboard UI."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing tracking session")
    token = authorization.split(" ", 1)[1].strip()
    session_user = get_session_user(token)
    if session_user is None:
        raise HTTPException(status_code=401, detail="Invalid tracking session")
    return session_user


def is_admin_username(username: str | None) -> bool:
    return str(username or "").strip().lower() in ADMIN_USERNAMES


def admin_user_payload(session_user) -> dict:
    return {
        "username": session_user.username,
        "is_admin": is_admin_username(session_user.username),
    }


def require_tracking_user_from_request(
    authorization: str | None = None,
    tracking_token: str | None = None,
):
    """Resolve a signed-in user from either the bearer header or the dashboard cookie."""
    header = authorization
    if (not header or not header.lower().startswith("bearer ")) and tracking_token:
        header = f"Bearer {tracking_token}"
    return require_tracking_user(header)


def require_admin_user(
    authorization: str | None = None,
    tracking_token: str | None = None,
):
    session_user = require_tracking_user_from_request(authorization=authorization, tracking_token=tracking_token)
    if not is_admin_username(session_user.username):
        raise HTTPException(status_code=403, detail="Admin access required")
    return session_user


STORE_PRICE_RULES_CSV = SCRIPT_DIR.parents[1] / "data" / "store_price_rules.csv"
SQUARESPACE_EXPORT_CSV = SCRIPT_DIR.parents[1] / "products_Apr-09_04-31-18PM.csv"
SQUARESPACE_EXPORT_ARCHIVE_DIR = SCRIPT_DIR.parents[1] / "data" / "archive" / "squarespace_exports"
SUPPLIER_NAME_MAPPING_CSV = SCRIPT_DIR.parents[1] / "data" / "supplier_name_mapping.csv"
SUPPLIER_QUOTES_CSV = SCRIPT_DIR.parents[1] / "data" / "supplier_quotes.csv"
SQUARESPACE_MAPPING_CSV = SCRIPT_DIR.parents[1] / "output" / "squarespace_product_mapping.csv"


def _normalize_supplier_name(value: str) -> str:
    """Normalize supplier screenshot item names for mapping-file lookup.

    Expected input:
    - OCR text or manually edited supplier item names such as "Mega Dream"

    Expected output:
    - lowercase whitespace-normalized key that matches `supplier_name_mapping.csv`
    """
    text = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()
    return re.sub(r"\s+", " ", text)


def load_supplier_name_mapping() -> dict[str, str]:
    """Load supplier raw-name to SKU mappings used during OCR review.

    Source:
    - `data/supplier_name_mapping.csv`

    Expected output:
    - dict where key is normalized supplier item name and value is store SKU

    Failure behavior:
    - returns an empty dict when the mapping file does not exist yet
    """
    mapping: dict[str, str] = {}
    if not SUPPLIER_NAME_MAPPING_CSV.exists():
        return mapping
    with SUPPLIER_NAME_MAPPING_CSV.open(newline="") as handle:
        for row in csv.DictReader(handle):
            key = _normalize_supplier_name(row.get("supplier_name_raw") or "")
            sku = str(row.get("sku") or "").strip()
            if key and sku:
                mapping[key] = sku
    return mapping


def load_current_store_mapping() -> dict[str, dict]:
    """Load the current SKU -> Squarespace product mapping generated by the sync script.

    Expected output:
    - dict keyed by SKU
    - each value includes product id, variant id, title, and current prices
    """
    mapping: dict[str, dict] = {}
    if not SQUARESPACE_MAPPING_CSV.exists():
        return mapping
    with SQUARESPACE_MAPPING_CSV.open(newline="") as handle:
        for row in csv.DictReader(handle):
            sku = str(row.get("sku") or "").strip()
            if sku:
                mapping[sku] = row
    return mapping


def load_latest_market_targets() -> dict[str, dict]:
    """Load the most recent generated pricing targets for SKU-level comparison.

    Expected output:
    - dict keyed by SKU
    - each value includes `market_price`, `target_price`, and pricing metadata
    """
    market_path = SCRIPT_DIR.parents[1] / "data" / "market_prices_latest.csv"
    mapping: dict[str, dict] = {}
    if not market_path.exists():
        return mapping
    with market_path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            sku = str(row.get("sku") or "").strip()
            if sku:
                mapping[sku] = row
    return mapping


def summarize_store_price_rules() -> dict:
    """Count how many store pricing rules are automatic vs manual."""
    total_rules = 0
    manual_rules = 0
    with STORE_PRICE_RULES_CSV.open(newline="") as handle:
        for row in csv.DictReader(handle):
            sku = str(row.get("sku") or "").strip()
            if not sku:
                continue
            total_rules += 1
            if str(row.get("pricing_mode") or "").strip() == "manual":
                manual_rules += 1
    return {
        "total_rules": total_rules,
        "manual_rules": manual_rules,
        "auto_rules": total_rules - manual_rules,
    }


def summarize_uploaded_export(upload_path: Path) -> dict:
    """Build a pricing coverage summary for a newly uploaded Squarespace export.

    Expected output:
    - counts for export rows, covered target rows, unmatched rules, and a preview table
    """
    rows = []
    with upload_path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            sku = str(row.get("SKU") or "").strip()
            if not sku:
                continue
            rows.append(
                {
                    "sku": sku,
                    "title": str(row.get("Title") or "").strip(),
                    "price": str(row.get("Price") or "").strip(),
                    "sale_price": str(row.get("Sale Price") or "").strip(),
                }
            )
    rows_by_sku = {row["sku"]: row for row in rows}
    target_rows, unmatched = build_target_rows(upload_path)
    auto_preview = []
    covered = 0
    missing_from_export = []
    for row in target_rows:
        export_row = rows_by_sku.get(row["sku"])
        if export_row is None:
            missing_from_export.append(row["sku"])
            continue
        covered += 1
        auto_preview.append(
            {
                "sku": row["sku"],
                "title": export_row["title"],
                "current_price": export_row["price"],
                "target_price": row["target_price"],
                "market_price": row["market_price"],
                "pricing_mode": row["pricing_mode"],
                "market_source": row["market_source"],
            }
        )
    auto_preview.sort(key=lambda item: item["sku"])
    return {
        "export_rows": len(rows),
        "covered_rows": covered,
        "target_rows": len(target_rows),
        "missing_from_export": missing_from_export,
        "unmatched_rules": unmatched,
        "preview": auto_preview[:80],
        **summarize_store_price_rules(),
    }


def enrich_supplier_rows(rows: list[dict]) -> list[dict]:
    """Attach store and market context to OCR-reviewed supplier quote rows.

    Expected output:
    - each row is returned with `sku`, `store_title`, `store_price`, `market_price`,
      and `target_price` added when available
    """
    supplier_mapping = load_supplier_name_mapping()
    store_mapping = load_current_store_mapping()
    market_targets = load_latest_market_targets()
    enriched = []
    for row in rows:
        raw_name = str(row.get("item_name_raw") or "").strip()
        cost_jpy = str(row.get("cost_jpy") or "").strip()
        sku = str(row.get("sku") or "").strip()
        if not sku:
            sku = supplier_mapping.get(_normalize_supplier_name(raw_name), "")
        store_row = store_mapping.get(sku, {})
        market_row = market_targets.get(sku, {})
        enriched.append(
            {
                "item_name_raw": raw_name,
                "cost_jpy": cost_jpy,
                "sku": sku,
                "store_title": str(store_row.get("title") or ""),
                "store_price": str(store_row.get("current_price") or "") or None,
                "market_price": str(market_row.get("market_price") or "") or None,
                "target_price": str(market_row.get("target_price") or "") or None,
            }
        )
    return enriched


def load_latest_supplier_quotes() -> tuple[list[dict], list[dict]]:
    """Return the newest supplier quote row per SKU plus unmatched historical rows.

    Expected output:
    - first item: latest matched quote row per SKU
    - second item: quote rows that still have no SKU mapping
    """
    latest_by_sku: dict[str, dict] = {}
    unmatched_rows: list[dict] = []
    if not SUPPLIER_QUOTES_CSV.exists():
        return [], []
    with SUPPLIER_QUOTES_CSV.open(newline="") as handle:
        for row in csv.DictReader(handle):
            sku = str(row.get("sku") or "").strip()
            if not sku:
                unmatched_rows.append(row)
                continue
            sort_key = (
                str(row.get("quote_date") or "").strip(),
                str(row.get("quote_id") or "").strip(),
                str(row.get("item_name_raw") or "").strip().lower(),
            )
            existing = latest_by_sku.get(sku)
            existing_key = (
                str(existing.get("quote_date") or "").strip(),
                str(existing.get("quote_id") or "").strip(),
                str(existing.get("item_name_raw") or "").strip().lower(),
            ) if existing else None
            if existing is None or sort_key >= existing_key:
                latest_by_sku[sku] = row
    latest_rows = sorted(latest_by_sku.values(), key=lambda row: str(row.get("sku") or ""))
    return latest_rows, unmatched_rows


def _round_money(value: float | None) -> float | None:
    """Round floats for JSON responses while preserving `None` for missing values."""
    if value is None:
        return None
    return round(value + 1e-9, 2)


def _profitability_status(required_price: float | None, target_price: float | None, market_price: float | None) -> str:
    """Classify whether a SKU looks buyable under the current assumptions."""
    if required_price is None:
        return "Check assumptions"
    if target_price is not None and target_price >= required_price:
        return "Buy"
    if market_price is not None and market_price >= required_price:
        return "Thin margin"
    return "Pass"


@app.post("/supplier-profitability/data")
def supplier_profitability_data(
    payload: dict,
    authorization: str | None = Header(default=None),
    tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token"),
):
    """Return per-SKU profitability rows for the Supplier Profitability admin page.

    Expected input:
    - JSON payload of operator assumptions from the browser UI

    Expected output:
    - `rows`: one per SKU with supplier/store/market economics
    - `stats`: summary counts for the page
    - `assumptions`: normalized values actually used in the calculation

    Important:
    - every row is a per-unit result, not a whole-order result
    - when `inbound_shipping_mode=order-estimate`, whole-order shipping is allocated
      down to a per-box estimate before profitability is calculated
    """
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
    assumptions = payload if isinstance(payload, dict) else {}
    latest_quotes, unmatched_quotes = load_latest_supplier_quotes()
    store_mapping = load_current_store_mapping()
    market_targets = load_latest_market_targets()

    jpy_per_usd = _safe_float(assumptions.get("jpy_per_usd")) or 145.0
    import_duty_pct = _safe_float(assumptions.get("import_duty_pct")) or 0.0
    inbound_shipping_mode = str(assumptions.get("inbound_shipping_mode") or "manual").strip().lower()
    inbound_shipping_usd = _safe_float(assumptions.get("inbound_shipping_usd")) or 0.0
    order_shipping_jpy = _safe_float(assumptions.get("order_shipping_jpy")) or 0.0
    order_box_count = _safe_float(assumptions.get("order_box_count")) or 0.0
    handling_cost_usd = _safe_float(assumptions.get("handling_cost_usd")) or 0.0
    outbound_shipping_usd = _safe_float(assumptions.get("outbound_shipping_usd")) or 0.0
    shipping_credit_usd = _safe_float(assumptions.get("shipping_credit_usd")) or 0.0
    disbursement_fee_usd = _safe_float(assumptions.get("disbursement_fee_usd")) or 15.0
    platform_fee_pct = _safe_float(assumptions.get("platform_fee_pct")) or 0.0
    payment_fee_pct = _safe_float(assumptions.get("payment_fee_pct")) or 0.0
    payment_fee_fixed = _safe_float(assumptions.get("payment_fee_fixed")) or 0.0
    income_tax_pct = _safe_float(assumptions.get("income_tax_pct")) or 0.0
    target_margin_pct = _safe_float(assumptions.get("target_margin_pct")) or 0.0

    fee_rate = max(0.0, (platform_fee_pct + payment_fee_pct) / 100.0)
    tax_rate = max(0.0, income_tax_pct / 100.0)
    target_margin_rate = max(0.0, target_margin_pct / 100.0)
    margin_denominator = 1.0 - fee_rate - target_margin_rate
    break_even_denominator = 1.0 - fee_rate

    def profit_at_price(price: float | None, fixed_costs: float) -> tuple[float | None, float | None, float | None, float | None]:
        """Return before/after-tax profit and margin for one hypothetical sale price."""
        if price is None:
            return None, None, None, None
        profit_before_tax = price * (1.0 - fee_rate) - fixed_costs
        profit_after_tax = profit_before_tax if profit_before_tax <= 0 else profit_before_tax * (1.0 - tax_rate)
        margin_before_tax = (profit_before_tax / price) * 100.0 if price > 0 else None
        margin_after_tax = (profit_after_tax / price) * 100.0 if price > 0 else None
        return (
            _round_money(profit_before_tax),
            _round_money(profit_after_tax),
            _round_money(margin_before_tax),
            _round_money(margin_after_tax),
        )

    rows: list[dict] = []
    for quote in latest_quotes:
        sku = str(quote.get("sku") or "").strip()
        if not sku:
            continue
        cost_jpy = _safe_float(quote.get("cost_jpy"))
        if cost_jpy is None or cost_jpy <= 0 or jpy_per_usd <= 0:
            continue
        store_row = store_mapping.get(sku, {})
        market_row = market_targets.get(sku, {})
        supplier_cost_usd = cost_jpy / jpy_per_usd
        import_cost_usd = supplier_cost_usd * (import_duty_pct / 100.0)
        estimated_inbound_shipping_jpy = None
        estimated_inbound_shipping_usd = None
        effective_inbound_shipping_usd = inbound_shipping_usd
        # Supplier quotes often arrive as whole-order shipping. When that mode is
        # selected, allocate the quote down to a per-box estimate so every row on
        # the page still answers "what does one unit need to sell for?"
        if inbound_shipping_mode == "order-estimate" and order_shipping_jpy > 0 and order_box_count > 0 and jpy_per_usd > 0:
            estimated_inbound_shipping_jpy = order_shipping_jpy / order_box_count
            estimated_inbound_shipping_usd = estimated_inbound_shipping_jpy / jpy_per_usd
            effective_inbound_shipping_usd = estimated_inbound_shipping_usd
        landed_cost_usd = supplier_cost_usd + import_cost_usd + effective_inbound_shipping_usd + handling_cost_usd + disbursement_fee_usd
        fixed_costs = landed_cost_usd + outbound_shipping_usd + payment_fee_fixed - shipping_credit_usd
        break_even_price = (fixed_costs / break_even_denominator) if break_even_denominator > 0 else None
        required_price_for_target_margin = (fixed_costs / margin_denominator) if margin_denominator > 0 else None
        store_price = _safe_float(store_row.get("current_price"))
        market_price = _safe_float(market_row.get("market_price"))
        target_price = _safe_float(market_row.get("target_price"))
        store_profit = profit_at_price(store_price, fixed_costs)
        market_profit = profit_at_price(market_price, fixed_costs)
        target_profit = profit_at_price(target_price, fixed_costs)
        recommended_floor = required_price_for_target_margin or break_even_price
        rows.append(
            {
                "sku": sku,
                "title": str(store_row.get("title") or market_row.get("title") or quote.get("item_name_raw") or sku),
                "supplier_item": str(quote.get("item_name_raw") or ""),
                "quote_date": str(quote.get("quote_date") or ""),
                "supplier_name": str(quote.get("supplier_name") or ""),
                "cost_jpy": int(round(cost_jpy)),
                "supplier_cost_usd": _round_money(supplier_cost_usd),
                "import_cost_usd": _round_money(import_cost_usd),
                "inbound_shipping_mode": inbound_shipping_mode,
                "inbound_shipping_usd": _round_money(effective_inbound_shipping_usd),
                "estimated_inbound_shipping_jpy": _round_money(estimated_inbound_shipping_jpy),
                "estimated_inbound_shipping_usd": _round_money(estimated_inbound_shipping_usd),
                "landed_cost_usd": _round_money(landed_cost_usd),
                "fixed_costs_usd": _round_money(fixed_costs),
                "store_price": _round_money(store_price),
                "market_price": _round_money(market_price),
                "target_price": _round_money(target_price),
                "break_even_price": _round_money(break_even_price),
                "required_price_for_target_margin": _round_money(required_price_for_target_margin),
                "recommended_floor_price": _round_money(recommended_floor),
                "profit_at_store_before_tax": store_profit[0],
                "profit_at_store_after_tax": store_profit[1],
                "margin_at_store_before_tax_pct": store_profit[2],
                "margin_at_store_after_tax_pct": store_profit[3],
                "profit_at_market_before_tax": market_profit[0],
                "profit_at_market_after_tax": market_profit[1],
                "margin_at_market_before_tax_pct": market_profit[2],
                "margin_at_market_after_tax_pct": market_profit[3],
                "profit_at_target_before_tax": target_profit[0],
                "profit_at_target_after_tax": target_profit[1],
                "margin_at_target_before_tax_pct": target_profit[2],
                "margin_at_target_after_tax_pct": target_profit[3],
                "headroom_vs_target": _round_money((target_price - required_price_for_target_margin) if target_price is not None and required_price_for_target_margin is not None else None),
                "decision": _profitability_status(required_price_for_target_margin, target_price, market_price),
            }
        )

    rows.sort(
        key=lambda row: (
            {"Buy": 0, "Thin margin": 1, "Pass": 2, "Check assumptions": 3}.get(str(row.get("decision")), 9),
            -(float(row.get("headroom_vs_target")) if row.get("headroom_vs_target") is not None else -10_000.0),
            str(row.get("sku") or ""),
        )
    )

    stats = {
        "rows": len(rows),
        "buy_count": sum(1 for row in rows if row.get("decision") == "Buy"),
        "thin_margin_count": sum(1 for row in rows if row.get("decision") == "Thin margin"),
        "pass_count": sum(1 for row in rows if row.get("decision") == "Pass"),
        "unmatched_quote_rows": len(unmatched_quotes),
        "avg_target_profit_after_tax": _round_money(
            sum(float(row["profit_at_target_after_tax"]) for row in rows if row.get("profit_at_target_after_tax") is not None)
            / max(1, sum(1 for row in rows if row.get("profit_at_target_after_tax") is not None))
        ) if rows else None,
    }

    return {
        "ok": True,
        "rows": rows,
        "stats": stats,
        "assumptions": {
            "jpy_per_usd": jpy_per_usd,
            "import_duty_pct": import_duty_pct,
            "inbound_shipping_mode": inbound_shipping_mode,
            "inbound_shipping_usd": inbound_shipping_usd,
            "order_shipping_jpy": order_shipping_jpy,
            "order_box_count": order_box_count,
            "handling_cost_usd": handling_cost_usd,
            "outbound_shipping_usd": outbound_shipping_usd,
            "shipping_credit_usd": shipping_credit_usd,
            "disbursement_fee_usd": disbursement_fee_usd,
            "platform_fee_pct": platform_fee_pct,
            "payment_fee_pct": payment_fee_pct,
            "payment_fee_fixed": payment_fee_fixed,
            "income_tax_pct": income_tax_pct,
            "target_margin_pct": target_margin_pct,
        },
    }


@app.post("/pricing-upload/compare")
async def pricing_upload_compare(
    file: UploadFile = File(...),
    authorization: str | None = Header(default=None),
    tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token"),
):
    """Accept a fresh Squarespace export, archive it, and preview pricing coverage.

    Side effects:
    - overwrites the canonical local export used by the pricing pipeline
    - writes a timestamped archive copy for later debugging
    """
    admin_user = require_admin_user(authorization=authorization, tracking_token=tracking_token)
    filename = str(file.filename or "").strip()
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a Squarespace CSV export.")

    payload = await file.read()
    if not payload:
        raise HTTPException(status_code=400, detail="Uploaded file was empty.")

    header_line = payload.splitlines()[0].decode("utf-8", errors="ignore") if payload.splitlines() else ""
    if "Product ID [Non Editable]" not in header_line or "SKU" not in header_line:
        raise HTTPException(status_code=400, detail="CSV does not look like a Squarespace product export.")

    SQUARESPACE_EXPORT_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_path = SQUARESPACE_EXPORT_ARCHIVE_DIR / f"squarespace_export_{timestamp}.csv"
    archive_path.write_bytes(payload)
    SQUARESPACE_EXPORT_CSV.write_bytes(payload)

    summary = summarize_uploaded_export(SQUARESPACE_EXPORT_CSV)
    return {
        "ok": True,
        "saved_to": str(SQUARESPACE_EXPORT_CSV),
        "archive_path": str(archive_path),
        "uploaded_by": admin_user.username,
        "uploaded_at": timestamp,
        **summary,
    }


@app.post("/supplier-pricing/enrich")
def supplier_pricing_enrich(
    payload: dict,
    authorization: str | None = Header(default=None),
    tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token"),
):
    """Enrich OCR-reviewed supplier rows with SKU, store, and market context."""
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
    rows = payload.get("rows") or []
    if not isinstance(rows, list):
        raise HTTPException(status_code=400, detail="rows must be a list")
    return {"ok": True, "rows": enrich_supplier_rows(rows)}


@app.post("/supplier-pricing/save")
def supplier_pricing_save(
    payload: dict,
    authorization: str | None = Header(default=None),
    tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token"),
):
    """Persist reviewed supplier quote rows to the historical supplier quote ledger.

    Expected output:
    - JSON with `quote_id`, `saved_rows`, `matched_skus`, and the CSV path
    """
    admin_user = require_admin_user(authorization=authorization, tracking_token=tracking_token)
    supplier_name = str(payload.get("supplier_name") or "").strip() or "Unknown Supplier"
    quote_date = str(payload.get("quote_date") or "").strip()
    source_name = str(payload.get("source_name") or "").strip()
    source_type = str(payload.get("source_type") or "").strip() or "screenshot"
    rows = payload.get("rows") or []
    if not quote_date:
        raise HTTPException(status_code=400, detail="quote_date is required")
    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=400, detail="rows must be a non-empty list")

    quote_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    SUPPLIER_QUOTES_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "quote_id",
        "quote_date",
        "supplier_name",
        "source_name",
        "source_type",
        "item_name_raw",
        "sku",
        "cost_jpy",
        "image_name",
        "notes",
    ]
    needs_header = not SUPPLIER_QUOTES_CSV.exists() or SUPPLIER_QUOTES_CSV.stat().st_size == 0
    saved_rows = 0
    matched_skus = 0
    with SUPPLIER_QUOTES_CSV.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if needs_header:
            writer.writeheader()
        for row in rows:
            item_name_raw = str(row.get("item_name_raw") or "").strip()
            cost_jpy = str(row.get("cost_jpy") or "").strip()
            sku = str(row.get("sku") or "").strip()
            if not item_name_raw or not cost_jpy:
                continue
            writer.writerow(
                {
                    "quote_id": quote_id,
                    "quote_date": quote_date,
                    "supplier_name": supplier_name,
                    "source_name": source_name,
                    "source_type": source_type,
                    "item_name_raw": item_name_raw,
                    "sku": sku,
                    "cost_jpy": cost_jpy,
                    "image_name": source_name,
                    "notes": f"saved_by={admin_user.username}",
                }
            )
            saved_rows += 1
            if sku:
                matched_skus += 1

    return {
        "ok": True,
        "quote_id": quote_id,
        "saved_rows": saved_rows,
        "matched_skus": matched_skus,
        "quotes_path": str(SUPPLIER_QUOTES_CSV),
    }


def verify_google_identity_token(credential: str) -> dict:
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=503, detail="Google sign-in is not configured")
    token = str(credential or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="Missing Google credential")
    try:
        response = requests.get(
            "https://oauth2.googleapis.com/tokeninfo",
            params={"id_token": token},
            timeout=8,
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Google token verification failed: {exc}") from exc
    if response.status_code != 200:
        raise HTTPException(status_code=401, detail="Google sign-in could not be verified")
    payload = response.json()
    audience = str(payload.get("aud", "")).strip()
    email = str(payload.get("email", "")).strip().lower()
    email_verified = str(payload.get("email_verified", "")).lower() == "true"
    subject = str(payload.get("sub", "")).strip()
    if audience != GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=401, detail="Google sign-in was issued for a different app")
    if not email or not email_verified or not subject:
        raise HTTPException(status_code=401, detail="Google account details were incomplete")
    return {
        "email": email,
        "sub": subject,
        "name": str(payload.get("name", "")).strip(),
        "picture": str(payload.get("picture", "")).strip(),
    }


@app.post("/tracking/session")
def tracking_session(payload: dict):
    """Legacy username + PIN sign-in kept for existing local tracking accounts."""
    username = str(payload.get("username", "")).strip()
    pin = str(payload.get("pin", "")).strip()
    action = str(payload.get("action", "auto")).strip().lower()
    create_if_missing = bool(payload.get("create_if_missing", action in {"auto", "create"}))
    if len(username) < 3:
        raise HTTPException(status_code=400, detail="Username must be at least 3 characters")
    if len(pin) < 4:
        raise HTTPException(status_code=400, detail="PIN must be at least 4 characters")

    existing = get_user_by_username(username)
    generic_auth_error = "That username or PIN is incorrect."
    if action == "create" and existing is not None:
        raise HTTPException(
            status_code=409,
            detail=f"{generic_auth_error} Sign in with the existing account or choose a different username.",
        )
    if action == "sign_in" and existing is None:
        raise HTTPException(status_code=401, detail=generic_auth_error)

    user = verify_user(username, pin)
    if user is None:
        if existing is not None:
            raise HTTPException(status_code=401, detail=generic_auth_error)
        if not create_if_missing:
            raise HTTPException(status_code=401, detail=generic_auth_error)
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
            "is_admin": is_admin_username(username_out),
        },
    }


@app.get("/tracking/auth_config")
def tracking_auth_config():
    return {
        "google_enabled": bool(GOOGLE_CLIENT_ID),
        "google_client_id": GOOGLE_CLIENT_ID,
    }


@app.post("/tracking/google_session")
def tracking_google_session(payload: dict):
    identity = verify_google_identity_token(payload.get("credential", ""))
    email = identity["email"]
    existing = get_user_by_username(email)
    if existing is None:
        user_id = create_google_user(email)
        username_out = email
    else:
        user_id = int(existing["id"])
        username_out = existing["username"]
    token = create_session(user_id)
    return {
        "token": token,
        "user": {
            "username": username_out,
            "is_admin": is_admin_username(username_out),
        },
    }


@app.get("/tracking/session")
def tracking_session_status(authorization: str | None = Header(default=None)):
    session_user = require_tracking_user(authorization)
    return {"user": admin_user_payload(session_user)}


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


@app.delete("/tracking/account")
def tracking_delete_account(payload: dict | None = None, authorization: str | None = Header(default=None)):
    session_user = require_tracking_user(authorization)
    delete_user(session_user.user_id)
    return {"ok": True}


@app.post("/bug_reports")
def submit_bug_report(payload: dict):
    title = str(payload.get("title", "")).strip()
    details = str(payload.get("details", "")).strip()
    if len(title) < 3:
        raise HTTPException(status_code=400, detail="Title must be at least 3 characters")
    if len(details) < 10:
        raise HTTPException(status_code=400, detail="Details must be at least 10 characters")

    context_payload = {
        "page_path": str(payload.get("page_path", "")).strip(),
        "page_url": str(payload.get("page_url", "")).strip(),
        "category_id": payload.get("category_id"),
        "tab": str(payload.get("tab", "")).strip(),
        "segment": str(payload.get("segment", "")).strip(),
        "chart_mode": str(payload.get("chart_mode", "")).strip(),
        "product_key": str(payload.get("product_key", "")).strip(),
        "group_id": payload.get("group_id"),
        "search_query": str(payload.get("search_query", "")).strip(),
        "reporter_username": str(payload.get("reporter_username", "")).strip(),
        "user_agent": str(payload.get("user_agent", "")).strip(),
        "expected": str(payload.get("expected", "")).strip(),
        "discord_status": "not_configured",
    }
    bug_report_id = create_bug_report(
        {
            **context_payload,
            "title": title,
            "details": details,
            "expected": context_payload["expected"],
            "context_json": json.dumps(context_payload, ensure_ascii=True),
        }
    )
    return {"ok": True, "id": bug_report_id}


@app.get("/bug_reports")
def bug_reports(
    limit: int = 200,
    authorization: str | None = Header(default=None),
    tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token"),
):
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
    return {"items": list_bug_reports(limit=limit)}


@app.get("/eod/index_components")
def eod_index_components(index: str, authorization: str | None = Header(default=None), tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token")):
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
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
def eod_series(code: str, days: int = 365, authorization: str | None = Header(default=None), tracking_token: str | None = Cookie(default=None, alias="pm_tracking_token")):
    require_admin_user(authorization=authorization, tracking_token=tracking_token)
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
def universe(limit: int = 5000, category_id: int = 3, product_id: int | None = None, sub_type_name: str | None = None, keys: str | None = None):
    limit = max(1, min(limit, 50000))
    category = category_config(category_id)
    product_signal_source = product_signal_from(category.category_id)
    metadata_cte = build_metadata_cte(category.category_id, include_classification=True, cte_name="metadata")
    filters: list[str] = []
    if product_id is not None:
        filters.append(f"s.productId = {int(product_id)}")
    if sub_type_name is not None:
        safe_sub_type_name = str(sub_type_name).replace("'", "''")
        filters.append(f"COALESCE(s.subTypeName, '') = '{safe_sub_type_name}'")
    if keys:
        key_filters: list[str] = []
        for raw_key in str(keys).split(","):
            product_part, _, subtype_part = raw_key.partition("||")
            try:
                parsed_product_id = int(product_part)
            except (TypeError, ValueError):
                continue
            safe_subtype = subtype_part.replace("'", "''")
            key_filters.append(
                f"(s.productId = {parsed_product_id} AND COALESCE(s.subTypeName, '') = '{safe_subtype}')"
            )
        if key_filters:
            filters.append(f"({' OR '.join(key_filters)})")
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    sql = f"""
    WITH
    {metadata_cte}
    SELECT
      s.productId,
      s.subTypeName,
      COALESCE(m.groupId, s.groupId) AS groupId,
      COALESCE(NULLIF(trim(m.groupName), ''), s.groupName) AS groupName,
      COALESCE(
        CASE
          WHEN lower(trim(COALESCE(m.productName, ''))) IN (
            lower('product ' || CAST(s.productId AS VARCHAR)),
            lower('productid ' || CAST(s.productId AS VARCHAR))
          ) THEN NULL
          ELSE NULLIF(trim(m.productName), '')
        END,
        CASE
          WHEN lower(trim(COALESCE(s.productName, ''))) IN (
            lower('product ' || CAST(s.productId AS VARCHAR)),
            lower('productid ' || CAST(s.productId AS VARCHAR))
          ) THEN NULL
          ELSE NULLIF(trim(s.productName), '')
        END,
        'productId ' || CAST(s.productId AS VARCHAR)
      ) AS productName,
      COALESCE(NULLIF(trim(m.imageUrl), ''), NULLIF(trim(s.imageUrl), '')) AS imageUrl,
      COALESCE(NULLIF(trim(m.rarity), ''), s.rarity) AS rarity,
      COALESCE(NULLIF(trim(m.number), ''), s.number) AS number,
      COALESCE(NULLIF(trim(m.productClass), ''), s.productClass) AS productClass,
      COALESCE(NULLIF(trim(m.productKind), ''), s.productKind) AS productKind,
      s.latest_price,
      s.latest_date
    FROM {product_signal_source} s
    LEFT JOIN metadata m
      ON m.productId = s.productId
     AND m.groupId = s.groupId
    {where_clause}
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/product_picker")
def product_picker(limit: int = 50000, offset: int = 0, category_id: int = 3):
    limit = max(1, min(limit, 50000))
    offset = max(0, offset)
    category = category_config(category_id)
    product_signal_source = product_signal_from(category.category_id)
    metadata_cte = build_metadata_cte(category.category_id, include_classification=False, cte_name="metadata")

    sql = f"""
    WITH
    {metadata_cte}
    SELECT
      s.productId,
      s.subTypeName,
      COALESCE(m.groupId, s.groupId) AS groupId,
      COALESCE(NULLIF(trim(m.groupName), ''), s.groupName) AS groupName,
      COALESCE(
        CASE
          WHEN lower(trim(COALESCE(m.productName, ''))) IN (
            lower('product ' || CAST(s.productId AS VARCHAR)),
            lower('productid ' || CAST(s.productId AS VARCHAR))
          ) THEN NULL
          ELSE NULLIF(trim(m.productName), '')
        END,
        CASE
          WHEN lower(trim(COALESCE(s.productName, ''))) IN (
            lower('product ' || CAST(s.productId AS VARCHAR)),
            lower('productid ' || CAST(s.productId AS VARCHAR))
          ) THEN NULL
          ELSE NULLIF(trim(s.productName), '')
        END,
        'productId ' || CAST(s.productId AS VARCHAR)
      ) AS productName
    FROM {product_signal_source} s
    LEFT JOIN metadata m
      ON m.productId = s.productId
     AND m.groupId = s.groupId
    ORDER BY lower(COALESCE(NULLIF(trim(m.groupName), ''), s.groupName)),
             lower(
               COALESCE(
                 CASE
                   WHEN lower(trim(COALESCE(m.productName, ''))) IN (
                     lower('product ' || CAST(s.productId AS VARCHAR)),
                     lower('productid ' || CAST(s.productId AS VARCHAR))
                   ) THEN NULL
                   ELSE NULLIF(trim(m.productName), '')
                 END,
                 CASE
                   WHEN lower(trim(COALESCE(s.productName, ''))) IN (
                     lower('product ' || CAST(s.productId AS VARCHAR)),
                     lower('productid ' || CAST(s.productId AS VARCHAR))
                   ) THEN NULL
                   ELSE NULLIF(trim(s.productName), '')
                 END,
                 'productId ' || CAST(s.productId AS VARCHAR)
               )
             ),
             lower(COALESCE(s.subTypeName, ''))
    OFFSET {offset}
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/groups")
def groups(limit: int = 1000, offset: int = 0, category_id: int = 3):
    limit = max(1, min(limit, 5000))
    offset = max(0, offset)
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
    OFFSET {offset}
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/search")
def search(query: str, limit: int = 12, category_id: int = 3):
    """Search active sets/cards without forcing the client to preload the full universe."""
    term = " ".join(str(query or "").strip().split())
    if len(term) < 2:
        return {"items": [], "query": term, "limit": limit, "total_count": 0}

    safe_term = term.replace("'", "''")
    limit = max(1, min(limit, 500))
    category = category_config(category_id)
    product_signal_source = product_signal_from(category.category_id)
    metadata_cte = build_metadata_cte(category.category_id, include_classification=True, cte_name="metadata")
    normalized_term = "".join(ch.lower() for ch in term if ch.isalnum())
    safe_normalized_term = normalized_term.replace("'", "''")
    raw_tokens = [token for token in re.split(r"\s+", term.lower()) if token]
    normalized_tokens = ["".join(ch for ch in token if ch.isalnum()) for token in raw_tokens]

    def _safe_token(token: str) -> str:
        return token.replace("'", "''")

    def _raw_or_normalized_like(raw_expr: str, normalized_expr: str, raw_token: str, normalized_token: str) -> str:
        clauses = [f"{raw_expr} LIKE '%' || lower('{_safe_token(raw_token)}') || '%'"]
        if normalized_token:
            clauses.append(f"{normalized_expr} LIKE '%' || lower('{_safe_token(normalized_token)}') || '%'")
        return "(" + " OR ".join(clauses) + ")"

    def _all_token_matches(raw_expr: str, normalized_expr: str) -> str:
        if not raw_tokens:
            return "TRUE"
        clauses = [
            _raw_or_normalized_like(raw_expr, normalized_expr, raw_token, normalized_token)
            for raw_token, normalized_token in zip(raw_tokens, normalized_tokens)
        ]
        return "(" + " AND ".join(clauses) + ")"

    def _any_token_matches(raw_expr: str, normalized_expr: str) -> str:
        if not raw_tokens:
            return "FALSE"
        clauses = [
            _raw_or_normalized_like(raw_expr, normalized_expr, raw_token, normalized_token)
            for raw_token, normalized_token in zip(raw_tokens, normalized_tokens)
        ]
        return "(" + " OR ".join(clauses) + ")"

    product_token_match_expr = _all_token_matches(
        "lower(CONCAT_WS(' ', COALESCE(m.productName, ''), COALESCE(m.groupName, ''), COALESCE(m.groupAbbreviation, ''), COALESCE(m.number, ''), COALESCE(ap.subTypeName, '')))",
        "lower(regexp_replace(CONCAT_WS(' ', COALESCE(m.productName, ''), COALESCE(m.groupName, ''), COALESCE(m.groupAbbreviation, ''), COALESCE(m.number, ''), COALESCE(ap.subTypeName, '')), '[^a-z0-9]+', '', 'g'))",
    )
    group_token_match_expr = _all_token_matches(
        "lower(CONCAT_WS(' ', COALESCE(g.name, ''), COALESCE(g.abbreviation, '')))",
        "lower(regexp_replace(CONCAT_WS(' ', COALESCE(g.name, ''), COALESCE(g.abbreviation, '')), '[^a-z0-9]+', '', 'g'))",
    )
    product_name_token_expr = _any_token_matches(
        "lower(COALESCE(m.productName, ''))",
        "lower(regexp_replace(COALESCE(m.productName, ''), '[^a-z0-9]+', '', 'g'))",
    )
    product_group_token_expr = _any_token_matches(
        "lower(COALESCE(m.groupName, ''))",
        "lower(regexp_replace(COALESCE(m.groupName, ''), '[^a-z0-9]+', '', 'g'))",
    )
    product_group_abbrev_token_expr = _any_token_matches(
        "lower(COALESCE(m.groupAbbreviation, ''))",
        "lower(regexp_replace(COALESCE(m.groupAbbreviation, ''), '[^a-z0-9]+', '', 'g'))",
    )
    product_number_token_expr = _any_token_matches(
        "lower(COALESCE(m.number, ''))",
        "lower(regexp_replace(COALESCE(m.number, ''), '[^a-z0-9]+', '', 'g'))",
    )
    product_subtype_token_expr = _any_token_matches(
        "lower(COALESCE(ap.subTypeName, ''))",
        "lower(regexp_replace(COALESCE(ap.subTypeName, ''), '[^a-z0-9]+', '', 'g'))",
    )
    group_name_token_expr = _any_token_matches(
        "lower(COALESCE(g.name, ''))",
        "lower(regexp_replace(COALESCE(g.name, ''), '[^a-z0-9]+', '', 'g'))",
    )
    group_abbrev_token_expr = _any_token_matches(
        "lower(COALESCE(g.abbreviation, ''))",
        "lower(regexp_replace(COALESCE(g.abbreviation, ''), '[^a-z0-9]+', '', 'g'))",
    )
    product_name_raw_expr = "lower(COALESCE(m.productName, ''))"
    product_name_normalized_expr = "lower(regexp_replace(COALESCE(m.productName, ''), '[^a-z0-9]+', '', 'g'))"
    product_group_raw_expr = "lower(COALESCE(m.groupName, ''))"
    product_group_normalized_expr = "lower(regexp_replace(COALESCE(m.groupName, ''), '[^a-z0-9]+', '', 'g'))"
    product_group_abbrev_raw_expr = "lower(COALESCE(m.groupAbbreviation, ''))"
    product_group_abbrev_normalized_expr = "lower(regexp_replace(COALESCE(m.groupAbbreviation, ''), '[^a-z0-9]+', '', 'g'))"
    product_number_raw_expr = "lower(COALESCE(m.number, ''))"
    product_number_normalized_expr = "lower(regexp_replace(COALESCE(m.number, ''), '[^a-z0-9]+', '', 'g'))"
    product_subtype_raw_expr = "lower(COALESCE(ap.subTypeName, ''))"
    product_subtype_normalized_expr = "lower(regexp_replace(COALESCE(ap.subTypeName, ''), '[^a-z0-9]+', '', 'g'))"
    group_name_raw_expr = "lower(COALESCE(g.name, ''))"
    group_name_normalized_expr = "lower(regexp_replace(COALESCE(g.name, ''), '[^a-z0-9]+', '', 'g'))"
    group_abbrev_raw_expr = "lower(COALESCE(g.abbreviation, ''))"
    group_abbrev_normalized_expr = "lower(regexp_replace(COALESCE(g.abbreviation, ''), '[^a-z0-9]+', '', 'g'))"
    product_token_score_expr = " + ".join(
        [
            f"CASE WHEN {_raw_or_normalized_like(product_name_raw_expr, product_name_normalized_expr, raw_token, normalized_token)} THEN 70 ELSE 0 END"
            for raw_token, normalized_token in zip(raw_tokens, normalized_tokens)
        ]
        + [
            f"CASE WHEN {_raw_or_normalized_like(product_group_raw_expr, product_group_normalized_expr, raw_token, normalized_token)} THEN 55 ELSE 0 END"
            for raw_token, normalized_token in zip(raw_tokens, normalized_tokens)
        ]
        + [
            f"CASE WHEN {_raw_or_normalized_like(product_group_abbrev_raw_expr, product_group_abbrev_normalized_expr, raw_token, normalized_token)} THEN 62 ELSE 0 END"
            for raw_token, normalized_token in zip(raw_tokens, normalized_tokens)
        ]
        + [
            f"CASE WHEN {_raw_or_normalized_like(product_number_raw_expr, product_number_normalized_expr, raw_token, normalized_token)} THEN 45 ELSE 0 END"
            for raw_token, normalized_token in zip(raw_tokens, normalized_tokens)
        ]
        + [
            f"CASE WHEN {_raw_or_normalized_like(product_subtype_raw_expr, product_subtype_normalized_expr, raw_token, normalized_token)} THEN 30 ELSE 0 END"
            for raw_token, normalized_token in zip(raw_tokens, normalized_tokens)
        ]
    ) or "0"
    group_token_score_expr = " + ".join(
        [
            f"CASE WHEN {_raw_or_normalized_like(group_name_raw_expr, group_name_normalized_expr, raw_token, normalized_token)} THEN 60 ELSE 0 END"
            for raw_token, normalized_token in zip(raw_tokens, normalized_tokens)
        ]
        + [
            f"CASE WHEN {_raw_or_normalized_like(group_abbrev_raw_expr, group_abbrev_normalized_expr, raw_token, normalized_token)} THEN 42 ELSE 0 END"
            for raw_token, normalized_token in zip(raw_tokens, normalized_tokens)
        ]
    ) or "0"

    sql = f"""
    WITH active_products AS (
      SELECT
        productId,
        subTypeName,
        groupId,
        latest_price,
        latest_date
      FROM {product_signal_source}
    ),
    active_groups AS (
      SELECT
        groupId,
        COUNT(DISTINCT productId) AS productCount
      FROM {product_signal_source}
      GROUP BY groupId
    ),
    {metadata_cte},
    product_matches AS (
      SELECT
        'product' AS kind,
        0 AS set_focus_rank,
        m.productName AS title,
        CONCAT_WS(' | ', m.groupName, NULLIF(m.number, ''), NULLIF(ap.subTypeName, '')) AS meta,
        ap.productId,
        ap.subTypeName,
        ap.groupId,
        m.groupName,
        m.groupAbbreviation,
        m.productName,
        m.imageUrl,
        m.rarity,
        m.number,
        m.productClass,
        m.productKind,
        ap.latest_price,
        ap.latest_date,
        CASE
          WHEN {product_token_match_expr} AND {product_name_token_expr} AND ({product_group_token_expr} OR {product_group_abbrev_token_expr}) THEN 470
          WHEN lower(regexp_replace(COALESCE(m.productName, ''), '[^a-z0-9]+', '', 'g')) = lower('{safe_normalized_term}') THEN 520
          WHEN lower(regexp_replace(COALESCE(m.number, ''), '[^a-z0-9]+', '', 'g')) = lower('{safe_normalized_term}') THEN 500
          WHEN lower(m.productName) = lower('{safe_term}') THEN 400
          WHEN lower(m.number) = lower('{safe_term}') THEN 340
          WHEN lower(regexp_replace(COALESCE(m.productName, ''), '[^a-z0-9]+', '', 'g')) LIKE lower('{safe_normalized_term}') || '%' THEN 330
          WHEN lower(m.productName) LIKE lower('{safe_term}') || '%' THEN 300
          WHEN lower(m.number) LIKE lower('{safe_term}') || '%' THEN 260
          WHEN lower(m.productName) LIKE '%' || lower('{safe_term}') || '%' THEN 220
          WHEN lower(m.groupName) LIKE lower('{safe_term}') || '%' THEN 180
          WHEN lower(m.groupName) LIKE '%' || lower('{safe_term}') || '%' THEN 150
          WHEN lower(COALESCE(ap.subTypeName, '')) LIKE '%' || lower('{safe_term}') || '%' THEN 140
          ELSE 0
        END + ({product_token_score_expr}) AS score
      FROM active_products ap
      LEFT JOIN metadata m
        ON m.productId = ap.productId
       AND m.groupId = ap.groupId
      WHERE (
        lower(COALESCE(m.productName, '')) LIKE '%' || lower('{safe_term}') || '%'
        OR lower(regexp_replace(COALESCE(m.productName, ''), '[^a-z0-9]+', '', 'g')) LIKE '%' || lower('{safe_normalized_term}') || '%'
        OR lower(COALESCE(m.number, '')) LIKE '%' || lower('{safe_term}') || '%'
        OR lower(regexp_replace(COALESCE(m.number, ''), '[^a-z0-9]+', '', 'g')) LIKE '%' || lower('{safe_normalized_term}') || '%'
        OR lower(COALESCE(m.groupName, '')) LIKE '%' || lower('{safe_term}') || '%'
        OR lower(COALESCE(ap.subTypeName, '')) LIKE '%' || lower('{safe_term}') || '%'
        OR {product_token_match_expr}
      )
    ),
    group_matches AS (
      SELECT
        'set' AS kind,
        CASE
          -- When every query token matches the set name/abbreviation, treat the
          -- set row as the primary result and let the individual cards follow it.
          WHEN {group_token_match_expr} THEN 1
          ELSE 0
        END AS set_focus_rank,
        COALESCE(g.name, 'Unknown Group') AS title,
        CONCAT(CAST(ag.productCount AS VARCHAR), ' tracked products') AS meta,
        NULL AS productId,
        '' AS subTypeName,
        ag.groupId,
        COALESCE(g.name, 'Unknown Group') AS groupName,
        COALESCE(g.abbreviation, '') AS groupAbbreviation,
        NULL AS productName,
        NULL AS imageUrl,
        NULL AS rarity,
        NULL AS number,
        NULL AS productClass,
        'set' AS productKind,
        NULL AS latest_price,
        NULL AS latest_date,
        CASE
          WHEN {group_token_match_expr} AND {group_name_token_expr} THEN 300
          WHEN lower(regexp_replace(COALESCE(g.name, ''), '[^a-z0-9]+', '', 'g')) = lower('{safe_normalized_term}') THEN 560
          WHEN lower(regexp_replace(COALESCE(g.abbreviation, ''), '[^a-z0-9]+', '', 'g')) = lower('{safe_normalized_term}') THEN 540
          WHEN lower(COALESCE(g.name, '')) = lower('{safe_term}') THEN 360
          WHEN lower(COALESCE(g.abbreviation, '')) = lower('{safe_term}') THEN 340
          WHEN lower(regexp_replace(COALESCE(g.name, ''), '[^a-z0-9]+', '', 'g')) LIKE lower('{safe_normalized_term}') || '%' THEN 320
          WHEN lower(COALESCE(g.name, '')) LIKE lower('{safe_term}') || '%' THEN 280
          WHEN lower(COALESCE(g.name, '')) LIKE '%' || lower('{safe_term}') || '%' THEN 200
          WHEN lower(COALESCE(g.abbreviation, '')) LIKE '%' || lower('{safe_term}') || '%' THEN 180
          ELSE 0
        END + ({group_token_score_expr}) AS score
      FROM active_groups ag
      LEFT JOIN {groups_from(category.category_id)} g
        ON g.groupId = ag.groupId
      WHERE (
        lower(COALESCE(g.name, '')) LIKE '%' || lower('{safe_term}') || '%'
        OR lower(regexp_replace(COALESCE(g.name, ''), '[^a-z0-9]+', '', 'g')) LIKE '%' || lower('{safe_normalized_term}') || '%'
        OR lower(COALESCE(g.abbreviation, '')) LIKE '%' || lower('{safe_term}') || '%'
        OR lower(regexp_replace(COALESCE(g.abbreviation, ''), '[^a-z0-9]+', '', 'g')) LIKE '%' || lower('{safe_normalized_term}') || '%'
        OR {group_token_match_expr}
      )
    ),
    scored AS (
      SELECT * FROM group_matches
      UNION ALL
      SELECT * FROM product_matches
    )
    SELECT *
    FROM scored
    WHERE score > 0
    ORDER BY set_focus_rank DESC, score DESC, kind ASC, lower(title) ASC
    LIMIT {limit}
    """
    cols, rows = q(sql)
    count_sql = f"""
    WITH active_products AS (
      SELECT
        productId,
        subTypeName,
        groupId
      FROM {product_signal_source}
    ),
    active_groups AS (
      SELECT
        groupId,
        COUNT(DISTINCT productId) AS productCount
      FROM {product_signal_source}
      GROUP BY groupId
    ),
    {metadata_cte},
    product_matches AS (
      SELECT 1 AS marker
      FROM active_products ap
      LEFT JOIN metadata m
        ON m.productId = ap.productId
       AND m.groupId = ap.groupId
      WHERE (
        lower(COALESCE(m.productName, '')) LIKE '%' || lower('{safe_term}') || '%'
        OR lower(regexp_replace(COALESCE(m.productName, ''), '[^a-z0-9]+', '', 'g')) LIKE '%' || lower('{safe_normalized_term}') || '%'
        OR lower(COALESCE(m.number, '')) LIKE '%' || lower('{safe_term}') || '%'
        OR lower(regexp_replace(COALESCE(m.number, ''), '[^a-z0-9]+', '', 'g')) LIKE '%' || lower('{safe_normalized_term}') || '%'
        OR lower(COALESCE(m.groupName, '')) LIKE '%' || lower('{safe_term}') || '%'
        OR lower(COALESCE(ap.subTypeName, '')) LIKE '%' || lower('{safe_term}') || '%'
        OR {product_token_match_expr}
      )
    ),
    group_matches AS (
      SELECT 1 AS marker
      FROM active_groups ag
      LEFT JOIN {groups_from(category.category_id)} g
        ON g.groupId = ag.groupId
      WHERE (
        lower(COALESCE(g.name, '')) LIKE '%' || lower('{safe_term}') || '%'
        OR lower(regexp_replace(COALESCE(g.name, ''), '[^a-z0-9]+', '', 'g')) LIKE '%' || lower('{safe_normalized_term}') || '%'
        OR lower(COALESCE(g.abbreviation, '')) LIKE '%' || lower('{safe_term}') || '%'
        OR lower(regexp_replace(COALESCE(g.abbreviation, ''), '[^a-z0-9]+', '', 'g')) LIKE '%' || lower('{safe_normalized_term}') || '%'
        OR {group_token_match_expr}
      )
    )
    SELECT COUNT(*)
    FROM (
      SELECT * FROM group_matches
      UNION ALL
      SELECT * FROM product_matches
    )
    """
    _, count_rows = q(count_sql)
    total_count = int(count_rows[0][0]) if count_rows else 0
    return {"items": [dict(zip(cols, row)) for row in rows], "query": term, "limit": limit, "total_count": total_count}


@app.get("/group_products")
def group_products(groupId: int, limit: int = 2000, product_kind: str | None = None, category_id: int = 3):
    limit = max(1, min(limit, 10000))
    category = category_config(category_id)
    price_source = prices_from(category.category_id)
    signal_source = product_signal_from(category.category_id)
    metadata_cte = build_metadata_cte(category.category_id, include_classification=True, cte_name="metadata")
    product_kind_filter = ""
    if product_kind in {"card", "sealed"}:
        product_kind_filter = f"AND m.productKind = '{product_kind}'"

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
    signal_names AS (
      SELECT
        productId,
        groupId,
        subTypeName,
        productName
      FROM {signal_source}
    ),
    {metadata_cte}
    SELECT
      lp.productId,
      lp.groupId,
      m.groupName,
      COALESCE(
        -- Prefer metadata names when they are real names, but drop known placeholders so
        -- we can fall back to fresher signal snapshot naming when available.
        CASE
          WHEN lower(trim(COALESCE(m.productName, ''))) IN (
            lower('product ' || CAST(lp.productId AS VARCHAR)),
            lower('productid ' || CAST(lp.productId AS VARCHAR))
          ) THEN NULL
          ELSE NULLIF(trim(m.productName), '')
        END,
        CASE
          WHEN lower(trim(COALESCE(sn.productName, ''))) IN (
            lower('product ' || CAST(lp.productId AS VARCHAR)),
            lower('productid ' || CAST(lp.productId AS VARCHAR))
          ) THEN NULL
          ELSE NULLIF(trim(sn.productName), '')
        END,
        'productId ' || CAST(lp.productId AS VARCHAR)
      ) AS productName,
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
    LEFT JOIN signal_names sn
      ON sn.productId = lp.productId
     AND sn.groupId = lp.groupId
     AND COALESCE(sn.subTypeName, '') = COALESCE(lp.subTypeName, '')
    WHERE 1 = 1
      {product_kind_filter}
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
    max_30d_pct: float = -5.0,
    max_90d_pct: float = 10.0,
    max_7d_pct: float = 3.0,
    min_recent_distinct_prices_30d: int = 10,
    floor_days: int = 7,
    min_floor_observations: int = 5,
    max_floor_variance_pct: float = 8.0,
    exclude_prize_packs: bool = False,
    product_kind: str | None = None,
    category_id: int = 3,
):
    limit = max(1, min(limit, 5000))
    min_recent_distinct_prices_30d = max(2, min(min_recent_distinct_prices_30d, 30))
    floor_days = max(5, min(floor_days, 10))
    min_floor_observations = max(3, min(min_floor_observations, floor_days))
    category = category_config(category_id)
    signal_source = product_signal_from(category.category_id)
    price_source = prices_from(category.category_id)
    premium_rarity_filter = build_premium_rarity_filter("rarity")
    prize_pack_filter = ""
    product_kind_filter = "AND productKind = 'card'"
    rarity_filter = f"AND {premium_rarity_filter}"
    if product_kind == "sealed":
        product_kind_filter = "AND productKind = 'sealed'"
        rarity_filter = ""
    elif product_kind == "card":
        product_kind_filter = "AND productKind = 'card'"
        rarity_filter = f"AND {premium_rarity_filter}"
    if exclude_prize_packs:
        prize_pack_filter = "AND lower(COALESCE(groupName, '')) NOT LIKE '%prize pack%'"

    sql = f"""
    WITH latest_signal AS (
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
        recent_distinct_prices_30d,
        price_vs_sma30_pct,
        trend_score
      FROM {signal_source}
    ),
    recent_prices AS (
      SELECT
        productId,
        groupId,
        subTypeName,
        date,
        marketPrice,
        ROW_NUMBER() OVER (
          PARTITION BY productId, groupId, subTypeName
          ORDER BY date DESC
        ) AS rn
      FROM {price_source}
      WHERE categoryId = {category.category_id}
        AND marketPrice IS NOT NULL
    ),
    floor_window AS (
      SELECT
        rp.productId,
        rp.groupId,
        rp.subTypeName,
        COUNT(*) AS floor_observations,
        MIN(rp.marketPrice) AS floor_low,
        MAX(rp.marketPrice) AS floor_high,
        AVG(rp.marketPrice) AS floor_avg
      FROM recent_prices rp
      JOIN latest_signal s
        ON s.productId = rp.productId
       AND s.groupId = rp.groupId
       AND s.subTypeName = rp.subTypeName
      WHERE s.categoryId = {category.category_id}
        AND s.latest_date = (SELECT MAX(latest_date) FROM {signal_source})
        AND rp.date >= s.latest_date - INTERVAL {floor_days} DAY
      GROUP BY rp.productId, rp.groupId, rp.subTypeName
    ),
    recent_lift AS (
      SELECT
        productId,
        groupId,
        subTypeName,
        MAX(CASE WHEN rn = 1 THEN marketPrice END) AS latest_price_1d,
        MAX(CASE WHEN rn = 2 THEN marketPrice END) AS latest_price_2d,
        MAX(CASE WHEN rn = 3 THEN marketPrice END) AS latest_price_3d,
        COUNT(*) FILTER (WHERE rn <= 3) AS recent_price_points
      FROM recent_prices
      WHERE rn <= 3
      GROUP BY productId, groupId, subTypeName
    )
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
      recent_distinct_prices_30d,
      price_vs_sma30_pct,
      trend_score
    FROM latest_signal s
    JOIN floor_window fw
      ON fw.productId = s.productId
     AND fw.groupId = s.groupId
     AND fw.subTypeName = s.subTypeName
    JOIN recent_lift rl
      ON rl.productId = s.productId
     AND rl.groupId = s.groupId
     AND rl.subTypeName = s.subTypeName
    WHERE latest_price >= {min_price}
      {product_kind_filter}
      {rarity_filter}
      AND COALESCE(roc_30d_pct, 0) <= {max_30d_pct}
      AND COALESCE(roc_90d_pct, 0) <= {max_90d_pct}
      AND COALESCE(roc_7d_pct, 0) <= {max_7d_pct}
      AND COALESCE(recent_distinct_prices_30d, 0) >= {min_recent_distinct_prices_30d}
      AND (
        (
          rl.recent_price_points >= 3
          AND rl.latest_price_1d < rl.latest_price_2d
          AND rl.latest_price_2d < rl.latest_price_3d
        )
        OR (
          fw.floor_observations >= {min_floor_observations}
          AND GREATEST(
            ABS(((fw.floor_high / NULLIF(latest_price, 0)) - 1) * 100.0),
            ABS(((fw.floor_low / NULLIF(latest_price, 0)) - 1) * 100.0)
          ) <= {max_floor_variance_pct}
        )
      )
      {prize_pack_filter}
    ORDER BY
      CASE
        WHEN fw.floor_observations >= {min_floor_observations}
         AND GREATEST(
           ABS(((fw.floor_high / NULLIF(latest_price, 0)) - 1) * 100.0),
           ABS(((fw.floor_low / NULLIF(latest_price, 0)) - 1) * 100.0)
         ) <= {max_floor_variance_pct}
        THEN 0 ELSE 1
      END ASC,
      GREATEST(
        ABS(((fw.floor_high / NULLIF(latest_price, 0)) - 1) * 100.0),
        ABS(((fw.floor_low / NULLIF(latest_price, 0)) - 1) * 100.0)
      ) ASC,
      roc_30d_pct ASC,
      price_vs_sma30_pct ASC,
      latest_price DESC
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/time_to_buy")
def time_to_buy(
    limit: int = 250,
    min_price: float = 5.0,
    lookback_days: int = 10,
    min_recent_observations: int = 10,
    min_recent_distinct_prices_30d: int = 10,
    max_variance_to_current_pct: float = 10.0,
    max_30d_pct: float = 0.0,
    max_90d_pct: float = 0.0,
    group_id: int | None = None,
    product_kind: str | None = None,
    category_id: int = 3,
):
    limit = max(1, min(limit, 5000))
    lookback_days = max(5, min(lookback_days, 30))
    min_recent_observations = max(3, min(min_recent_observations, lookback_days))
    min_recent_distinct_prices_30d = max(2, min(min_recent_distinct_prices_30d, 30))
    if group_id is None:
        return {"columns": [], "rows": []}
    category = category_config(category_id)
    signal_source = product_signal_from(category.category_id)
    price_source = prices_from(category.category_id)
    metadata_cte = build_metadata_cte(category.category_id, include_classification=True, cte_name="metadata")
    product_kind_filter = ""
    group_filter = ""
    if product_kind in {"card", "sealed"}:
        product_kind_filter = f"AND COALESCE(m.productKind, s.productKind, '') = '{product_kind}'"
    if group_id is not None:
        group_filter = f"AND s.groupId = {int(group_id)}"

    sql = f"""
    WITH latest_signal AS (
      SELECT
        latest_date,
        productId,
        groupId,
        subTypeName,
        productKind,
        latest_price,
        latest_sma30,
        roc_7d_pct,
        roc_30d_pct,
        roc_90d_pct,
        price_vs_sma30_pct,
        recent_distinct_prices_30d
      FROM {signal_source}
      WHERE categoryId = {category.category_id}
        AND latest_date = (SELECT MAX(latest_date) FROM {signal_source})
        AND latest_price >= {min_price}
        AND latest_sma30 IS NOT NULL
        AND latest_price <= latest_sma30
        AND COALESCE(roc_30d_pct, 0) <= {max_30d_pct}
        AND COALESCE(roc_90d_pct, 0) <= {max_90d_pct}
        AND COALESCE(recent_distinct_prices_30d, 0) >= {min_recent_distinct_prices_30d}
    ),
    recent_window AS (
      SELECT
        p.productId,
        p.groupId,
        p.subTypeName,
        COUNT(*) AS recent_observations,
        COUNT(DISTINCT p.date) AS recent_days,
        MIN(p.marketPrice) AS recent_low,
        MAX(p.marketPrice) AS recent_high,
        AVG(p.marketPrice) AS recent_avg
      FROM {price_source} p
      JOIN latest_signal s
        ON p.productId = s.productId
       AND p.groupId = s.groupId
       AND p.subTypeName = s.subTypeName
      WHERE p.categoryId = {category.category_id}
        AND p.marketPrice IS NOT NULL
        AND p.date >= s.latest_date - INTERVAL {lookback_days - 1} DAY
      GROUP BY p.productId, p.groupId, p.subTypeName
    ),
    {metadata_cte}
    SELECT
      s.latest_date,
      s.groupId,
      COALESCE(m.groupName, 'Unknown Group') AS groupName,
      s.productId,
      COALESCE(m.productName, 'productId ' || CAST(s.productId AS VARCHAR)) AS productName,
      m.imageUrl,
      m.rarity,
      m.number,
      COALESCE(m.productClass, '') AS productClass,
      COALESCE(m.productKind, '') AS productKind,
      s.subTypeName,
      s.latest_price,
      s.latest_sma30,
      s.roc_7d_pct,
      s.roc_30d_pct,
      s.roc_90d_pct,
      s.price_vs_sma30_pct,
      s.recent_distinct_prices_30d,
      rw.recent_observations,
      rw.recent_days,
      rw.recent_low,
      rw.recent_high,
      rw.recent_avg,
      ((rw.recent_high - rw.recent_low) / NULLIF(s.latest_price, 0)) * 100.0 AS recent_range_pct,
      GREATEST(
        ABS(((rw.recent_high / NULLIF(s.latest_price, 0)) - 1) * 100.0),
        ABS(((rw.recent_low / NULLIF(s.latest_price, 0)) - 1) * 100.0)
      ) AS variance_to_current_pct
    FROM latest_signal s
    JOIN recent_window rw
      ON rw.productId = s.productId
     AND rw.groupId = s.groupId
     AND rw.subTypeName = s.subTypeName
    LEFT JOIN metadata m
      ON m.productId = s.productId
     AND m.groupId = s.groupId
    WHERE rw.recent_observations >= {min_recent_observations}
      AND GREATEST(
        ABS(((rw.recent_high / NULLIF(s.latest_price, 0)) - 1) * 100.0),
        ABS(((rw.recent_low / NULLIF(s.latest_price, 0)) - 1) * 100.0)
      ) <= {max_variance_to_current_pct}
      {product_kind_filter}
      {group_filter}
    ORDER BY variance_to_current_pct ASC, s.roc_90d_pct ASC, s.latest_price DESC
    LIMIT {limit}
    """
    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/group_signals")
def group_signals(limit: int = 500, min_items: int = 5, generation: str | None = None, category_id: int = 3):
    limit = max(1, min(limit, 5000))
    min_items = max(1, min(min_items, 1000))
    category = category_config(category_id)
    generation_case = build_generation_case("g.groupId", "g.name", "g.abbreviation", "g.publishedOn")
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
def set_baskets(
    limit: int = 500,
    min_cards: int = 10,
    filters: str | None = None,
    category_id: int = 3,
):
    """Return a lightweight set-completion and concentration view for the fun set explorer page."""
    limit = max(1, min(limit, 2000))
    min_cards = max(1, min(min_cards, 400))
    category = category_config(category_id)
    generation_case = build_generation_case("g.groupId", "g.name", "g.abbreviation", "g.publishedOn")
    active_filters = [part.strip() for part in (filters or "").split(",") if part.strip()]
    basket_filter = build_set_basket_filter(active_filters, "s.rarity", "s.subTypeName", "s.productName")

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
          AND {basket_filter}
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
        "filters": active_filters,
    }


@app.get("/sealed_deals")
def sealed_deals(
    category_id: int = 3,
    limit: int = 600,
    min_price: float = 5.0,
    max_price: float = 500.0,
    include_low_confidence: bool = False,
    include_trick_or_trade: bool = False,
):
    """Return normalized sealed-product deal rows with inferred pack count and retail baseline."""
    category = category_config(category_id)
    is_japanese_market = category.category_id == 85
    limit = max(1, min(limit, 2000))
    min_price = max(0.0, min_price)
    max_price = max(min_price, max_price)
    signal_source = _product_signal_source_resilient(category.category_id)

    def build_sql(source: str) -> str:
        return f"""
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
      latest_price
    FROM {signal_source}
    WHERE productKind IN ('sealed', 'mcap')
      AND latest_price IS NOT NULL
      AND latest_price >= {min_price}
      AND latest_price <= {max_price}
    ORDER BY latest_price ASC
    LIMIT {limit * 6}
    """
    sql = build_sql(signal_source)
    try:
        cols, rows = q(sql)
    except Exception as err:
        # If the dashboard DB file is locked, rerun this endpoint against CSV snapshots
        # using an in-memory DuckDB connection so the page keeps loading.
        message = str(err).lower()
        if "could not set lock on file" not in message and "conflicting lock is held" not in message:
            raise
        csv_source = _product_signal_source_resilient(category.category_id)
        cols, rows = _query_with_in_memory_duckdb(build_sql(csv_source))
    records = [dict(zip(cols, row)) for row in rows]

    items = []
    for row in records:
        name = str(row.get("productName") or "")
        lower_name = name.lower()
        if _should_exclude_sealed_name(name):
            continue
        if not include_trick_or_trade and _is_trick_or_trade_product(name):
            continue
        if _looks_like_individual_card(name, row.get("rarity"), row.get("number")):
            continue

        latest_price = _safe_float(row.get("latest_price"))
        if latest_price is None:
            continue
        product_class = str(row.get("productClass") or "")
        product_id = int(row.get("productId")) if row.get("productId") is not None else None
        pack_count_override = _find_pack_count_override(product_id) or _find_pack_count_override_by_name(name)
        composition_override = _find_pack_composition_override(name)
        packs = []
        if pack_count_override and isinstance(pack_count_override.get("packs"), list):
            packs = pack_count_override.get("packs", [])
        elif composition_override and isinstance(composition_override.get("packs"), list):
            packs = composition_override.get("packs", [])
        if pack_count_override and pack_count_override.get("pack_count"):
            pack_count = int(pack_count_override.get("pack_count"))
        elif packs:
            pack_count = sum(int(pack.get("count", 0)) for pack in packs if int(pack.get("count", 0)) > 0)
        else:
            pack_count = _infer_pack_count(name, product_class, category.category_id, str(row.get("groupName") or ""))
            pack_set = str(row.get("groupName") or "Unknown Set")
            packs = [{"set": pack_set, "count": int(pack_count or 0)}] if pack_count else []
        if pack_count is None or pack_count <= 0:
            continue

        price_per_pack = latest_price / pack_count
        if is_japanese_market:
            _, jp_yen_per_pack = _jp_pricing_profile(name, str(row.get("groupName") or ""))
            # Convert JPY MSRP to USD for comparison with USD market prices.
            retail_per_pack = jp_yen_per_pack * _JPY_TO_USD_RATE
            if retail_per_pack > 0:
                msrp_estimate = retail_per_pack * pack_count
                savings_dollar = msrp_estimate - latest_price
                savings_pct = (savings_dollar / msrp_estimate) * 100 if msrp_estimate else None
                deal_score = retail_per_pack - price_per_pack
            else:
                msrp_estimate = None
                savings_dollar = None
                savings_pct = None
                deal_score = None
        else:
            msrp_total_override = (
                _safe_float(pack_count_override.get("msrp_total"))
                if pack_count_override and pack_count_override.get("msrp_total") is not None
                else None
            )
            inferred_product_type = (
                str(pack_count_override.get("product_type"))
                if pack_count_override and pack_count_override.get("product_type")
                else str(composition_override.get("product_type"))
                if composition_override and composition_override.get("product_type")
                else _infer_product_type(name, product_class)
            )
            retail_per_pack = _infer_retail_per_pack(name, product_class, inferred_product_type)
            if msrp_total_override is not None and pack_count > 0:
                msrp_estimate = msrp_total_override
                retail_per_pack = msrp_estimate / pack_count
            else:
                msrp_estimate = None
            if retail_per_pack <= 0:
                continue
            if msrp_estimate is None:
                msrp_estimate = retail_per_pack * pack_count
            if msrp_estimate <= 0:
                continue
            savings_dollar = msrp_estimate - latest_price
            savings_pct = (savings_dollar / msrp_estimate) * 100 if msrp_estimate else None
            deal_score = retail_per_pack - price_per_pack

        inferred_type = inferred_product_type if not is_japanese_market else (
            str(pack_count_override.get("product_type"))
            if pack_count_override and pack_count_override.get("product_type")
            else str(composition_override.get("product_type"))
            if composition_override and composition_override.get("product_type")
            else _infer_product_type(name, product_class)
        )
        if pack_count_override or composition_override:
            confidence = "high"
        elif "[" in name and ("set of" in lower_name or "bundle of" in lower_name):
            confidence = "high"
        elif inferred_type in {"Booster Pack", "Booster Box", "Booster Bundle", "Elite Trainer Box", "Pokemon Center ETB", "Ultra Premium Collection", "Premium Collection"}:
            confidence = "high"
        elif inferred_type in {"Tin", "Mini Tin", "Collection Box", "Pack Blister"}:
            confidence = "medium"
        else:
            confidence = "low"

        if not include_low_confidence and confidence == "low":
            continue

        items.append(
            {
                "latest_date": str(row.get("latest_date") or ""),
                "groupId": row.get("groupId"),
                "groupName": row.get("groupName"),
                "productId": row.get("productId"),
                "productName": name,
                "imageUrl": row.get("imageUrl"),
                "productClass": product_class,
                "productType": inferred_type,
                "subTypeName": row.get("subTypeName"),
                "latest_price": round(latest_price, 2),
                "pack_count": int(pack_count),
                "pack_mix": _pack_mix_text(packs),
                "pack_sets": [str(pack.get("set")) for pack in packs if pack.get("set")],
                "price_per_pack": round(price_per_pack, 3),
                "retail_per_pack": round(retail_per_pack, 3) if retail_per_pack is not None else None,
                "msrp_estimate": round(msrp_estimate, 2) if msrp_estimate is not None else None,
                "savings_dollar": round(savings_dollar, 2) if savings_dollar is not None else None,
                "savings_pct": round(savings_pct, 2) if savings_pct is not None else None,
                "premium_pct": round((-savings_pct), 2) if savings_pct is not None else None,
                "deal_score": round(deal_score, 3) if deal_score is not None else None,
                "jp_value_score": None,
                "confidence": confidence,
            }
        )

    if is_japanese_market and items:
        ranked = sorted(items, key=lambda item: float(item.get("price_per_pack") or 10_000))
        denom = max(1, len(ranked) - 1)
        for index, item in enumerate(ranked):
            score = 100.0 - ((index / denom) * 100.0)
            item["jp_value_score"] = round(score, 2)
        items = sorted(ranked, key=lambda item: (-float(item.get("jp_value_score") or 0), float(item.get("price_per_pack") or 10_000)))
    else:
        items.sort(
            key=lambda item: (
                -float(item.get("deal_score") or 0),
                -float(item.get("savings_pct") or 0),
                float(item.get("price_per_pack") or 10_000),
            )
        )
    items = items[:limit]

    def _pick_best(rows, key):
        return max(rows, key=key) if rows else None
    def _pick_worst(rows, key):
        return min(rows, key=key) if rows else None

    if is_japanese_market:
        stats = {
            "count": len(items),
            "best_jp_value": _pick_best(items, lambda item: float(item.get("jp_value_score") or 0)),
            "cheapest_pack": min(items, key=lambda item: float(item.get("price_per_pack") or 10_000)) if items else None,
            "most_expensive_pack": _pick_best(items, lambda item: float(item.get("price_per_pack") or -1)),
            "avg_price_per_pack": round(sum(float(item["price_per_pack"]) for item in items) / len(items), 3) if items else None,
            "worst_deal": _pick_worst(items, lambda item: float(item.get("savings_pct") or 9_999)),
            "highest_premium_pct": _pick_best(items, lambda item: float(item.get("premium_pct") or -1_000)),
            "below_retail_count": None,
        }
    else:
        stats = {
            "count": len(items),
            "best_deal": _pick_best(items, lambda item: float(item.get("deal_score") or 0)),
            "biggest_pct_discount": _pick_best(items, lambda item: float(item.get("savings_pct") or -1_000)),
            "cheapest_pack": min(items, key=lambda item: float(item.get("price_per_pack") or 10_000)) if items else None,
            "most_expensive_pack": _pick_best(items, lambda item: float(item.get("price_per_pack") or -1)),
            "avg_price_per_pack": round(sum(float(item["price_per_pack"]) for item in items) / len(items), 3) if items else None,
            "worst_deal": _pick_worst(items, lambda item: float(item.get("deal_score") or 9_999)),
            "highest_premium_pct": _pick_best(items, lambda item: float(item.get("premium_pct") or -1_000)),
            "below_retail_count": sum(1 for item in items if float(item.get("savings_dollar") or 0) > 0),
        }

    return {
        "items": to_jsonable(items),
        "stats": to_jsonable(stats),
        "meta": {
            "category_id": category.category_id,
            "category": category.label,
            "limit": limit,
            "min_price": min_price,
            "max_price": max_price,
            "include_trick_or_trade": include_trick_or_trade,
            "heuristics": "v1",
            "notes": "Pack count is inferred from product naming patterns. JP mode uses MSRP tiers (regular/high-class/151) converted from JPY to USD for comparison.",
        },
    }


@app.get("/series")
def series(productId: int, subTypeName: str, days: int = 365, category_id: int = 3):
    days = max(7, min(days, 5000))
    st = subTypeName.replace("'", "''")
    category = category_config(category_id)
    if days <= 730:
        sql = f"""
            SELECT
                latest_date,
                dates_json,
                prices_json,
                sma7_json,
                sma30_json
            FROM {series_snapshot_from(category.category_id)}
            WHERE categoryId = {category.category_id}
              AND productId = {productId}
              AND subTypeName = '{st}'
              AND latest_date = (SELECT MAX(latest_date) FROM {series_snapshot_from(category.category_id)})
            LIMIT 1
        """
        _, rows = q(sql)
        if rows:
            latest, dates_json, prices_json, sma7_json, sma30_json = rows[0]
            dates = json.loads(dates_json or "[]")
            prices = json.loads(prices_json or "[]")
            sma7 = json.loads(sma7_json or "[]")
            sma30 = json.loads(sma30_json or "[]")
            slice_len = min(days, len(dates))
            rows_out = list(zip(
                dates[-slice_len:],
                prices[-slice_len:],
                sma7[-slice_len:],
                sma30[-slice_len:],
            ))
            start = dates[-slice_len] if slice_len else None
            # If the cached snapshot is too sparse, fall back to the raw prices table
            # so thin snapshots (like 3-4 points) don't flatten the chart view.
            if len(rows_out) >= min(7, days):
                return {
                    "columns": ["date", "price", "sma7", "sma30"],
                    "rows": rows_out,
                    "latest": str(latest),
                    "start": str(start) if start is not None else None,
                }

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


@app.post("/sparkline_batch")
def sparkline_batch(payload: dict, category_id: int = 3):
    """Return minimal rolling close-price series for many products in a single request."""
    raw_items = payload.get("items") or []
    days = max(7, min(int(payload.get("days", 90) or 90), 365))
    if not isinstance(raw_items, list) or not raw_items:
        return {"days": days, "items": {}}

    category = category_config(category_id)
    requested = []
    seen = set()
    for item in raw_items[:50]:
      try:
          product_id = int(item.get("productId"))
      except (TypeError, ValueError):
          continue
      sub_type_name = str(item.get("subTypeName") or "")
      dedupe_key = (product_id, sub_type_name)
      if dedupe_key in seen:
          continue
      seen.add(dedupe_key)
      requested.append(dedupe_key)

    if not requested:
        return {"days": days, "items": {}}

    filters = []
    key_map = {}
    for product_id, sub_type_name in requested:
        safe_sub_type = sub_type_name.replace("'", "''")
        filters.append(f"(productId = {product_id} AND subTypeName = '{safe_sub_type}')")
        key_map[f"{product_id}||{sub_type_name}"] = []

    sql = f"""
    SELECT
      productId,
      subTypeName,
      prices_json
    FROM {sparkline_snapshot_from(category.category_id)}
    WHERE categoryId = {category.category_id}
      AND latest_date = (SELECT MAX(latest_date) FROM {sparkline_snapshot_from(category.category_id)})
      AND ({' OR '.join(filters)})
    """
    _, rows = q(sql)
    for product_id, sub_type_name, prices_json in rows:
        try:
            values = json.loads(prices_json or "[]")
        except json.JSONDecodeError:
            values = []
        key_map[f"{product_id}||{sub_type_name}"] = values[-days:]
    return {"days": days, "items": key_map}


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
    min_recent_observations = max(2, min(min_recent_observations, 30))
    min_recent_distinct_prices = max(2, min(min_recent_distinct_prices, 15))
    recent_variation_window_days = max(3, min(recent_variation_window_days, 30))
    use_snapshot = (
        days == 30
        and abs(min_daily_move_pct - 1.0) < 1e-9
        and recent_variation_window_days == 14
    )

    if use_snapshot:
        product_kind_filter = ""
        if product_kind in {"card", "sealed"}:
            product_kind_filter = f"AND productKind = '{product_kind}'"
        sql = f"""
        SELECT
            productId,
            subTypeName,
            groupId,
            groupName,
            productName,
            imageUrl,
            rarity,
            number,
            productClass,
            productKind,
            latest_price AS p_now,
            price_30d AS p_prior,
            roc_30d_pct AS roc_pct,
            top_mover_signal_days AS signal_days,
            top_mover_recent_observations AS recent_observations,
            top_mover_recent_distinct_prices AS recent_distinct_prices,
            top_mover_recent_points AS recent_points,
            top_mover_last_change_date AS last_change_date
        FROM {product_signal_from(category.category_id)}
        WHERE categoryId = {category.category_id}
          AND latest_date = (SELECT MAX(latest_date) FROM {product_signal_from(category.category_id)})
          AND latest_price IS NOT NULL
          AND price_30d IS NOT NULL
          AND price_30d >= {min_prior}
          AND COALESCE(top_mover_signal_days, 0) >= {min_signal_days}
          AND COALESCE(top_mover_recent_observations, 0) >= {min_recent_observations}
          AND COALESCE(top_mover_recent_distinct_prices, 0) >= {min_recent_distinct_prices}
          {product_kind_filter}
          AND (
            NOT {1 if require_recent_change else 0}
            OR (
              top_mover_last_change_date IS NOT NULL
              AND top_mover_last_change_date >= latest_date - INTERVAL {recent_change_within_days} DAY
            )
          )
        ORDER BY roc_30d_pct DESC
        LIMIT {limit}
        """
        cols, rows = q(sql)
        return {"columns": cols, "rows": rows}

    price_source = prices_from(category.category_id)
    product_kind_filter = ""
    if product_kind in {"card", "sealed"}:
        product_kind_filter = f"AND m.productKind = '{product_kind}'"
    metadata_cte = build_metadata_cte(category.category_id, include_classification=True, cte_name="metadata")

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
def breakouts(
    days: int = 90,
    limit: int = 200,
    min_price: float = 5.0,
    min_breakout_pct: float = 1.0,
    recent_change_within_days: int = 5,
    min_recent_distinct_prices_30d: int = 10,
    product_kind: str | None = None,
    category_id: int = 3,
):
    category = category_config(category_id)
    price_source = prices_from(category.category_id)
    metadata_cte = build_metadata_cte(category.category_id, include_classification=True, cte_name="metadata")
    product_kind_filter = ""
    if product_kind in {"card", "sealed"}:
        product_kind_filter = f"AND m.productKind = '{product_kind}'"
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
            groupId,
            subTypeName,
            MAX(date) AS latest_date
        FROM base
        GROUP BY productId, groupId, subTypeName
    ),
    win AS (
        SELECT
            b.productId,
            b.groupId,
            b.subTypeName,
            l.latest_date,
            b.marketPrice
        FROM base b
        JOIN latest l
          ON b.productId = l.productId
         AND b.groupId = l.groupId
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
    prior_hi AS (
        SELECT
            b.productId,
            b.groupId,
            b.subTypeName,
            MAX(b.marketPrice) AS prior_high_n,
            MAX(b.date) AS last_change_date
        FROM base b
        JOIN latest l
          ON b.productId = l.productId
         AND b.groupId = l.groupId
         AND b.subTypeName = l.subTypeName
        WHERE b.date >= l.latest_date - INTERVAL {days} DAY
          AND b.date < l.latest_date
        GROUP BY b.productId, b.groupId, b.subTypeName
    ),
    recent_activity AS (
        -- Breakouts should reflect real price discovery, not a single flat print at a range high.
        -- This 30-day activity slice lets the screener require repeated distinct prices.
        SELECT
            b.productId,
            b.groupId,
            b.subTypeName,
            COUNT(*) AS recent_observations_30d,
            COUNT(DISTINCT b.marketPrice) AS recent_distinct_prices_30d
        FROM base b
        JOIN latest l
          ON b.productId = l.productId
         AND b.groupId = l.groupId
         AND b.subTypeName = l.subTypeName
        WHERE b.date >= l.latest_date - INTERVAL 30 DAY
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
        m.productClass,
        m.productKind,
        w.marketPrice AS latest_price,
        ph.prior_high_n AS prior_high_window,
        ((w.marketPrice / NULLIF(ph.prior_high_n, 0)) - 1) * 100.0 AS breakout_pct,
        ph.last_change_date,
        ra.recent_observations_30d,
        ra.recent_distinct_prices_30d
    FROM win w
    JOIN hi h
      ON w.productId = h.productId
     AND w.groupId = h.groupId
     AND w.subTypeName = h.subTypeName
    JOIN prior_hi ph
      ON w.productId = ph.productId
     AND w.groupId = ph.groupId
     AND w.subTypeName = ph.subTypeName
    JOIN recent_activity ra
      ON w.productId = ra.productId
     AND w.groupId = ra.groupId
     AND w.subTypeName = ra.subTypeName
    LEFT JOIN metadata m
      ON m.productId = w.productId
     AND m.groupId = w.groupId
    -- A breakout now means "freshly above the previous 90-day high with recent activity",
    -- not merely "equal to the highest observed price in the window."
    WHERE w.marketPrice >= h.high_n
      AND w.marketPrice > ph.prior_high_n
      AND ((w.marketPrice / NULLIF(ph.prior_high_n, 0)) - 1) * 100.0 >= {min_breakout_pct}
      AND ph.last_change_date >= w.latest_date - INTERVAL {recent_change_within_days} DAY
      AND ra.recent_distinct_prices_30d >= {min_recent_distinct_prices_30d}
      {product_kind_filter}
    ORDER BY breakout_pct DESC, w.marketPrice DESC
    LIMIT {limit}
    """

    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/sma30_holds")
def sma30_holds(
    days_required: int = 30,
    limit: int = 200,
    min_price: float = 5.0,
    min_recent_distinct_prices_30d: int = 10,
    product_kind: str | None = None,
    category_id: int = 3,
):
    days_required = max(1, min(days_required, 30))
    limit = max(1, min(limit, 1000))
    min_recent_distinct_prices_30d = max(2, min(min_recent_distinct_prices_30d, 30))
    category = category_config(category_id)
    source = product_signal_from(category.category_id)
    product_kind_filter = ""
    if product_kind in {"card", "sealed"}:
        product_kind_filter = f"AND productKind = '{product_kind}'"
    sql = f"""
    SELECT
        productId,
        groupId,
        subTypeName,
        groupName,
        productName,
        imageUrl,
        rarity,
        number,
        cross_date,
        hold_days,
        recent_distinct_prices_30d,
        latest_price,
        latest_sma30,
        CASE WHEN latest_sma30 IS NULL OR latest_sma30 = 0 THEN NULL
             ELSE ((latest_price / latest_sma30) - 1) * 100 END AS pct_vs_sma30
    FROM {source}
    WHERE categoryId = {category.category_id}
      AND latest_date = (SELECT MAX(latest_date) FROM {source})
      AND latest_price >= {min_price}
      AND hold_days >= {days_required}
      AND latest_sma30 IS NOT NULL
      AND COALESCE(recent_distinct_prices_30d, 0) >= {min_recent_distinct_prices_30d}
      {product_kind_filter}
    ORDER BY hold_days DESC, pct_vs_sma30 DESC
    LIMIT {limit}
    """

    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/confirmed_uptrends")
def confirmed_uptrends(
    days_required: int = 5,
    limit: int = 200,
    min_price: float = 5.0,
    product_kind: str | None = None,
    category_id: int = 3,
):
    days_required = max(1, min(days_required, 30))
    limit = max(1, min(limit, 1000))
    category = category_config(category_id)
    source = product_signal_from(category.category_id)
    product_kind_filter = ""
    if product_kind in {"card", "sealed"}:
        product_kind_filter = f"AND productKind = '{product_kind}'"
    sql = f"""
    SELECT
        productId,
        groupId,
        subTypeName,
        groupName,
        productName,
        imageUrl,
        rarity,
        number,
        bullish_streak,
        latest_price,
        latest_sma7,
        latest_sma30,
        CASE WHEN latest_sma30 IS NULL OR latest_sma30 = 0 THEN NULL
             ELSE ((latest_price / latest_sma30) - 1) * 100 END AS pct_vs_sma30
    FROM {source}
    WHERE categoryId = {category.category_id}
      AND latest_date = (SELECT MAX(latest_date) FROM {source})
      AND latest_price >= {min_price}
      AND bullish_streak >= {days_required}
      AND latest_sma30 IS NOT NULL
      {product_kind_filter}
    ORDER BY bullish_streak DESC, pct_vs_sma30 DESC
    LIMIT {limit}
    """

    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}


@app.get("/early_uptrends")
def early_uptrends(
    days_required: int = 3,
    limit: int = 200,
    min_price: float = 5.0,
    max_price_vs_sma30_pct: float = 8.0,
    min_7d_pct: float = 1.0,
    max_7d_pct: float = 10.0,
    max_30d_pct: float = 12.0,
    max_90d_pct: float = 25.0,
    min_acceleration_7d_vs_30d: float = 0.5,
    min_recent_distinct_prices_30d: int = 10,
    min_recent_observations: int = 3,
    recent_change_within_days: int = 5,
    product_kind: str | None = None,
    category_id: int = 3,
):
    days_required = max(1, min(days_required, 15))
    limit = max(1, min(limit, 1000))
    min_recent_distinct_prices_30d = max(2, min(min_recent_distinct_prices_30d, 30))
    min_recent_observations = max(2, min(min_recent_observations, 10))
    recent_change_within_days = max(1, min(recent_change_within_days, 15))
    category = category_config(category_id)
    source = product_signal_from(category.category_id)
    price_source = prices_from(category.category_id)
    product_kind_filter = ""
    if product_kind in {"card", "sealed"}:
        product_kind_filter = f"AND productKind = '{product_kind}'"
    sql = f"""
    WITH recent_prices AS (
      SELECT
        productId,
        groupId,
        subTypeName,
        marketPrice,
        ROW_NUMBER() OVER (
          PARTITION BY productId, groupId, subTypeName
          ORDER BY date DESC
        ) AS rn
      FROM {price_source}
      WHERE categoryId = {category.category_id}
        AND marketPrice IS NOT NULL
    ),
    recent_lift AS (
      SELECT
        productId,
        groupId,
        subTypeName,
        MAX(CASE WHEN rn = 1 THEN marketPrice END) AS latest_price_1d,
        MAX(CASE WHEN rn = 2 THEN marketPrice END) AS latest_price_2d,
        MAX(CASE WHEN rn = 3 THEN marketPrice END) AS latest_price_3d,
        COUNT(*) FILTER (WHERE rn <= 3) AS recent_price_points
      FROM recent_prices
      WHERE rn <= 3
      GROUP BY productId, groupId, subTypeName
    )
    SELECT
        s.productId,
        s.groupId,
        s.subTypeName,
        s.groupName,
        s.productName,
        s.imageUrl,
        s.rarity,
        s.number,
        s.early_streak,
        s.recent_observations_7d AS recent_observations,
        s.recent_distinct_prices_7d,
        s.recent_distinct_prices_30d,
        s.last_change_date,
        s.latest_price,
        s.roc_7d_pct,
        s.roc_30d_pct,
        s.roc_90d_pct,
        s.acceleration_7d_vs_30d,
        s.latest_sma3,
        s.latest_sma7,
        s.latest_sma30,
        rl.latest_price_1d,
        rl.latest_price_2d,
        rl.latest_price_3d,
        CASE WHEN latest_sma30 IS NULL OR latest_sma30 = 0 THEN NULL
             ELSE ((latest_price / latest_sma30) - 1) * 100 END AS pct_vs_sma30
    FROM {source} s
    JOIN recent_lift rl
      ON rl.productId = s.productId
     AND rl.groupId = s.groupId
     AND rl.subTypeName = s.subTypeName
    WHERE s.categoryId = {category.category_id}
      AND s.latest_date = (SELECT MAX(latest_date) FROM {source})
      AND s.latest_price >= {min_price}
      AND s.early_streak >= {days_required}
      AND s.latest_sma30 IS NOT NULL
      AND COALESCE(s.recent_observations_7d, 0) >= {min_recent_observations}
      AND COALESCE(s.recent_distinct_prices_7d, 0) >= 2
      AND COALESCE(s.recent_distinct_prices_30d, 0) >= {min_recent_distinct_prices_30d}
      AND s.last_change_date IS NOT NULL
      AND s.last_change_date >= s.latest_date - INTERVAL {recent_change_within_days} DAY
      AND COALESCE(s.roc_7d_pct, 0) >= {min_7d_pct}
      AND COALESCE(s.roc_7d_pct, 0) <= {max_7d_pct}
      AND COALESCE(s.roc_30d_pct, 0) <= {max_30d_pct}
      AND COALESCE(s.roc_90d_pct, 0) <= {max_90d_pct}
      AND COALESCE(s.acceleration_7d_vs_30d, 0) >= {min_acceleration_7d_vs_30d}
      AND rl.recent_price_points >= 3
      AND rl.latest_price_1d > rl.latest_price_2d
      AND rl.latest_price_2d > rl.latest_price_3d
      AND ((s.latest_price / NULLIF(s.latest_sma30, 0)) - 1) * 100 <= {max_price_vs_sma30_pct}
      {product_kind_filter}
    ORDER BY s.acceleration_7d_vs_30d DESC, pct_vs_sma30 ASC, s.roc_30d_pct ASC, s.latest_price DESC
    LIMIT {limit}
    """

    cols, rows = q(sql)
    return {"columns": cols, "rows": rows}
