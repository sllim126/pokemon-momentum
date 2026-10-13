import csv
import math
import threading
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
from fastapi import HTTPException

from scripts.common.category_config import get_category_config
from scripts.common.product_classification import get_product_class_sql, get_product_kind_sql


DATA_ROOT = Path("/app/data")
EXTRACTED_DIR = DATA_ROOT / "extracted"
PROCESSED_DIR = DATA_ROOT / "processed"
OUTPUT_DIR = Path("/app/output")
DB_PATH = PROCESSED_DIR / "prices_db.duckdb"
PARQUET_ROOT = DATA_ROOT / "parquet"
PARQUET_GLOB = str(PARQUET_ROOT / "**/*.parquet")

PRODUCT_CLASS_SQL = get_product_class_sql("p")
PRODUCT_KIND_SQL = get_product_kind_sql("p")

# Exact set-level overrides are safer than trying to keep stretching the era
# heuristics to fit every promo, subset, or custom supplemental release.
# This table is expected to grow as real-world metadata edge cases are found,
# especially for JP Mega-era releases that use multiple parallel prefixes.
GENERATION_OVERRIDES = {
    1522: "XY",      # Kalos Starter Set is the XY-series introduction despite its 2013 date.
    17688: "SWSH",   # Crown Zenith
    17689: "SWSH",   # Crown Zenith: Galarian Gallery
    1384: "DP/HGSS", # Supreme Victors
    1414: "POP",     # POP Series 7
    1422: "POP",     # POP Series 1
    1432: "POP",     # POP Series 6
    1439: "POP",     # POP Series 5
    1442: "POP",     # POP Series 3
    1446: "POP",     # POP Series 9
    1447: "POP",     # POP Series 2
    1450: "POP",     # POP Series 8
    1452: "POP",     # POP Series 4
    24380: "MEG",    # ME01: Mega Evolution
    24451: "MEG",    # ME: Mega Evolution Promo
    24448: "MEG",    # ME02: Phantasmal Flames
    24444: "MEG",    # MBG: MEGA Starter Set Mega Gengar ex
    24445: "MEG",    # MBD: MEGA Starter Set Mega Diancie ex
    24541: "MEG",    # ME: Ascended Heroes
    24461: "MEG",    # MEE: Mega Evolution Energies
    24459: "MEG",    # M2: Inferno X
    24499: "MEG",    # M2a: High Class Pack: MEGA Dream ex
    24607: "MEG",    # MP1: Start Deck 100 Battle Collection CoroCiao Version
    24587: "MEG",    # ME03: Perfect Order
    24600: "MEG",    # M3: Nihil Zero
    24655: "MEG",    # ME04: Chaos Rising
}


def category_config(category_id: int):
    return get_category_config(category_id)


# ---- Cached source checks and the shared database connection ------------------------
#
# Every request used to open the 13 GB DuckDB file several times just to ask which tables
# exist (~100 ms each), query the parquet history to see whether a market is in it
# (~270 ms), and open the file again for each query (~80 ms). Those answers only change
# when the nightly update rebuilds the data, and the nightly update stops the website
# first, so they are worked out once and remembered. SOURCE_CHECK_TTL_SECONDS is a
# backstop in case the data is rebuilt while the site is running. Failed checks (for
# example a locked database) are never remembered.
SOURCE_CHECK_TTL_SECONDS = 600
_table_names_cache: tuple[float, frozenset] | None = None
_parquet_category_cache: dict[int, tuple[float, bool]] = {}
_shared_con = None
_shared_con_lock = threading.Lock()


def clear_source_caches() -> None:
    """Forget the cached table list and parquet checks and close the shared connection."""
    global _table_names_cache, _shared_con
    _table_names_cache = None
    _parquet_category_cache.clear()
    _release_dates_cache.clear()
    with _shared_con_lock:
        if _shared_con is not None:
            try:
                _shared_con.close()
            except Exception:  # noqa: BLE001 - closing a dead connection is fine
                pass
        _shared_con = None


def _fresh(cached_at: float) -> bool:
    return time.monotonic() - cached_at < SOURCE_CHECK_TTL_SECONDS


def has_parquet() -> bool:
    return PARQUET_ROOT.exists() and any(PARQUET_ROOT.rglob("*.parquet"))


def parquet_has_category(category_id: int) -> bool:
    """Whether the parquet price history has rows for a category (remembered; see above)."""
    cached = _parquet_category_cache.get(category_id)
    if cached and _fresh(cached[0]):
        return cached[1]
    if not has_parquet():
        return False
    con = duckdb.connect()
    try:
        row = con.execute(
            f"""
            SELECT 1
            FROM read_parquet('{PARQUET_GLOB}')
            WHERE categoryId = ?
            LIMIT 1
            """,
            [category_id],
        ).fetchone()
        found = row is not None
    finally:
        con.close()
    _parquet_category_cache[category_id] = (time.monotonic(), found)
    return found


def prices_from(category_id: int | None = None) -> str:
    if has_parquet() and (category_id is None or parquet_has_category(category_id)):
        return f"read_parquet('{PARQUET_GLOB}')"
    return "pokemon_prices"


def db_has_table(name: str) -> bool:
    """Whether the DuckDB file has a table (the table list is remembered; see above)."""
    global _table_names_cache
    if _table_names_cache and _fresh(_table_names_cache[0]):
        return name in _table_names_cache[1]
    if not DB_PATH.exists():
        return False
    try:
        con = duckdb.connect(str(DB_PATH), read_only=True)
    except duckdb.IOException:
        # When another process holds the DuckDB write/read lock, metadata checks
        # should gracefully fall back to CSV/parquet sources instead of taking
        # the whole dashboard down.
        return False
    try:
        tables = frozenset(row[0] for row in con.execute("SHOW TABLES").fetchall())
    finally:
        con.close()
    _table_names_cache = (time.monotonic(), tables)
    return name in tables


def first_existing_path(*paths: Path) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def prefer_csv_source(csv_path: Path | None) -> bool:
    """Prefer CSV metadata exports whenever they exist.

    The dashboard has repeatedly hit stale placeholder names in DuckDB metadata
    tables even after the CSV export was corrected. The CSV export is the safer
    source of truth for product/group naming because it is rebuilt directly from
    the upstream metadata pull and is small enough to read on demand.
    """
    return csv_path is not None and csv_path.exists()


def products_from(category_id: int) -> str:
    category = category_config(category_id)
    csv_path = first_existing_path(
        EXTRACTED_DIR / category.products_csv,
        OUTPUT_DIR / category.products_csv,
    )
    if db_has_table(category.products_table) and not prefer_csv_source(csv_path):
        return category.products_table
    if csv_path is None:
        raise HTTPException(status_code=500, detail=f"{category.products_table} metadata not found")
    return f"read_csv_auto('{csv_path}')"


def groups_from(category_id: int) -> str:
    category = category_config(category_id)
    csv_path = first_existing_path(
        EXTRACTED_DIR / category.groups_csv,
        OUTPUT_DIR / category.groups_csv,
    )
    if db_has_table(category.groups_table) and not prefer_csv_source(csv_path):
        return category.groups_table
    if csv_path is None:
        raise HTTPException(status_code=500, detail=f"{category.groups_table} metadata not found")
    return f"read_csv_auto('{csv_path}')"


def product_signal_from(category_id: int) -> str:
    category = category_config(category_id)
    if db_has_table(category.product_signal_table):
        return category.product_signal_table
    csv_path = EXTRACTED_DIR / category.product_signal_csv
    if csv_path.exists():
        return f"read_csv_auto('{csv_path}')"
    raise HTTPException(status_code=500, detail=f"{category.product_signal_table} snapshot not found")


def group_signal_from(category_id: int) -> str:
    category = category_config(category_id)
    if db_has_table(category.group_signal_table):
        return category.group_signal_table
    csv_path = EXTRACTED_DIR / category.group_signal_csv
    if csv_path.exists():
        return f"read_csv_auto('{csv_path}')"
    raise HTTPException(status_code=500, detail=f"{category.group_signal_table} snapshot not found")


def sparkline_snapshot_from(category_id: int) -> str:
    category = category_config(category_id)
    if db_has_table(category.sparkline_snapshot_table):
        return category.sparkline_snapshot_table
    csv_path = EXTRACTED_DIR / category.sparkline_snapshot_csv
    if csv_path.exists():
        return f"read_csv_auto('{csv_path}')"
    raise HTTPException(status_code=500, detail=f"{category.sparkline_snapshot_table} snapshot not found")


def health_snapshot_from(category_id: int) -> str:
    category = category_config(category_id)
    if db_has_table(category.health_snapshot_table):
        return category.health_snapshot_table
    csv_path = EXTRACTED_DIR / category.health_snapshot_csv
    if csv_path.exists():
        return f"read_csv_auto('{csv_path}')"
    raise HTTPException(status_code=500, detail=f"{category.health_snapshot_table} snapshot not found")


def series_snapshot_from(category_id: int) -> str:
    category = category_config(category_id)
    if db_has_table(category.series_snapshot_table):
        return category.series_snapshot_table
    csv_path = EXTRACTED_DIR / category.series_snapshot_csv
    if csv_path.exists():
        return f"read_csv_auto('{csv_path}')"
    raise HTTPException(status_code=500, detail=f"{category.series_snapshot_table} snapshot not found")


def long_series_snapshot_from(category_id: int) -> str | None:
    """The weekly older-history table (see build_series_snapshot.py), or None until the
    nightly update has built it; callers then fall back to the raw price history."""
    category = category_config(category_id)
    if db_has_table(category.long_series_snapshot_table):
        return category.long_series_snapshot_table
    csv_path = EXTRACTED_DIR / category.long_series_snapshot_csv
    if csv_path.exists():
        return f"read_csv_auto('{csv_path}')"
    return None


def screener_snapshot_from(category_id: int) -> str:
    category = category_config(category_id)
    if db_has_table(category.screener_snapshot_table):
        return category.screener_snapshot_table
    csv_path = EXTRACTED_DIR / category.screener_snapshot_csv
    if csv_path.exists():
        return f"read_csv_auto('{csv_path}')"
    raise HTTPException(status_code=500, detail=f"{category.screener_snapshot_table} snapshot not found")


def get_con():
    """A connection for one query; close it when done.

    Returns a cursor on one shared read-only connection to the DuckDB file, opened on first
    use (cursors are DuckDB's way to use one connection from several threads, and closing
    a cursor leaves the shared connection open). While the site runs it holds a read lock,
    so anything that writes to the DuckDB file must run with the website stopped, as the
    nightly update does. If the file is missing or locked by a writer, falls back to an
    in-memory connection so queries on CSV/parquet sources still work.
    """
    global _shared_con
    if not DB_PATH.exists():
        return duckdb.connect()
    with _shared_con_lock:
        if _shared_con is None:
            try:
                _shared_con = duckdb.connect(str(DB_PATH), read_only=True)
            except duckdb.IOException:
                return duckdb.connect()
        return _shared_con.cursor()


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


# --- Feed-gap scaling for screener activity thresholds -------------------------------
#
# The Discover screeners require trading activity such as "10 different prices in the last
# 30 days" or "4 readings in the last 7 days". Those numbers assume a price every day. After
# a gap in the feed (TCGCSV had none from 2026-09-16 to 2026-10-07) no card can reach them
# and every list goes empty, so each threshold is scaled to the days of data that actually
# exist in its window. With a complete feed the thresholds are unchanged.

FEED_DATES_TTL_SECONDS = 600  # re-read after the nightly update within ten minutes
FEED_DATES_LOOKBACK_DAYS = 400
_feed_dates_cache: dict[int, tuple[float, list]] = {}


def recent_feed_dates(category_id: int) -> list:
    """Dates (newest first) in the last ~400 days that have any prices for a category.

    Cached for FEED_DATES_TTL_SECONDS because the query scans the price history (~1 s).
    Returns an empty list if the price source can't be read.
    """
    cached = _feed_dates_cache.get(category_id)
    if cached and time.monotonic() - cached[0] < FEED_DATES_TTL_SECONDS:
        return cached[1]
    source = prices_from(category_id)
    try:
        _, rows = q(
            f"""
            WITH d AS (SELECT MAX(date) AS max_date FROM {source} WHERE categoryId = {int(category_id)})
            SELECT DISTINCT date
            FROM {source}
            WHERE categoryId = {int(category_id)}
              AND date >= (SELECT max_date FROM d) - INTERVAL {FEED_DATES_LOOKBACK_DAYS} DAY
            ORDER BY date DESC
            """
        )
        dates = [row[0] for row in rows]
    except Exception:  # noqa: BLE001 - scaling must never break a screener
        dates = []
    _feed_dates_cache[category_id] = (time.monotonic(), dates)
    return dates


def gap_scaled_threshold(threshold: int, window_days: int, category_id: int, floor: int = 2) -> int:
    """Scale an activity threshold to the days of data in its look-back window.

    `window_days` matches the SQL `date >= latest - INTERVAL window_days DAY`, which spans
    window_days + 1 dates. The result keeps the same rate (threshold per full window),
    rounded up, and is never above `threshold` or below `floor` (or the threshold itself,
    if that is smaller). Example: 10 distinct prices in 30 days, with prices on only 9 of
    the last 31 dates, becomes ceil(10 * 9 / 31) = 3.
    """
    dates = recent_feed_dates(category_id)
    if not dates or window_days <= 0:
        return threshold
    start = dates[0] - timedelta(days=window_days)
    days_with_data = sum(1 for d in dates if d >= start)
    expected = window_days + 1
    if days_with_data >= expected:
        return threshold
    scaled = math.ceil(threshold * days_with_data / expected)
    return max(min(floor, threshold), min(threshold, scaled))


# ---- Set release dates ---------------------------------------------------------------
#
# TCGCSV's set "publishedOn" date is the release date for normal sets (new sets show
# pre-sale prices months before it, which is fine), but TCGplayer sometimes re-publishes
# old sets and their date jumps to that day (on 2026-10-12, POP Series 1-9 and many promo
# sets got that date). So, per set:
#   1. a hand-checked date from data/set_release_dates.csv, if listed there;
#   2. otherwise publishedOn, unless it is more than REPUBLISHED_AFTER_DAYS after the set's
#      first price (then it was re-published and the date is unknown: None).
SET_RELEASE_DATES_CSV = DATA_ROOT / "set_release_dates.csv"
REPUBLISHED_AFTER_DAYS = 300
_release_dates_cache: dict[int, tuple[float, dict]] = {}


def set_release_dates(category_id: int) -> dict[int, dict]:
    """{groupId: {"release_date": "YYYY-MM-DD" or None, "release_precision": ...}} for a market.

    release_precision is "day", "month", "approximate" (from the CSV) or "published"
    (TCGCSV's date), or "unknown". Remembered for SOURCE_CHECK_TTL_SECONDS.
    """
    cached = _release_dates_cache.get(category_id)
    if cached and _fresh(cached[0]):
        return cached[1]
    _, rows = q(
        f"""
        WITH first_price AS (
            SELECT groupId, MIN(date) AS first_price
            FROM {prices_from(category_id)}
            WHERE categoryId = {int(category_id)}
            GROUP BY groupId
        )
        SELECT g.groupId, CAST(CAST(g.publishedOn AS TIMESTAMP) AS DATE), fp.first_price
        FROM {groups_from(category_id)} g
        LEFT JOIN first_price fp ON fp.groupId = g.groupId
        """
    )
    dates: dict[int, dict] = {}
    for group_id, published, first_price in rows:
        republished = published is not None and first_price is not None and (
            (published - (first_price.date() if hasattr(first_price, "date") else first_price)).days > REPUBLISHED_AFTER_DAYS
        )
        if published is None or republished:
            dates[int(group_id)] = {"release_date": None, "release_precision": "unknown"}
        else:
            dates[int(group_id)] = {"release_date": published.isoformat(), "release_precision": "published"}
    # The hand-kept file is edited by people: a row that can't be read is skipped (and
    # printed to the server log) rather than breaking the Sets pages.
    if SET_RELEASE_DATES_CSV.exists():
        with SET_RELEASE_DATES_CSV.open(newline="", encoding="utf-8") as f:
            for line_number, row in enumerate(csv.DictReader(f), start=2):
                try:
                    if int(row["category_id"]) == int(category_id):
                        released = date.fromisoformat(row["release_date"].strip()).isoformat()
                        dates[int(row["group_id"])] = {"release_date": released, "release_precision": (row.get("precision") or "day").strip()}
                except (KeyError, TypeError, ValueError, AttributeError):
                    print(f"set_release_dates.csv line {line_number} skipped: {row}")
    _release_dates_cache[category_id] = (time.monotonic(), dates)
    return dates


def add_release_dates(cols: list, rows: list, category_id: int, group_column: str = "groupId") -> tuple[list, list]:
    """Append release_date and release_precision columns to a result keyed by groupId."""
    dates = set_release_dates(category_id)
    index = cols.index(group_column)
    out = []
    for row in rows:
        info = dates.get(int(row[index]), {"release_date": None, "release_precision": "unknown"})
        out.append(tuple(row) + (info["release_date"], info["release_precision"]))
    return cols + ["release_date", "release_precision"], out


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


def build_metadata_cte(category_id: int, include_classification: bool = False, cte_name: str = "metadata") -> str:
    """Return a reusable metadata CTE for product/group name joins in dashboard queries."""
    product_fields = [
        "p.productId",
        "p.groupId",
        "COALESCE(p.name, p.cleanName, 'Product ' || CAST(p.productId AS VARCHAR)) AS productName",
        "p.imageUrl",
        "p.rarity",
        "p.number",
    ]
    if include_classification:
        product_fields.extend(
            [
                f"{PRODUCT_CLASS_SQL} AS productClass",
                f"{PRODUCT_KIND_SQL} AS productKind",
            ]
        )
    product_fields.append("COALESCE(g.name, 'Unknown Group') AS groupName")
    product_fields.append("COALESCE(g.abbreviation, '') AS groupAbbreviation")
    fields_sql = ",\n            ".join(product_fields)
    return f"""
    {cte_name} AS (
        SELECT
            {fields_sql}
        FROM {products_from(category_id)} p
        LEFT JOIN {groups_from(category_id)} g
          ON g.groupId = p.groupId
    )
    """.strip()


def build_premium_rarity_filter(column: str = "rarity") -> str:
    """Return a SQL predicate matching the premium card rarities the dashboard treats as higher-end buys."""
    lower_col = f"lower(COALESCE({column}, ''))"
    return f"""(
        {lower_col} LIKE '%double rare%'
        OR {lower_col} LIKE '%illustration rare%'
        OR {lower_col} LIKE '%special illustration rare%'
        OR {lower_col} LIKE '%ultra rare%'
        OR {lower_col} LIKE '%hyper rare%'
        OR {lower_col} LIKE '%secret rare%'
        OR {lower_col} LIKE '%amazing rare%'
    )"""


def build_set_basket_filter(
    filters: list[str],
    rarity_column: str = "rarity",
    subtype_column: str = "subTypeName",
    product_name_column: str = "productName",
) -> str:
    """Return a SQL predicate for Set Explorer rarity/variant filtering.

    The explorer now answers multiple set questions through one basket view:
    full tracked set, only hits, only reverse holos, only IR/SIR, etc. The UI
    sends normalized filter keys which are translated here into the matching
    rarity/subtype rules.
    """
    normalized = {str(value).strip().lower() for value in (filters or []) if str(value).strip()}
    if not normalized or "all" in normalized:
        return "1=1"

    rarity = f"lower(COALESCE({rarity_column}, ''))"
    subtype = f"lower(COALESCE({subtype_column}, ''))"
    product_name = f"lower(COALESCE({product_name_column}, ''))"
    clauses = []
    if "common" in normalized:
        clauses.append(f"{rarity} = 'common'")
    if "uncommon" in normalized:
        clauses.append(f"{rarity} = 'uncommon'")
    if "rare" in normalized:
        clauses.append(f"{rarity} = 'rare'")
    if "reverse_holo" in normalized:
        clauses.append(f"{subtype} LIKE '%reverse holo%'")
    if "pokeball_holo" in normalized:
        clauses.append(f"({product_name} LIKE '%poke ball%' OR {subtype} LIKE '%poke ball%')")
    if "masterball_holo" in normalized:
        clauses.append(f"({product_name} LIKE '%master ball%' OR {subtype} LIKE '%master ball%')")
    if "holo_rare" in normalized:
        clauses.append(f"{rarity} = 'holo rare'")
    if "double_rare" in normalized:
        clauses.append(f"{rarity} LIKE '%double rare%'")
    if "illustration_rare" in normalized:
        clauses.append(f"{rarity} LIKE '%illustration rare%' AND {rarity} NOT LIKE '%special illustration rare%'")
    if "special_illustration_rare" in normalized:
        clauses.append(f"{rarity} LIKE '%special illustration rare%'")
    if "ultra_rare" in normalized:
        clauses.append(f"{rarity} LIKE '%ultra rare%'")
    if "hyper_rare" in normalized:
        clauses.append(f"{rarity} LIKE '%hyper rare%'")
    if "secret_rare" in normalized:
        clauses.append(f"{rarity} LIKE '%secret rare%'")
    if "promo" in normalized:
        clauses.append(f"{rarity} LIKE '%promo%'")
    if "stamped" in normalized:
        clauses.append(f"({product_name} LIKE '%stamp%' OR {subtype} LIKE '%stamp%')")

    return "(\n        " + "\n        OR ".join(clauses or ["1=1"]) + "\n    )"


def build_generation_case(
    group_id_column: str = "g.groupId",
    name_column: str = "g.name",
    abbreviation_column: str = "g.abbreviation",
    published_on_column: str = "g.publishedOn",
) -> str:
    """Return a broad generation/era label for set-level grouping in the dashboard.

    The matching intentionally prefers exact program buckets and explicit era
    prefixes in the set name (for example `SV:` or `SWSH:`) over loose
    abbreviations or raw dates. Some real set abbreviations like `MEW` would
    otherwise collide with the custom Mega bucket if we matched every `ME*`
    abbreviation.
    """
    group_id = f"CAST({group_id_column} AS BIGINT)"
    name = f"upper(COALESCE({name_column}, ''))"
    abbr = f"upper(COALESCE({abbreviation_column}, ''))"
    published_on = f"CAST({published_on_column} AS DATE)"
    # TCGCSV sometimes fills an unknown release date with the metadata refresh
    # timestamp. Real release dates in the current feeds are normalized to
    # midnight; only those values are safe as an era-classification fallback.
    has_release_date = (
        f"strftime(CAST({published_on_column} AS TIMESTAMP), '%H:%M:%S') = '00:00:00'"
    )
    override_clauses = "\n".join(
        f"  WHEN {group_id} = {group_id_value} THEN '{generation}'"
        for group_id_value, generation in sorted(GENERATION_OVERRIDES.items())
    )
    return f"""
CASE
{override_clauses}
  WHEN {name} LIKE 'MCDONALD%'
    OR {abbr} LIKE 'M%'
    AND {name} LIKE '%PROMO%'
    THEN 'MCD'
  WHEN {name} LIKE 'TRICK OR TRADE%'
    OR {abbr} LIKE 'TT%'
    THEN 'TOTT'
  WHEN {name} LIKE '%PRIZE PACK%'
    OR {name} LIKE 'PRIZE PACK SERIES%'
    OR {abbr} LIKE 'PPS%'
    THEN 'PRIZE'
  WHEN {name} LIKE '%PROMO%'
    OR {name} LIKE '%BLACK STAR%'
    OR {abbr} IN ('SVP', 'SWSD', 'SMP', 'BWP', 'HSP', 'DPP', 'NP', 'WP', 'MEP')
    THEN 'PROMO'
  WHEN {name} LIKE '%MEGA EVOLUTION%'
    OR {name} LIKE 'ME:%'
    OR {name} LIKE 'ME0%'
    OR {name} LIKE 'M1:%'
    OR {name} LIKE 'M1A:%'
    OR {name} LIKE 'M1L:%'
    OR {name} LIKE 'M1S:%'
    OR {name} LIKE 'M2:%'
    OR {name} LIKE 'M2A:%'
    OR {name} LIKE 'M3:%'
    OR {name} LIKE 'M3A:%'
    OR {name} LIKE 'M4:%'
    OR {name} LIKE 'M4A:%'
    OR {name} LIKE 'M5:%'
    OR {name} LIKE 'M5A:%'
    OR {name} LIKE 'M6:%'
    OR {name} LIKE 'M6A:%'
    OR {name} LIKE 'MBD:%'
    OR {name} LIKE 'MBG:%'
    OR {name} LIKE 'MEM:%'
    OR {name} LIKE 'MEZ:%'
    OR {name} LIKE 'MF:%'
    OR {name} LIKE 'MP1:%'
    OR {name} LIKE '%START DECK 100 BATTLE COLLECTION%'
    OR {name} LIKE 'MEE:%'
    OR {abbr} = 'ME'
    OR {abbr} LIKE 'ME0%'
    OR {abbr} LIKE 'MEE%'
    OR {abbr} IN ('M1', 'M1A', 'M1L', 'M1S', 'M2', 'M2A', 'M3', 'M3A', 'M4', 'M4A', 'M5', 'M5A', 'M6', 'M6A', 'MBD', 'MBG', 'MEM', 'MEZ', 'MF', 'MP1')
    THEN 'MEG'
  WHEN {name} LIKE 'SV:%'
    OR {name} LIKE 'SV %'
    OR {name} LIKE 'SCARLET & VIOLET%'
    OR ({has_release_date} AND {published_on} >= DATE '2023-03-31')
    THEN 'SV'
  WHEN {name} LIKE 'SWSH:%'
    OR {name} LIKE 'SWSH %'
    OR {name} LIKE 'SWORD & SHIELD%'
    OR {abbr} LIKE 'SWSH%'
    OR ({has_release_date} AND {published_on} >= DATE '2020-02-07')
    THEN 'SWSH'
  WHEN {name} LIKE 'SM:%'
    OR {name} LIKE 'SM %'
    OR {name} LIKE 'SUN & MOON%'
    OR {abbr} LIKE 'SM%'
    OR ({has_release_date} AND {published_on} >= DATE '2017-02-03')
    THEN 'SM'
  WHEN {name} LIKE 'XY:%'
    OR {name} LIKE 'XY %'
    OR {abbr} LIKE 'XY%'
    OR ({has_release_date} AND {published_on} >= DATE '2014-02-05')
    THEN 'XY'
  WHEN {name} LIKE 'BW:%'
    OR {name} LIKE 'BW %'
    OR {name} LIKE 'BLACK & WHITE%'
    OR {abbr} LIKE 'BW%'
    OR ({has_release_date} AND {published_on} >= DATE '2011-04-25')
    THEN 'BW'
  WHEN {name} LIKE 'POP SERIES%'
    OR {abbr} = 'POP'
    THEN 'POP'
  WHEN {name} LIKE 'EX %'
    OR {name} IN (
      'RUBY & SAPPHIRE',
      'SANDSTORM',
      'DRAGON',
      'TEAM MAGMA VS TEAM AQUA',
      'HIDDEN LEGENDS',
      'FIRERED & LEAFGREEN',
      'TEAM ROCKET RETURNS',
      'DEOXYS',
      'EMERALD',
      'UNSEEN FORCES',
      'DELTA SPECIES',
      'LEGEND MAKER',
      'HOLON PHANTOMS',
      'CRYSTAL GUARDIANS',
      'DRAGON FRONTIERS',
      'POWER KEEPERS'
    )
    OR ({has_release_date} AND {published_on} >= DATE '2003-07-18' AND {published_on} < DATE '2007-05-23')
    THEN 'EX'
  WHEN {name} LIKE 'DP%'
    OR {name} LIKE 'HGSS%'
    OR {abbr} LIKE 'DP%'
    OR {abbr} LIKE 'HGSS%'
    OR ({has_release_date} AND {published_on} >= DATE '2007-01-01')
    THEN 'DP/HGSS'
  ELSE 'Legacy'
END
""".strip()
