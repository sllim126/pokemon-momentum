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
# This table is expected to grow as real-world metadata edge cases are found.
GENERATION_OVERRIDES = {
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
    24541: "MEG",    # ME: Ascended Heroes
    24461: "MEG",    # MEE: Mega Evolution Energies
    24587: "MEG",    # ME03: Perfect Order
    24655: "MEG",    # ME04: Chaos Rising
}


def category_config(category_id: int):
    return get_category_config(category_id)


def has_parquet() -> bool:
    return PARQUET_ROOT.exists() and any(PARQUET_ROOT.rglob("*.parquet"))


def parquet_has_category(category_id: int) -> bool:
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
        return row is not None
    finally:
        con.close()


def prices_from(category_id: int | None = None) -> str:
    if has_parquet() and (category_id is None or parquet_has_category(category_id)):
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


def get_con():
    if DB_PATH.exists():
        return duckdb.connect(str(DB_PATH), read_only=True)
    return duckdb.connect()


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
  WHEN {name} LIKE 'SV:%'
    OR {name} LIKE 'SV %'
    OR {name} LIKE 'SCARLET & VIOLET%'
    OR {published_on} >= DATE '2023-03-31'
    THEN 'SV'
  WHEN {name} LIKE 'SWSH:%'
    OR {name} LIKE 'SWSH %'
    OR {name} LIKE 'SWORD & SHIELD%'
    OR {abbr} LIKE 'SWSH%'
    OR {published_on} >= DATE '2020-02-07'
    THEN 'SWSH'
  WHEN {name} LIKE 'SM:%'
    OR {name} LIKE 'SM %'
    OR {name} LIKE 'SUN & MOON%'
    OR {abbr} LIKE 'SM%'
    OR {published_on} >= DATE '2017-02-03'
    THEN 'SM'
  WHEN {name} LIKE 'XY:%'
    OR {name} LIKE 'XY %'
    OR {abbr} LIKE 'XY%'
    OR {published_on} >= DATE '2014-02-05'
    THEN 'XY'
  WHEN {name} LIKE 'BW:%'
    OR {name} LIKE 'BW %'
    OR {name} LIKE 'BLACK & WHITE%'
    OR {abbr} LIKE 'BW%'
    OR {published_on} >= DATE '2011-04-25'
    THEN 'BW'
  WHEN {name} LIKE 'POP SERIES%'
    OR {abbr} = 'POP'
    THEN 'POP'
  WHEN {name} LIKE '%MEGA EVOLUTION%'
    OR {name} LIKE 'ME:%'
    OR {name} LIKE 'ME0%'
    OR {name} LIKE 'MEE:%'
    OR {abbr} = 'ME'
    OR {abbr} LIKE 'ME0%'
    OR {abbr} LIKE 'MEE%'
    THEN 'MEG'
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
    OR {published_on} >= DATE '2003-07-18' AND {published_on} < DATE '2007-05-23'
    THEN 'EX'
  WHEN {name} LIKE 'DP%'
    OR {name} LIKE 'HGSS%'
    OR {abbr} LIKE 'DP%'
    OR {abbr} LIKE 'HGSS%'
    OR {published_on} >= DATE '2007-01-01'
    THEN 'DP/HGSS'
  ELSE 'Legacy'
END
""".strip()
