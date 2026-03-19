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


def products_from(category_id: int) -> str:
    category = category_config(category_id)
    if db_has_table(category.products_table):
        return category.products_table
    csv_path = first_existing_path(
        EXTRACTED_DIR / category.products_csv,
        OUTPUT_DIR / category.products_csv,
    )
    if csv_path is None:
        raise HTTPException(status_code=500, detail=f"{category.products_table} metadata not found")
    return f"read_csv_auto('{csv_path}')"


def groups_from(category_id: int) -> str:
    category = category_config(category_id)
    if db_has_table(category.groups_table):
        return category.groups_table
    csv_path = first_existing_path(
        EXTRACTED_DIR / category.groups_csv,
        OUTPUT_DIR / category.groups_csv,
    )
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


def build_generation_case(
    name_column: str = "g.name",
    abbreviation_column: str = "g.abbreviation",
    published_on_column: str = "g.publishedOn",
) -> str:
    """Return a broad generation/era label for set-level grouping in the dashboard."""
    name = f"upper(COALESCE({name_column}, ''))"
    abbr = f"upper(COALESCE({abbreviation_column}, ''))"
    published_on = f"CAST({published_on_column} AS DATE)"
    return f"""
CASE
  WHEN {name} LIKE '%MEGA EVOLUTION%'
    OR {name} LIKE 'ME:%'
    OR {name} LIKE 'ME0%'
    OR {name} LIKE 'MEE:%'
    OR {abbr} LIKE 'ME%'
    THEN 'MEG'
  WHEN {published_on} >= DATE '2023-01-01'
    OR {name} LIKE 'SV%'
    OR {abbr} LIKE 'SV%'
    THEN 'SV'
  WHEN {published_on} >= DATE '2020-01-01'
    OR {name} LIKE 'SWSH%'
    OR {abbr} LIKE 'SWSH%'
    THEN 'SWSH'
  WHEN {published_on} >= DATE '2017-01-01'
    OR {name} LIKE 'SM%'
    OR {abbr} LIKE 'SM%'
    THEN 'SM'
  WHEN {published_on} >= DATE '2014-01-01'
    OR {name} LIKE 'XY%'
    OR {abbr} LIKE 'XY%'
    THEN 'XY'
  WHEN {published_on} >= DATE '2011-01-01'
    OR {name} LIKE 'BW%'
    OR {abbr} LIKE 'BW%'
    THEN 'BW'
  WHEN {published_on} >= DATE '2007-01-01'
    OR {name} LIKE 'DP%'
    OR {name} LIKE 'HGSS%'
    OR {abbr} LIKE 'DP%'
    OR {abbr} LIKE 'HGSS%'
    THEN 'DP/HGSS'
  ELSE 'Legacy'
END
""".strip()
