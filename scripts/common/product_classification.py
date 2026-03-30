"""Shared product classification rules used by snapshots and API queries."""


def _name_expr(alias: str) -> str:
    return f"lower(COALESCE({alias}.name, ''))"


def get_product_class_sql(alias: str = "p") -> str:
    """Return the SQL CASE expression for the detailed productClass field."""
    name = _name_expr(alias)
    return f"""
CASE
  WHEN {name} LIKE '%ultra-premium collection%'
    OR {name} LIKE '%ultra premium collection%'
    THEN 'mcap'
  WHEN {name} LIKE '%booster box%'
    OR {name} LIKE '%booster box case%'
    OR ({name} LIKE '%booster%' AND {name} LIKE '%case%')
    OR {name} LIKE '%elite trainer box%'
    OR {name} LIKE '% etb%'
    OR {name} LIKE 'etb%'
    OR {name} LIKE '%build & battle box%'
    OR {name} LIKE '%build and battle box%'
    THEN 'sealed_booster_box'
  WHEN {name} LIKE '%v battle deck%'
    OR {name} LIKE '%battle deck%'
    OR {name} LIKE '%league battle deck%'
    OR {name} LIKE '%deck bundle%'
    OR {name} LIKE '%battle academy%'
    THEN 'sealed_deck'
  WHEN {name} LIKE '%booster pack%'
    OR {name} LIKE '%booster bundle%'
    OR {name} LIKE '%binder collection case%'
    OR {name} LIKE '%binder collection%'
    OR {name} LIKE '%mini tin%'
    OR {name} LIKE '% tin%'
    OR {name} LIKE 'tin %'
    OR {name} LIKE '%blister case%'
    OR {name} LIKE '%blister%'
    OR {name} LIKE '%premium figure collection%'
    OR {name} LIKE '%figure collection%'
    OR {name} LIKE '%premium figure set%'
    OR {name} LIKE '%sleeved%'
    THEN 'sealed_booster_pack'
  WHEN COALESCE(NULLIF({alias}.number, ''), '') <> ''
    OR COALESCE(NULLIF({alias}.rarity, ''), '') <> ''
    THEN 'card'
  ELSE 'other'
END
""".strip()


def get_product_kind_sql(alias: str = "p") -> str:
    """Return the SQL CASE expression for the broader productKind field."""
    name = _name_expr(alias)
    return f"""
CASE
  WHEN {name} LIKE '%ultra-premium collection%'
    OR {name} LIKE '%ultra premium collection%'
    THEN 'mcap'
  WHEN {name} LIKE '%booster box%'
    OR {name} LIKE '%booster box case%'
    OR ({name} LIKE '%booster%' AND {name} LIKE '%case%')
    OR {name} LIKE '%elite trainer box%'
    OR {name} LIKE '% etb%'
    OR {name} LIKE 'etb%'
    OR {name} LIKE '%build & battle box%'
    OR {name} LIKE '%build and battle box%'
    OR {name} LIKE '%v battle deck%'
    OR {name} LIKE '%battle deck%'
    OR {name} LIKE '%league battle deck%'
    OR {name} LIKE '%deck bundle%'
    OR {name} LIKE '%battle academy%'
    OR {name} LIKE '%booster pack%'
    OR {name} LIKE '%booster bundle%'
    OR {name} LIKE '%binder collection case%'
    OR {name} LIKE '%binder collection%'
    OR {name} LIKE '%mini tin%'
    OR {name} LIKE '% tin%'
    OR {name} LIKE 'tin %'
    OR {name} LIKE '%blister case%'
    OR {name} LIKE '%blister%'
    OR {name} LIKE '%premium figure collection%'
    OR {name} LIKE '%figure collection%'
    OR {name} LIKE '%premium figure set%'
    OR {name} LIKE '%sleeved%'
    THEN 'sealed'
  WHEN COALESCE(NULLIF({alias}.number, ''), '') <> ''
    OR COALESCE(NULLIF({alias}.rarity, ''), '') <> ''
    THEN 'card'
  ELSE 'other'
END
""".strip()
