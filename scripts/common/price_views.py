"""Price rows with each product under its current set (groupId), for the snapshot builders.

TCGplayer sometimes moves a product to a different set listing (a new groupId) and keeps its
productId: Basic Darkness Energy (Cracked Ice Holo), 599747, moved from 23537 to 24382 on
2025-08-08. pokemon_prices keeps the old rows under the old groupId. The snapshot builders
group by (groupId, productId, subTypeName), so a moved card got two histories: two chart rows
(the /series endpoint picked one at random, sometimes the one that ended in 2025) and signals
worked out on only the newest part of its history. On 2026-11-05: 116 English and 7 Japanese
products had moved, and none had prices under two sets on the same day.

prices_by_current_group() is a drop-in for pokemon_prices that relabels every row of a moved
product with the set it was in on its latest price day, so each card has one history.
"""

from __future__ import annotations

import duckdb

VIEW = "prices_by_current_group"


def prices_by_current_group(con: duckdb.DuckDBPyConnection, category_id: int) -> str:
    """Create (or replace) a temp view of pokemon_prices for one category with moved products
    relabelled to their current groupId, and return its name to use in FROM clauses."""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW {VIEW} AS
        WITH moved AS (
            SELECT productId, arg_max(groupId, date) AS current_group_id
            FROM pokemon_prices
            WHERE categoryId = {int(category_id)}
            GROUP BY productId
            HAVING COUNT(DISTINCT groupId) > 1
        )
        SELECT * EXCLUDE (current_group_id) REPLACE (COALESCE(m.current_group_id, p.groupId) AS groupId)
        FROM pokemon_prices p
        LEFT JOIN moved m USING (productId)
        WHERE p.categoryId = {int(category_id)}
        """
    )
    return VIEW
