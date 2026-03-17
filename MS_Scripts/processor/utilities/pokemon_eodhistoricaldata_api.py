"""
Pokemon-themed drop-in replacement for eodhistoricaldata_api.

This implementation preserves the public EodApi method surface but reads from the
local Pokemon Momentum DuckDB database instead of a remote securities API.
"""

from __future__ import annotations

import logging
import time
import urllib.error
from datetime import date, datetime
from pathlib import Path
from threading import Lock
from typing import Literal

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = Path("/app/data/processed/prices_db.duckdb")
PARQUET_ROOT = Path("/app/data/parquet")
PARQUET_GLOB = str(PARQUET_ROOT / "**/*.parquet")


class PaymentRequiredError(RuntimeError):
    """Retained for interface compatibility with the stock API wrapper."""


class EodApi:
    """Pokemon local-data implementation that mirrors the stock EodApi surface."""

    def __init__(self, market: str, token: str | None = None, debug: bool = False) -> None:
        self.token = token
        self.api_url = "local://pokemon-momentum/"
        self.market_code = market
        self.debug = debug
        self._rate_limit_limit: int | None = None
        self._rate_limit_remaining: int | None = None
        self._rate_limit_lock = Lock()

    def get_rate_limit_snapshot(self) -> tuple[int | None, int | None]:
        with self._rate_limit_lock:
            return self._rate_limit_limit, self._rate_limit_remaining

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if not DB_PATH.exists():
            raise FileNotFoundError(f"DuckDB database not found: {DB_PATH}")
        return duckdb.connect(str(DB_PATH), read_only=True)

    def _db_has_table(self, name: str) -> bool:
        con = self._connect()
        try:
            tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
            return name in tables
        finally:
            con.close()

    def _prices_from(self) -> str:
        if PARQUET_ROOT.exists() and any(PARQUET_ROOT.rglob("*.parquet")):
            return f"read_parquet('{PARQUET_GLOB}')"
        return "pokemon_prices"

    @staticmethod
    def _split_code(code: str) -> tuple[str, str | None]:
        parts = code.split("||", 1)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
        return code.strip(), None

    def resolve_product(self, code: str) -> pd.Series | None:
        return self._resolve_product(code)

    def _resolve_product(self, code: str) -> pd.Series | None:
        raw_code, subtype = self._split_code(code)
        con = self._connect()
        try:
            prices_from = self._prices_from()
            where_parts = []
            params: list[object] = []

            if raw_code.isdigit():
                where_parts.append("CAST(base.productId AS VARCHAR) = ?")
                params.append(raw_code)
            else:
                where_parts.append(
                    "(lower(COALESCE(pp.name, '')) = lower(?) OR lower(COALESCE(pp.cleanName, '')) = lower(?))"
                )
                params.extend([raw_code, raw_code])

            if subtype:
                where_parts.append("base.subTypeName = ?")
                params.append(subtype)

            signal_join = ""
            signal_cols = """
                NULL AS roc_7d_pct,
                NULL AS roc_30d_pct,
                NULL AS roc_90d_pct,
                NULL AS trend_score,
                NULL AS price_vs_sma30_pct,
                NULL AS price_vs_sma90_pct,
                NULL AS breakout_90d_flag,
                NULL AS acceleration_7d_vs_30d
            """
            if self._db_has_table("product_signal_snapshot"):
                signal_join = """
                LEFT JOIN product_signal_snapshot ps
                  ON ps.groupId = base.groupId
                 AND ps.productId = base.productId
                 AND ps.subTypeName = base.subTypeName
                """
                signal_cols = """
                    ps.roc_7d_pct,
                    ps.roc_30d_pct,
                    ps.roc_90d_pct,
                    ps.trend_score,
                    ps.price_vs_sma30_pct,
                    ps.price_vs_sma90_pct,
                    ps.breakout_90d_flag,
                    ps.acceleration_7d_vs_30d
                """

            sql = f"""
            WITH latest_rows AS (
                SELECT
                    p.groupId,
                    p.productId,
                    p.subTypeName,
                    MAX(p.date) AS latest_date
                FROM {prices_from} p
                WHERE p.categoryId = 3
                  AND p.marketPrice IS NOT NULL
                GROUP BY p.groupId, p.productId, p.subTypeName
            ),
            base AS (
                SELECT
                    p.groupId,
                    p.productId,
                    p.subTypeName,
                    p.date AS latest_date,
                    p.marketPrice AS latest_price,
                    (
                        SELECT p7.marketPrice
                        FROM {prices_from} p7
                        WHERE p7.categoryId = 3
                          AND p7.groupId = p.groupId
                          AND p7.productId = p.productId
                          AND p7.subTypeName = p.subTypeName
                          AND p7.marketPrice IS NOT NULL
                          AND p7.date <= p.date - INTERVAL 7 DAY
                        ORDER BY p7.date DESC
                        LIMIT 1
                    ) AS price_7d
                FROM {prices_from} p
                JOIN latest_rows l
                  ON l.groupId = p.groupId
                 AND l.productId = p.productId
                 AND l.subTypeName = p.subTypeName
                 AND l.latest_date = p.date
                WHERE p.categoryId = 3
                  AND p.marketPrice IS NOT NULL
            )
            SELECT
                base.*,
                COALESCE(pp.name, pp.cleanName, 'Product ' || CAST(base.productId AS VARCHAR)) AS productName,
                pp.cleanName,
                pp.imageUrl,
                pp.rarity,
                pp.number,
                g.name AS groupName,
                {signal_cols},
                ROW_NUMBER() OVER (
                    PARTITION BY base.productId, base.subTypeName
                    ORDER BY base.latest_price DESC NULLS LAST, trend_score DESC NULLS LAST
                ) AS rn
            FROM base
            LEFT JOIN pokemon_products pp
              ON pp.groupId = base.groupId
             AND pp.productId = base.productId
            LEFT JOIN pokemon_groups g
              ON g.groupId = base.groupId
            {signal_join}
            WHERE {' AND '.join(where_parts)}
            QUALIFY rn = 1
            ORDER BY base.latest_price DESC NULLS LAST, trend_score DESC NULLS LAST
            LIMIT 1
            """
            df = con.execute(sql, params).fetchdf()
            if df.empty:
                return None
            return df.iloc[0]
        finally:
            con.close()

    def _code_label(self, row: pd.Series) -> str:
        return f"{int(row['productId'])}||{row['subTypeName']}"

    def get_market_details(self, from_date: datetime | None = None, to_date: datetime | None = None):
        con = self._connect()
        try:
            summary = con.execute(
                f"""
                SELECT
                    COUNT(*) AS rows,
                    COUNT(DISTINCT groupId) AS groups,
                    COUNT(DISTINCT productId) AS products,
                    MIN(date) AS min_date,
                    MAX(date) AS max_date
                FROM {self._prices_from()}
                WHERE categoryId = 3
                """
            ).fetchone()
            return pd.Series(
                {
                    "Code": self.market_code,
                    "Name": "Pokemon TCG",
                    "Rows": summary[0],
                    "Groups": summary[1],
                    "Products": summary[2],
                    "From": from_date or summary[3],
                    "To": to_date or summary[4],
                }
            )
        finally:
            con.close()

    def code_eod(
        self,
        code: str,
        to_date: date | None = None,
        from_date: date | None = None,
        product: pd.Series | None = None,
    ) -> pd.DataFrame | None:
        product = product if product is not None else self._resolve_product(code)
        if product is None:
            return None

        latest_date = product.get("latest_date")
        if pd.notna(latest_date) and hasattr(latest_date, "date"):
            latest_date = latest_date.date()
        to_date = to_date or latest_date or datetime.today().date()
        con = self._connect()
        try:
            from_clause = ""
            params = [
                int(product["groupId"]),
                int(product["productId"]),
                str(product["subTypeName"]),
                to_date,
            ]
            if from_date is not None:
                from_clause = "AND date >= ?"
                params.append(from_date)
            df = con.execute(
                f"""
                SELECT
                    date,
                    lowPrice AS open,
                    highPrice AS high,
                    lowPrice AS low,
                    marketPrice AS close,
                    marketPrice AS adjusted_close,
                    1 AS volume
                FROM {self._prices_from()}
                WHERE categoryId = 3
                  AND groupId = ?
                  AND productId = ?
                  AND subTypeName = ?
                  AND date <= ?
                  {from_clause}
                  AND marketPrice IS NOT NULL
                ORDER BY date
                """,
                params,
            ).fetchdf()
            return df if not df.empty else None
        finally:
            con.close()

    def code_div(self, code: str, to_date: date | None = None) -> pd.DataFrame | None:
        columns = [
            "date",
            "declarationDate",
            "recordDate",
            "paymentDate",
            "period",
            "value",
            "unadjustedValue",
            "currency",
        ]
        return pd.DataFrame(columns=columns)

    def code_splits(self, code: str, to_date: date | None = None) -> pd.DataFrame | None:
        columns = ["date", "ratio"]
        return pd.DataFrame(columns=columns)

    def bulk_eod(self, fetch_date: date | None = None, extended: bool = False) -> pd.DataFrame | None:
        fetch_date = fetch_date or datetime.today().date()
        con = self._connect()
        try:
            sql = f"""
            WITH latest_date AS (
                SELECT MAX(date) AS latest_date
                FROM {self._prices_from()}
                WHERE categoryId = 3
                  AND date <= ?
                  AND marketPrice IS NOT NULL
            )
            SELECT
                CAST(productId AS VARCHAR) || '||' || subTypeName AS code,
                'POKEMON' AS exchange_short_name,
                date,
                lowPrice AS open,
                highPrice AS high,
                lowPrice AS low,
                marketPrice AS close,
                marketPrice AS adjusted_close,
                1 AS volume
            FROM {self._prices_from()}
            WHERE categoryId = 3
              AND date = (SELECT latest_date FROM latest_date)
              AND marketPrice IS NOT NULL
            """
            df = con.execute(sql, [fetch_date]).fetchdf()
            if df.empty:
                return None
            if extended:
                meta = con.execute(
                    """
                    SELECT
                        CAST(productId AS VARCHAR) || '||' || subTypeName AS code,
                        groupId,
                        groupName,
                        productName,
                        rarity,
                        number,
                        trend_score
                    FROM product_signal_snapshot
                    """
                ).fetchdf()
                df = df.merge(meta, on="code", how="left")
            return df
        finally:
            con.close()

    def bulk_div(self, fetch_date: date | None = None) -> pd.DataFrame | None:
        columns = [
            "code",
            "exchange",
            "date",
            "dividend",
            "currency",
            "declarationDate",
            "recordDate",
            "paymentDate",
            "period",
            "unadjustedValue",
        ]
        return pd.DataFrame(columns=columns)

    def bulk_splits(self, fetch_date: date | None = None) -> pd.DataFrame | None:
        columns = ["code", "exchange", "date", "split"]
        return pd.DataFrame(columns=columns)

    def code_live(self, code: str, product: pd.Series | None = None) -> pd.Series | None:
        product = product if product is not None else self._resolve_product(code)
        if product is None:
            return None

        latest_price = float(product["latest_price"]) if pd.notna(product["latest_price"]) else 0.0
        prev = float(product["price_7d"]) if pd.notna(product["price_7d"]) else latest_price
        change = latest_price - prev
        change_p = 0.0 if prev == 0 else (change / prev) * 100

        return pd.Series(
            {
                "code": f"{self._code_label(product)}.{self.market_code}",
                "timestamp": int(time.time()),
                "gmtoffset": 0,
                "open": prev,
                "high": latest_price,
                "low": prev,
                "close": latest_price,
                "volume": 1,
                "previousClose": prev,
                "change": change,
                "change_p": change_p,
            }
        )

    def currency_live(self, currency: str) -> pd.Series | None:
        normalized = currency.upper()
        return pd.Series({"code": normalized, "timestamp": int(time.time()), "close": 1.0})

    def code_intraday(
        self,
        code: str,
        interval: str,
        from_date: date,
        to_date: date | None = None,
    ) -> pd.DataFrame | Literal[False] | None:
        if interval not in ["1m", "5m", "1h"]:
            logger.error("Invalid interval specified: %s. Expecting one of [1m, 5m, 1h]", interval)
            return False

        live = self.code_live(code)
        if live is None:
            return None

        to_date = to_date or datetime.today().date()
        from_ts = int(time.mktime(from_date.timetuple()))
        to_ts = int(time.mktime(to_date.timetuple()))
        step = 60 if interval == "1m" else 300 if interval == "5m" else 3600
        if to_ts <= from_ts:
            to_ts = from_ts + step

        rows = []
        for ts in range(from_ts, to_ts + 1, step):
            rows.append(
                {
                    "timestamp": ts,
                    "gmtoffset": 0,
                    "datetime": datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"),
                    "open": live["open"],
                    "high": live["high"],
                    "low": live["low"],
                    "close": live["close"],
                    "volume": 1,
                }
            )
        return pd.DataFrame(rows)

    def code_fundamentals(self, code: str, product: pd.Series | None = None) -> pd.Series | None:
        product = product if product is not None else self._resolve_product(code)
        if product is None:
            return None

        return pd.Series(
            {
                "General": {
                    "Code": self._code_label(product),
                    "Name": product.get("productName"),
                    "Set": product.get("groupName"),
                    "GroupId": int(product.get("groupId")),
                    "ProductId": int(product.get("productId")),
                    "Subtype": product.get("subTypeName"),
                },
                "Highlights": {
                    "Rarity": product.get("rarity"),
                    "Number": product.get("number"),
                    "LatestPrice": product.get("latest_price"),
                    "ROC7": product.get("roc_7d_pct"),
                    "ROC30": product.get("roc_30d_pct"),
                    "ROC90": product.get("roc_90d_pct"),
                    "TrendScore": product.get("trend_score"),
                },
                "Valuation": {
                    "price_vs_sma30_pct": product.get("price_vs_sma30_pct"),
                    "price_vs_sma90_pct": product.get("price_vs_sma90_pct"),
                    "breakout_90d_flag": product.get("breakout_90d_flag"),
                    "acceleration_7d_vs_30d": product.get("acceleration_7d_vs_30d"),
                },
                "SharesStats": {},
                "Technicals": {
                    "ImageUrl": product.get("imageUrl"),
                    "CleanName": product.get("cleanName"),
                },
                "SplitsDividends": {},
                "AnalystRatings": {},
                "Holders": {},
                "InsiderTransactions": {},
                "ESGScores": {},
                "outstandingShares": {},
                "Earnings": {},
                "Financials": {},
            }
        )

    def get_index_list(self) -> pd.Series:
        con = self._connect()
        try:
            rows = con.execute(
                """
                SELECT DISTINCT CAST(groupId AS VARCHAR)
                FROM pokemon_groups
                ORDER BY groupId
                """
            ).fetchall()
            return pd.Series([row[0] for row in rows])
        finally:
            con.close()

    def get_index_components(self, index: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        con = self._connect()
        try:
            prices_from = self._prices_from()
            if str(index).isdigit():
                group_df = con.execute(
                    """
                    SELECT groupId, name, abbreviation, publishedOn
                    FROM pokemon_groups
                    WHERE groupId = ?
                    LIMIT 1
                    """,
                    [int(index)],
                ).fetchdf()
            else:
                group_df = con.execute(
                    """
                    SELECT groupId, name, abbreviation, publishedOn
                    FROM pokemon_groups
                    WHERE lower(name) = lower(?)
                    LIMIT 1
                    """,
                    [index],
                ).fetchdf()

            if group_df.empty:
                raise urllib.error.HTTPError(url="", code=404, msg="Not Found", hdrs=None, fp=None)

            group_id = int(group_df.iloc[0]["groupId"])
            components = con.execute(
                f"""
                WITH latest_date AS (
                    SELECT MAX(date) AS latest_date
                    FROM {prices_from}
                    WHERE categoryId = 3
                      AND groupId = ?
                )
                SELECT
                    CAST(lp.productId AS VARCHAR) || '||' || lp.subTypeName AS Code,
                    COALESCE(pp.name, pp.cleanName, 'Product ' || CAST(lp.productId AS VARCHAR)) AS Name,
                    pp.number AS Number,
                    pp.rarity AS Rarity,
                    lp.subTypeName AS Subtype,
                    lp.marketPrice AS LatestPrice
                FROM {prices_from} lp
                LEFT JOIN pokemon_products pp
                  ON pp.groupId = lp.groupId
                 AND pp.productId = lp.productId
                WHERE lp.categoryId = 3
                  AND lp.groupId = ?
                  AND lp.date = (SELECT latest_date FROM latest_date)
                  AND lp.marketPrice IS NOT NULL
                ORDER BY
                  CASE WHEN pp.number IS NULL OR pp.number = '' THEN 1 ELSE 0 END,
                  pp.number,
                  Name,
                  Subtype
                """,
                [group_id, group_id],
            ).fetchdf()

            general = pd.DataFrame.from_dict(
                [
                    {
                        "Name": group_df.iloc[0]["name"],
                        "Code": str(group_id),
                        "Series": "Pokemon",
                        "ReleaseDate": group_df.iloc[0]["publishedOn"],
                    }
                ]
            )
            comp_df = components.set_index("Code") if not components.empty else pd.DataFrame(
                columns=["Name", "Number", "Rarity", "Subtype", "LatestPrice"]
            )
            return general, comp_df
        finally:
            con.close()
