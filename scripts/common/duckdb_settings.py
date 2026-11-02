"""Shared DuckDB settings for the nightly pipeline steps.

DuckDB lets a query use up to 80% of the machine's RAM by default. This server also runs
Elasticsearch, editors and other containers, so on 2026-11-02 the product signal snapshot
grew to 12.6 GB and the kernel killed it (exit -9), stopping the nightly update. Capping
DuckDB's memory makes large queries spill to disk (next to the database file) instead.
Not every query can spill: the sparkline and series snapshots build an ordered LIST() per
card, and both ran out at a 6 GB cap (sparkline with 8 threads, series even with 4). So the
cap is 10 GB, below the 12.6 GB that was killed, and 4 threads, which also lowers the peak.
Elasticsearch and the VS Code server use about 20 GB between them on this machine, so if
the nightly update is killed again, look at those first.
"""

import os

import duckdb

# Override with PM_DUCKDB_MEMORY_LIMIT (DuckDB syntax, e.g. "8GB") and PM_DUCKDB_THREADS
# if the server changes.
DEFAULT_MEMORY_LIMIT = "10GB"
DEFAULT_THREADS = 4


def connect_capped(db_path: str, **kwargs) -> duckdb.DuckDBPyConnection:
    """Open the DuckDB database with the pipeline's memory and thread caps applied."""
    con = duckdb.connect(db_path, **kwargs)
    limit = os.environ.get("PM_DUCKDB_MEMORY_LIMIT", DEFAULT_MEMORY_LIMIT)
    threads = int(os.environ.get("PM_DUCKDB_THREADS", DEFAULT_THREADS))
    con.execute(f"SET memory_limit = '{limit}'")
    con.execute(f"SET threads = {threads}")
    return con
