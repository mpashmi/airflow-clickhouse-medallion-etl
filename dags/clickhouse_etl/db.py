"""
Shared database helpers used by all repository modules.

Eliminates repeated boilerplate across bronze, silver, gold, and catalog layers:
  - get_hook()              — cached ClickHouse connection per database
  - scalar_int()            — extract a single integer from a query result
  - render_sql()            — load .sql template and substitute placeholders
  - ensure_database()       — CREATE DATABASE IF NOT EXISTS
  - run_ddl()               — execute a list of DDL files against a database
"""

from functools import lru_cache
from pathlib import Path
from typing import List, Optional

from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

from common.config import get_clickhouse_conn_id


@lru_cache(maxsize=8)
def get_hook(database: str) -> ClickHouseHook:
    """Return a cached ClickHouseHook connected to the given database.

    Cached per database name so we reuse connections within the same
    worker process instead of creating a new hook on every call.
    The cache is per-process and lives for the duration of the task.
    """
    return ClickHouseHook(
        clickhouse_conn_id=get_clickhouse_conn_id(),
        database=database,
    )


def scalar_int(result: Optional[List[tuple]]) -> int:
    """Extract the first cell of the first row as int, or 0 if empty."""
    return result[0][0] if result else 0


def render_sql(sql_dir: Path, filename: str, **replacements: str) -> str:
    """Load a SQL template from *sql_dir* and substitute {placeholders}."""
    sql = (sql_dir / filename).read_text()
    for key, val in replacements.items():
        sql = sql.replace(f"{{{key}}}", str(val))
    return sql


def ensure_database(database: str) -> None:
    """CREATE DATABASE IF NOT EXISTS."""
    get_hook("default").execute(f"CREATE DATABASE IF NOT EXISTS {database}")


def run_ddl(database: str, sql_dir: Path, filenames: List[str]) -> None:
    """Execute a list of DDL files against the given database."""
    hook = get_hook(database)
    for f in filenames:
        hook.execute((sql_dir / f).read_text())
