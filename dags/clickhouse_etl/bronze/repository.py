"""
Bronze repository — raw data ingest into stg_events.

Writes source data as-is. No transformations, no cleaning.
Owns only bronze-layer logic.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from clickhouse_etl.db import get_hook, scalar_int, ensure_database, run_ddl

from common.config import (
    BRONZE_DATABASE,
    BRONZE_STG_EVENTS,
)

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent / "sql"

_hook = lambda: get_hook(BRONZE_DATABASE)


def ensure_table() -> None:
    """Create bronze database and stg_events table if they do not exist."""
    ensure_database(BRONZE_DATABASE)
    run_ddl(BRONZE_DATABASE, SQL_DIR, ["create_raw_events.sql"])
    logger.info("Bronze: ensured %s.%s", BRONZE_DATABASE, BRONZE_STG_EVENTS)


def get_batch_count(batch_id: str) -> int:
    """Return the row count for a specific batch in stg_events."""
    return scalar_int(_hook().execute(
        f"SELECT count() FROM {BRONZE_STG_EVENTS} WHERE batch_id = '{batch_id}'"
    ))


def batch_exists(batch_id: str) -> bool:
    """Check if a batch has already been ingested."""
    return get_batch_count(batch_id) > 0


def insert_raw_events(rows: List[Dict]) -> int:
    """Bulk-insert raw event dicts into stg_events. Returns row count."""
    if not rows:
        return 0

    data = [
        (
            r["event_id"],
            r["user_name"],
            r["category"],
            r["source"],
            r["value"],
            datetime.strptime(r["event_time"], "%Y-%m-%d %H:%M:%S"),
            r["batch_id"],
        )
        for r in rows
    ]

    _hook().execute(
        f"INSERT INTO {BRONZE_STG_EVENTS} "
        f"(event_id, user_name, category, source, value, event_time, batch_id) "
        f"VALUES",
        data,
    )
    logger.info("Bronze: inserted %d raw rows into %s", len(data), BRONZE_STG_EVENTS)
    return len(data)
