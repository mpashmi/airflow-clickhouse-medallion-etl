"""
Silver repository — cleaned & deduplicated fact_events.

Reads from bronze.stg_events, applies transformations via SQL,
writes to silver.fact_events.

Also handles:
  - SCD Type 2: maintains dim_users with full change history
"""

import logging
from pathlib import Path
from typing import Tuple

from clickhouse_etl.db import get_hook, scalar_int, render_sql, ensure_database, run_ddl

from common.config import (
    BRONZE_DATABASE,
    BRONZE_STG_EVENTS,
    SILVER_DATABASE,
    SILVER_FACT_EVENTS,
    SILVER_QUARANTINE_EVENTS,
    SILVER_DIM_USERS,
)

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent / "sql"

_hook = lambda: get_hook(SILVER_DATABASE)


# ── DDL ───────────────────────────────────────────────────────────────────────

def ensure_table() -> None:
    """Create silver database and all silver tables if they do not exist."""
    ensure_database(SILVER_DATABASE)
    run_ddl(SILVER_DATABASE, SQL_DIR, [
        "create_cleaned_events.sql",
        "create_quarantine_events.sql",
        "create_dim_users.sql",
    ])
    logger.info(
        "Silver: ensured %s.{%s, %s, %s}",
        SILVER_DATABASE, SILVER_FACT_EVENTS,
        SILVER_QUARANTINE_EVENTS, SILVER_DIM_USERS,
    )


# ── Transform ─────────────────────────────────────────────────────────────────

def transform_batch(batch_id: str) -> None:
    """
    Read raw rows for *batch_id* from bronze.stg_events,
    clean & enrich, and INSERT into silver.fact_events.

    Idempotent: ReplacingMergeTree deduplicates by event_id on merge.
    """
    sql = render_sql(
        SQL_DIR, "transform.sql",
        batch_id=batch_id,
        bronze_db=BRONZE_DATABASE,
        bronze_table=BRONZE_STG_EVENTS,
    )
    _hook().execute(sql)
    logger.info(
        "Silver: transformed batch %s  (%s.%s → %s.%s)",
        batch_id,
        BRONZE_DATABASE, BRONZE_STG_EVENTS,
        SILVER_DATABASE, SILVER_FACT_EVENTS,
    )


def quarantine_batch(batch_id: str) -> int:
    """
    Move rejected rows from bronze.stg_events to silver.quarantine_events.

    Rows are quarantined if event_id or user_name is empty.
    Idempotent: skips if batch was already quarantined.
    Returns the count of quarantined rows.
    """
    hook = _hook()

    existing = scalar_int(hook.execute(
        f"SELECT count() FROM {SILVER_QUARANTINE_EVENTS} WHERE batch_id = '{batch_id}'"
    ))
    if existing > 0:
        logger.info(
            "Silver: quarantine already done for batch %s (%d rows). Skipping.",
            batch_id, existing,
        )
        return existing

    sql = render_sql(
        SQL_DIR, "quarantine.sql",
        batch_id=batch_id,
        bronze_db=BRONZE_DATABASE,
        bronze_table=BRONZE_STG_EVENTS,
    )
    hook.execute(sql)
    count = scalar_int(hook.execute(
        f"SELECT count() FROM {SILVER_QUARANTINE_EVENTS} WHERE batch_id = '{batch_id}'"
    ))
    if count > 0:
        logger.warning(
            "Silver: quarantined %d invalid rows for batch %s", count, batch_id,
        )
    else:
        logger.info("Silver: no rows quarantined for batch %s", batch_id)
    return count


# ── SCD Type 2: dim_users ────────────────────────────────────────────────────

def update_dim_users(batch_id: str) -> Tuple[int, int]:
    """
    Apply SCD Type 2 logic to dim_users based on the current batch.

    Returns (new_users, updated_users) counts.
    """
    hook = _hook()

    before_count = scalar_int(hook.execute(
        f"SELECT count() FROM {SILVER_DIM_USERS} FINAL WHERE is_current = 1"
    ))

    sql = render_sql(
        SQL_DIR, "update_dim_users.sql",
        batch_id=batch_id,
        silver_db=SILVER_DATABASE,
        fact_table=SILVER_FACT_EVENTS,
        dim_table=SILVER_DIM_USERS,
    )

    for stmt in (s.strip() for s in sql.split(";") if s.strip()):
        hook.execute(stmt)

    after_count = scalar_int(hook.execute(
        f"SELECT count() FROM {SILVER_DIM_USERS} FINAL WHERE is_current = 1"
    ))

    updated_count = scalar_int(hook.execute(
        f"SELECT count() FROM {SILVER_DIM_USERS} "
        f"WHERE is_current = 0 AND valid_to >= now() - INTERVAL 1 MINUTE"
    ))

    new_count = after_count - before_count + updated_count

    logger.info(
        "Silver SCD2: dim_users — %d new, %d updated for batch %s",
        new_count, updated_count, batch_id,
    )
    return new_count, updated_count


# ── Counts ────────────────────────────────────────────────────────────────────

def get_batch_count(batch_id: str) -> int:
    """Return the row count for a specific batch in fact_events."""
    return scalar_int(_hook().execute(
        f"SELECT count() FROM {SILVER_FACT_EVENTS} WHERE batch_id = '{batch_id}'"
    ))
