"""
Aurora repository — replicate gold aggregates to Aurora PostgreSQL.

Reads deduplicated gold data from ClickHouse (via ClickHouseHook) and
upserts into Aurora PostgreSQL (via PostgresHook).  All credentials
live in Airflow Connections — nothing is hardcoded.

Incremental strategy:
  Hourly — reads only the current hour from gold, then UPSERT by PK
           (event_date, event_hour, category, source).  Previous hours
           are never read or modified.
  Daily  — UPSERT by PK (event_date).  First run of the day inserts,
           subsequent hourly runs update the single row in-place.

Uses PostgreSQL INSERT … ON CONFLICT … DO UPDATE (true UPSERT).
No DELETE.  No data gap.  No full reload.
"""

import logging
from pathlib import Path
from typing import List, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook

from clickhouse_etl.db import get_hook as ch_hook

from common.config import (
    get_aurora_conn_id,
    AURORA_SCHEMA,
    GOLD_DATABASE,
    GOLD_AGG_EVENTS_HOURLY,
    GOLD_RPT_DAILY_SUMMARY,
)

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent / "sql"


def _pg_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=get_aurora_conn_id(), schema="postgres")


# ── DDL ───────────────────────────────────────────────────────────────────────

def ensure_tables() -> None:
    """Create medallion_etl schema and tables in Aurora (idempotent).

    The schema is checked via information_schema first because the
    pipeline user may not have CREATE privilege on the database.
    If the schema doesn't exist and can't be created, we fail fast
    with a clear message instead of a cryptic permission error.
    """
    hook = _pg_hook()

    # Check if schema exists (requires only SELECT on information_schema)
    rows = hook.get_records(
        "SELECT 1 FROM information_schema.schemata "
        "WHERE schema_name = %s",
        parameters=[AURORA_SCHEMA],
    )
    if not rows:
        try:
            hook.run(f"CREATE SCHEMA IF NOT EXISTS {AURORA_SCHEMA}")
            logger.info("Aurora: created schema %s", AURORA_SCHEMA)
        except Exception as exc:
            raise RuntimeError(
                f"Schema '{AURORA_SCHEMA}' does not exist and this user "
                f"lacks CREATE privilege on the database. Ask a DBA to run: "
                f"CREATE SCHEMA {AURORA_SCHEMA}; "
                f"GRANT ALL ON SCHEMA {AURORA_SCHEMA} TO medallion_etl_user;"
            ) from exc

    for ddl_file in ["create_hourly_metrics.sql", "create_daily_summary.sql"]:
        sql = (SQL_DIR / ddl_file).read_text()
        hook.run(sql)
    logger.info("Aurora: ensured schema + tables in %s", AURORA_SCHEMA)


# ── Read from ClickHouse gold (scoped to the current hour / date) ─────────

def _read_gold_hourly(event_date: str, event_hour: int) -> List[Tuple]:
    """Read deduplicated hourly aggregates for a SPECIFIC hour only."""
    return ch_hook(GOLD_DATABASE).execute(
        f"SELECT event_date, event_hour, category, source, "
        f"event_count, revenue_events, total_value "
        f"FROM {GOLD_AGG_EVENTS_HOURLY} FINAL "
        f"WHERE event_date = '{event_date}' AND event_hour = {event_hour}"
    )


def _read_gold_daily(event_date: str) -> List[Tuple]:
    """Read deduplicated daily summary for a specific date."""
    return ch_hook(GOLD_DATABASE).execute(
        f"SELECT event_date, total_events, unique_users, "
        f"revenue_events, total_revenue, avg_value "
        f"FROM {GOLD_RPT_DAILY_SUMMARY} FINAL "
        f"WHERE event_date = '{event_date}'"
    )


# ── Incremental UPSERT to Aurora ─────────────────────────────────────────────

_UPSERT_HOURLY_SQL = f"""
INSERT INTO {AURORA_SCHEMA}.agg_events_hourly
    (event_date, event_hour, category, source,
     event_count, revenue_events, total_value, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (event_date, event_hour, category, source)
DO UPDATE SET
    event_count    = EXCLUDED.event_count,
    revenue_events = EXCLUDED.revenue_events,
    total_value    = EXCLUDED.total_value,
    updated_at     = now()
"""

_UPSERT_DAILY_SQL = f"""
INSERT INTO {AURORA_SCHEMA}.rpt_daily_summary
    (event_date, total_events, unique_users,
     revenue_events, total_revenue, avg_value, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, now())
ON CONFLICT (event_date)
DO UPDATE SET
    total_events   = EXCLUDED.total_events,
    unique_users   = EXCLUDED.unique_users,
    revenue_events = EXCLUDED.revenue_events,
    total_revenue  = EXCLUDED.total_revenue,
    avg_value      = EXCLUDED.avg_value,
    updated_at     = now()
"""


def _upsert_to_aurora(rows: List[Tuple], insert_sql: str, label: str) -> int:
    """Execute an UPSERT batch against Aurora. Returns row count."""
    if not rows:
        logger.info("Aurora: no %s rows — skipping", label)
        return 0

    conn = _pg_hook().get_conn()
    cursor = conn.cursor()
    try:
        cursor.executemany(insert_sql, rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info("Aurora: upserted %d %s rows into %s", len(rows), label, AURORA_SCHEMA)
    return len(rows)


def upsert_hourly(event_date: str, event_hour: int) -> int:
    """Incremental load: UPSERT hourly aggregates for a single hour."""
    rows = _read_gold_hourly(event_date, event_hour)
    return _upsert_to_aurora(rows, _UPSERT_HOURLY_SQL, f"hourly {event_date} h={event_hour:02d}")


def upsert_daily(event_date: str) -> int:
    """Incremental load: UPSERT daily summary for a single date."""
    rows = _read_gold_daily(event_date)
    return _upsert_to_aurora(rows, _UPSERT_DAILY_SQL, f"daily {event_date}")
