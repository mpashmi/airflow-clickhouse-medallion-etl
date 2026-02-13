"""
Gold repository — aggregated, business-ready data.

Reads from silver.fact_events, produces:
  - gold.agg_events_hourly  (hourly metrics)
  - gold.rpt_daily_summary  (daily business KPIs)

Also handles:
  - Audit trail: snapshots to history tables before each re-aggregation
  - S3 export: Parquet files with Hive-compatible partitioning

All gold tables use ReplacingMergeTree for idempotent re-runs.
Read queries use FINAL to return deduplicated results.
"""

import logging
from pathlib import Path
from typing import List

from clickhouse_etl.db import get_hook, scalar_int, render_sql, ensure_database, run_ddl

from common.config import (
    GOLD_AGG_EVENTS_HOURLY,
    GOLD_AGG_HOURLY_HISTORY,
    GOLD_DATABASE,
    GOLD_RPT_DAILY_SUMMARY,
    GOLD_RPT_DAILY_HISTORY,
    get_s3_bucket,
    get_s3_prefix,
    SILVER_DATABASE,
    SILVER_FACT_EVENTS,
)

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent / "sql"

_hook = lambda: get_hook(GOLD_DATABASE)


# ── DDL ───────────────────────────────────────────────────────────────────────

def ensure_tables() -> None:
    """Create gold database and all gold tables (incl. history) if they do not exist."""
    ensure_database(GOLD_DATABASE)
    run_ddl(GOLD_DATABASE, SQL_DIR, [
        "create_hourly_metrics.sql",
        "create_daily_summary.sql",
        "create_hourly_history.sql",
        "create_daily_history.sql",
    ])
    logger.info(
        "Gold: ensured %s.{%s, %s, %s, %s}",
        GOLD_DATABASE,
        GOLD_AGG_EVENTS_HOURLY, GOLD_AGG_HOURLY_HISTORY,
        GOLD_RPT_DAILY_SUMMARY, GOLD_RPT_DAILY_HISTORY,
    )


# ── Transforms ────────────────────────────────────────────────────────────────

def _gold_sql(filename: str, batch_id: str, **extra: str) -> str:
    """Load a gold SQL template with standard placeholders."""
    return render_sql(
        SQL_DIR, filename,
        batch_id=batch_id,
        silver_db=SILVER_DATABASE,
        silver_table=SILVER_FACT_EVENTS,
        gold_db=GOLD_DATABASE,
        **extra,
    )


def aggregate_hourly(batch_id: str) -> None:
    """Aggregate silver.fact_events → gold.agg_events_hourly."""
    _hook().execute(_gold_sql("transform_hourly.sql", batch_id))
    logger.info("Gold: aggregated %s for batch %s", GOLD_AGG_EVENTS_HOURLY, batch_id)


def aggregate_daily(batch_id: str) -> None:
    """Aggregate silver.fact_events → gold.rpt_daily_summary."""
    _hook().execute(_gold_sql("transform_daily.sql", batch_id))
    logger.info("Gold: aggregated %s for batch %s", GOLD_RPT_DAILY_SUMMARY, batch_id)


# ── Gold Audit: snapshot to history ──────────────────────────────────────────

def _snapshot_to_history(
    sql_file: str, batch_id: str, gold_table: str, history_table: str,
) -> None:
    """Generic snapshot of a gold table to its corresponding history table.

    Idempotent: skips if a snapshot for this batch already exists.
    """
    hook = _hook()
    existing = scalar_int(hook.execute(
        f"SELECT count() FROM {history_table} WHERE _snapshot_batch = '{batch_id}'"
    ))
    if existing > 0:
        logger.info(
            "Gold: snapshot of %s already exists for batch %s (%d rows). Skipping.",
            gold_table, batch_id, existing,
        )
        return

    hook.execute(_gold_sql(
        sql_file, batch_id,
        gold_table=gold_table,
        history_table=history_table,
    ))
    logger.info("Gold: snapshotted %s to history for batch %s", gold_table, batch_id)


def snapshot_hourly_to_history(batch_id: str) -> None:
    """Snapshot current hourly aggregates to history before re-aggregation."""
    _snapshot_to_history(
        "snapshot_hourly_to_history.sql", batch_id,
        GOLD_AGG_EVENTS_HOURLY, GOLD_AGG_HOURLY_HISTORY,
    )


def snapshot_daily_to_history(batch_id: str) -> None:
    """Snapshot current daily summary to history before re-aggregation."""
    _snapshot_to_history(
        "snapshot_daily_to_history.sql", batch_id,
        GOLD_RPT_DAILY_SUMMARY, GOLD_RPT_DAILY_HISTORY,
    )


# ── S3 Export (Parquet via Airflow S3Hook) ────────────────────────────────────
# Uses Airflow worker's IAM role for S3 access — no credentials in code,
# no dependency on ClickHouse server having S3 write permissions.
# Gold aggregates are small (hundreds of rows), so passing through Airflow is fine.
#
# Hourly table: partitioned by event_date AND event_hour
#   → each file is written once per hour, never overwritten by later runs
# Daily table: partitioned by event_date only
#   → overwritten hourly as the day accumulates (final snapshot at day end)

def _s3_key_hourly(table_name: str, event_date: str, event_hour: int) -> str:
    """Build Hive-compatible S3 key with date + hour partitions."""
    return (
        f"{get_s3_prefix()}/{table_name}"
        f"/event_date={event_date}"
        f"/event_hour={event_hour:02d}"
        f"/data.parquet"
    )


def _s3_key_daily(table_name: str, event_date: str) -> str:
    """Build Hive-compatible S3 key with date partition only."""
    return f"{get_s3_prefix()}/{table_name}/event_date={event_date}/data.parquet"


def _write_parquet_to_s3(rows: list, columns: List[str], s3_key: str) -> None:
    """Serialise rows as Parquet and upload to S3."""
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    table = pa.table(
        {col: [row[i] for row in rows] for i, col in enumerate(columns)}
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    S3Hook().load_bytes(
        buf.getvalue(), key=s3_key,
        bucket_name=get_s3_bucket(), replace=True,
    )


def _export_to_s3(table: str, columns: List[str], where: str, s3_key: str) -> str:
    """Query ClickHouse gold, write Parquet to S3, return the S3 URI."""
    col_list = ", ".join(columns)
    rows = _hook().execute(
        f"SELECT {col_list} FROM {table} FINAL WHERE {where}"
    )
    _write_parquet_to_s3(rows, columns, s3_key)
    friendly = f"s3://{get_s3_bucket()}/{s3_key}"
    logger.info("Gold Export: %s → %s (%d rows)", table, friendly, len(rows))
    return friendly


def export_hourly_to_s3(event_date: str, event_hour: int) -> str:
    """Export agg_events_hourly for a specific hour to S3 as Parquet."""
    columns = [
        "event_date", "event_hour", "category", "source",
        "event_count", "revenue_events", "total_value", "_updated_at",
    ]
    return _export_to_s3(
        GOLD_AGG_EVENTS_HOURLY, columns,
        f"event_date = '{event_date}' AND event_hour = {event_hour}",
        _s3_key_hourly(GOLD_AGG_EVENTS_HOURLY, event_date, event_hour),
    )


def export_daily_to_s3(event_date: str) -> str:
    """Export rpt_daily_summary for the given date to S3 as Parquet."""
    columns = [
        "event_date", "total_events", "unique_users",
        "revenue_events", "total_revenue", "avg_value", "_updated_at",
    ]
    return _export_to_s3(
        GOLD_RPT_DAILY_SUMMARY, columns,
        f"event_date = '{event_date}'",
        _s3_key_daily(GOLD_RPT_DAILY_SUMMARY, event_date),
    )


# ── Counts ────────────────────────────────────────────────────────────────────

def _row_count(table: str, event_date: str) -> int:
    """Return the number of deduplicated rows for a given date in a gold table."""
    return scalar_int(_hook().execute(
        f"SELECT count() FROM {table} FINAL WHERE event_date = '{event_date}'"
    ))


def get_hourly_row_count(event_date: str) -> int:
    """Return deduplicated row count for agg_events_hourly on a given date."""
    return _row_count(GOLD_AGG_EVENTS_HOURLY, event_date)


def get_daily_row_count(event_date: str) -> int:
    """Return deduplicated row count for rpt_daily_summary on a given date."""
    return _row_count(GOLD_RPT_DAILY_SUMMARY, event_date)


