"""
Quality gate — cross-layer pipeline verification and cross-target delivery validation.

This module owns:
  1. The single-query pipeline summary that spans all medallion layers.
  2. Cross-target delivery validation (ClickHouse gold vs Aurora PostgreSQL)
     with persistent logging to catalog.validation_log.

It lives outside any specific layer because it is the only code that
intentionally crosses layer boundaries and database engines.
"""

import logging
from datetime import date as _date
from typing import Any, Dict, List

from clickhouse_etl.db import get_hook

from common.config import (
    AURORA_SCHEMA,
    BRONZE_DATABASE,
    BRONZE_STG_EVENTS,
    CATALOG_DATABASE,
    CATALOG_VALIDATION_LOG,
    GOLD_DATABASE,
    GOLD_AGG_EVENTS_HOURLY,
    GOLD_AGG_HOURLY_HISTORY,
    GOLD_RPT_DAILY_SUMMARY,
    GOLD_RPT_DAILY_HISTORY,
    SILVER_DATABASE,
    SILVER_FACT_EVENTS,
    SILVER_QUARANTINE_EVENTS,
    SILVER_DIM_USERS,
    get_aurora_conn_id,
)

logger = logging.getLogger(__name__)


def get_pipeline_summary(batch_id: str, event_date: str) -> List[str]:
    """
    Run all verification queries in a single connection and return
    formatted summary lines.  Avoids 6+ separate round-trips.

    Uses the 'default' database so it can reach bronze.*, silver.*, gold.*
    in one query — this is why it lives here, not inside any layer repo.
    """
    result = get_hook("default").execute(
        f"SELECT "
        f"  (SELECT count() FROM {BRONZE_DATABASE}.{BRONZE_STG_EVENTS})                          AS bronze_total, "
        f"  (SELECT count() FROM {BRONZE_DATABASE}.{BRONZE_STG_EVENTS} WHERE batch_id = '{batch_id}') AS bronze_batch, "
        f"  (SELECT count() FROM {SILVER_DATABASE}.{SILVER_FACT_EVENTS})                         AS silver_total, "
        f"  (SELECT count() FROM {SILVER_DATABASE}.{SILVER_FACT_EVENTS} WHERE batch_id = '{batch_id}') AS silver_batch, "
        f"  (SELECT count() FROM {SILVER_DATABASE}.{SILVER_QUARANTINE_EVENTS})                   AS quarantine_total, "
        f"  (SELECT count() FROM {SILVER_DATABASE}.{SILVER_QUARANTINE_EVENTS} WHERE batch_id = '{batch_id}') AS quarantine_batch, "
        f"  (SELECT count() FROM {SILVER_DATABASE}.{SILVER_DIM_USERS} FINAL WHERE is_current = 1) AS dim_users_current, "
        f"  (SELECT count() FROM {GOLD_DATABASE}.{GOLD_AGG_EVENTS_HOURLY} FINAL "
        f"      WHERE event_date = '{event_date}')                                                AS gold_hourly_rows, "
        f"  (SELECT count() FROM {GOLD_DATABASE}.{GOLD_RPT_DAILY_SUMMARY} FINAL "
        f"      WHERE event_date = '{event_date}')                                                AS gold_daily_rows, "
        f"  (SELECT count() FROM {GOLD_DATABASE}.{GOLD_AGG_HOURLY_HISTORY})                       AS gold_hourly_history, "
        f"  (SELECT count() FROM {GOLD_DATABASE}.{GOLD_RPT_DAILY_HISTORY})                        AS gold_daily_history"
    )

    if not result:
        return ["No data available"]

    (b_total, b_batch, s_total, s_batch, q_total, q_batch, dim_users,
     g_hourly, g_daily, g_h_history, g_d_history) = result[0]

    return [
        f"Bronze  {BRONZE_STG_EVENTS:<25s}: {b_total:>8d} total | {b_batch:>6d} this batch",
        f"Silver  {SILVER_FACT_EVENTS:<25s}: {s_total:>8d} total | {s_batch:>6d} this batch",
        f"Silver  {SILVER_QUARANTINE_EVENTS:<25s}: {q_total:>8d} total | {q_batch:>6d} this batch (rejected)",
        f"Silver  {SILVER_DIM_USERS:<25s}: {dim_users:>8d} current users (SCD2)",
        f"Gold    {GOLD_AGG_EVENTS_HOURLY:<25s}: {g_hourly:>8d} rows for {event_date}",
        f"Gold    {GOLD_RPT_DAILY_SUMMARY:<25s}: {g_daily:>8d} rows for {event_date}",
        f"Gold    {GOLD_AGG_HOURLY_HISTORY:<25s}: {g_h_history:>8d} audit snapshots",
        f"Gold    {GOLD_RPT_DAILY_HISTORY:<25s}: {g_d_history:>8d} audit snapshots",
    ]


# ── Cross-target delivery validation (ClickHouse gold ↔ Aurora) ──────────────

_VALUE_TOLERANCE = 0.01  # floating-point comparison tolerance


def _read_ch_metrics(event_date: str, event_hour: int) -> Dict[str, Any]:
    """Read gold metrics from ClickHouse for a specific date + hour."""
    hook = get_hook(GOLD_DATABASE)

    hourly = hook.execute(
        f"SELECT count(), sum(event_count), sum(total_value) "
        f"FROM {GOLD_AGG_EVENTS_HOURLY} FINAL "
        f"WHERE event_date = '{event_date}' AND event_hour = {event_hour}"
    )
    daily = hook.execute(
        f"SELECT count(), sum(total_events) "
        f"FROM {GOLD_RPT_DAILY_SUMMARY} FINAL "
        f"WHERE event_date = '{event_date}'"
    )

    h = hourly[0] if hourly else (0, 0, 0.0)
    d = daily[0] if daily else (0, 0)

    return {
        "ch_hourly_rows": int(h[0]),
        "ch_hourly_events": int(h[1]),
        "ch_hourly_value": float(h[2]),
        "ch_daily_rows": int(d[0]),
        "ch_daily_events": int(d[1]),
    }


def _read_aurora_metrics(event_date: str, event_hour: int) -> Dict[str, Any]:
    """Read gold metrics from Aurora for a specific date + hour."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=get_aurora_conn_id(), schema="postgres")

    hourly = hook.get_records(
        f"SELECT count(*), coalesce(sum(event_count),0), coalesce(sum(total_value),0) "
        f"FROM {AURORA_SCHEMA}.agg_events_hourly "
        f"WHERE event_date = %s AND event_hour = %s",
        parameters=[event_date, event_hour],
    )
    daily = hook.get_records(
        f"SELECT count(*), coalesce(sum(total_events),0) "
        f"FROM {AURORA_SCHEMA}.rpt_daily_summary "
        f"WHERE event_date = %s",
        parameters=[event_date],
    )

    h = hourly[0] if hourly else (0, 0, 0.0)
    d = daily[0] if daily else (0, 0)

    return {
        "aurora_hourly_rows": int(h[0]),
        "aurora_hourly_events": int(h[1]),
        "aurora_hourly_value": float(h[2]),
        "aurora_daily_rows": int(d[0]),
        "aurora_daily_events": int(d[1]),
    }


def validate_delivery(
    run_id: str, event_date: str, event_hour: int,
) -> Dict[str, Any]:
    """Compare ClickHouse gold vs Aurora and log results to validation_log.

    Returns a dict with all metrics and match flags.
    Raises RuntimeError if any check fails.
    """
    ch = _read_ch_metrics(event_date, event_hour)
    au = _read_aurora_metrics(event_date, event_hour)

    hourly_rows_match = int(ch["ch_hourly_rows"] == au["aurora_hourly_rows"])
    hourly_events_match = int(ch["ch_hourly_events"] == au["aurora_hourly_events"])
    hourly_value_match = int(
        abs(ch["ch_hourly_value"] - au["aurora_hourly_value"]) < _VALUE_TOLERANCE
    )
    daily_rows_match = int(ch["ch_daily_rows"] == au["aurora_daily_rows"])
    daily_events_match = int(ch["ch_daily_events"] == au["aurora_daily_events"])

    all_pass = all([
        hourly_rows_match, hourly_events_match, hourly_value_match,
        daily_rows_match, daily_events_match,
    ])
    overall_status = "pass" if all_pass else "fail"

    record = {
        **ch, **au,
        "hourly_rows_match": hourly_rows_match,
        "hourly_events_match": hourly_events_match,
        "hourly_value_match": hourly_value_match,
        "daily_rows_match": daily_rows_match,
        "daily_events_match": daily_events_match,
        "overall_status": overall_status,
    }

    # Log to catalog.validation_log (append-only — every check is preserved)
    # clickhouse-driver requires a Python date object for ClickHouse Date columns
    event_date_obj = _date.fromisoformat(event_date)
    get_hook(CATALOG_DATABASE).execute(
        f"INSERT INTO {CATALOG_VALIDATION_LOG} "
        f"(run_id, event_date, event_hour, "
        f"ch_hourly_rows, ch_hourly_events, ch_hourly_value, "
        f"ch_daily_rows, ch_daily_events, "
        f"aurora_hourly_rows, aurora_hourly_events, aurora_hourly_value, "
        f"aurora_daily_rows, aurora_daily_events, "
        f"hourly_rows_match, hourly_events_match, hourly_value_match, "
        f"daily_rows_match, daily_events_match, overall_status) VALUES",
        [(
            run_id, event_date_obj, event_hour,
            ch["ch_hourly_rows"], ch["ch_hourly_events"], ch["ch_hourly_value"],
            ch["ch_daily_rows"], ch["ch_daily_events"],
            au["aurora_hourly_rows"], au["aurora_hourly_events"], au["aurora_hourly_value"],
            au["aurora_daily_rows"], au["aurora_daily_events"],
            hourly_rows_match, hourly_events_match, hourly_value_match,
            daily_rows_match, daily_events_match, overall_status,
        )],
    )

    logger.info(
        "Delivery validation [%s] event_date=%s hour=%02d: "
        "CH hourly=%d/%d/%.2f  Aurora hourly=%d/%d/%.2f  "
        "CH daily=%d/%d  Aurora daily=%d/%d  → %s",
        run_id, event_date, event_hour,
        ch["ch_hourly_rows"], ch["ch_hourly_events"], ch["ch_hourly_value"],
        au["aurora_hourly_rows"], au["aurora_hourly_events"], au["aurora_hourly_value"],
        ch["ch_daily_rows"], ch["ch_daily_events"],
        au["aurora_daily_rows"], au["aurora_daily_events"],
        overall_status.upper(),
    )

    if not all_pass:
        checks = [
            (hourly_rows_match,   "hourly rows",   "ch_hourly_rows",   "aurora_hourly_rows"),
            (hourly_events_match, "hourly events", "ch_hourly_events", "aurora_hourly_events"),
            (hourly_value_match,  "hourly value",  "ch_hourly_value",  "aurora_hourly_value"),
            (daily_rows_match,    "daily rows",    "ch_daily_rows",    "aurora_daily_rows"),
            (daily_events_match,  "daily events",  "ch_daily_events",  "aurora_daily_events"),
        ]
        mismatches = [
            f"{label}: CH={ch[ck]} vs Aurora={au[ak]}"
            for flag, label, ck, ak in checks if not flag
        ]
        raise RuntimeError(
            f"Delivery validation FAILED for {event_date} hour={event_hour}: "
            + "; ".join(mismatches)
        )

    return record
