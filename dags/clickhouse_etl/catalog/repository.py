"""
Catalog repository — metadata governance for ClickHouse.

Provides metadata registration, data lineage tracking, and pipeline
execution logging across all medallion layers.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from clickhouse_etl.db import get_hook, ensure_database, run_ddl

from common.config import (
    CATALOG_DATABASE,
    CATALOG_TABLE_REGISTRY,
    CATALOG_COLUMN_REGISTRY,
    CATALOG_DATA_LINEAGE,
    CATALOG_PIPELINE_RUNS,
    CATALOG_VALIDATION_LOG,
    BRONZE_DATABASE,
    BRONZE_STG_EVENTS,
    SILVER_DATABASE,
    SILVER_FACT_EVENTS,
    SILVER_QUARANTINE_EVENTS,
    SILVER_DIM_USERS,
    GOLD_DATABASE,
    GOLD_AGG_EVENTS_HOURLY,
    GOLD_AGG_HOURLY_HISTORY,
    GOLD_RPT_DAILY_SUMMARY,
    GOLD_RPT_DAILY_HISTORY,
)

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent / "sql"

_hook = lambda: get_hook(CATALOG_DATABASE)


# ── DDL ───────────────────────────────────────────────────────────────────────

def ensure_tables() -> None:
    """Create catalog database and all catalog tables."""
    ensure_database(CATALOG_DATABASE)
    run_ddl(CATALOG_DATABASE, SQL_DIR, [
        "create_table_registry.sql",
        "create_column_registry.sql",
        "create_data_lineage.sql",
        "create_pipeline_runs.sql",
        "create_validation_log.sql",
        "create_benchmark_log.sql",
    ])
    logger.info("Catalog: ensured all catalog tables in %s", CATALOG_DATABASE)


# ── Table Registration ────────────────────────────────────────────────────────

def register_all_tables() -> None:
    """Register every medallion table in the catalog (idempotent)."""
    tables = [
        # Bronze
        {
            "table_id": f"{BRONZE_DATABASE}.{BRONZE_STG_EVENTS}",
            "database_name": BRONZE_DATABASE,
            "table_name": BRONZE_STG_EVENTS,
            "layer": "bronze",
            "engine": "MergeTree",
            "description": "Raw append-only staging events from source",
            "owner": "data-engineering",
            "partition_key": "toYYYYMMDD(event_time)",
            "order_key": "(batch_id, event_id)",
            "tags": ["raw", "events", "append-only"],
        },
        # Silver - facts
        {
            "table_id": f"{SILVER_DATABASE}.{SILVER_FACT_EVENTS}",
            "database_name": SILVER_DATABASE,
            "table_name": SILVER_FACT_EVENTS,
            "layer": "silver",
            "engine": "ReplacingMergeTree",
            "description": "Cleaned and deduplicated event facts",
            "owner": "data-engineering",
            "partition_key": "toYYYYMMDD(event_date)",
            "order_key": "(event_id)",
            "tags": ["cleaned", "facts", "deduplicated"],
        },
        # Silver - quarantine (rejected rows)
        {
            "table_id": f"{SILVER_DATABASE}.{SILVER_QUARANTINE_EVENTS}",
            "database_name": SILVER_DATABASE,
            "table_name": SILVER_QUARANTINE_EVENTS,
            "layer": "silver",
            "engine": "MergeTree",
            "description": "Quarantined rows rejected during bronze-to-silver transformation",
            "owner": "data-engineering",
            "partition_key": "toYYYYMMDD(event_time)",
            "order_key": "(batch_id, event_id, user_name)",
            "tags": ["quarantine", "data-quality", "rejected"],
        },
        # Silver - SCD2 dimension
        {
            "table_id": f"{SILVER_DATABASE}.{SILVER_DIM_USERS}",
            "database_name": SILVER_DATABASE,
            "table_name": SILVER_DIM_USERS,
            "layer": "silver",
            "engine": "ReplacingMergeTree",
            "description": "SCD Type 2 user dimension with full history",
            "owner": "data-engineering",
            "partition_key": "toYYYYMM(valid_from)",
            "order_key": "(user_name, valid_from)",
            "tags": ["dimension", "scd2", "users"],
        },
        # Gold - hourly
        {
            "table_id": f"{GOLD_DATABASE}.{GOLD_AGG_EVENTS_HOURLY}",
            "database_name": GOLD_DATABASE,
            "table_name": GOLD_AGG_EVENTS_HOURLY,
            "layer": "gold",
            "engine": "ReplacingMergeTree",
            "description": "Hourly event aggregates by category and source",
            "owner": "data-engineering",
            "partition_key": "toYYYYMM(event_date)",
            "order_key": "(event_date, event_hour, category, source)",
            "tags": ["aggregated", "hourly", "metrics"],
        },
        # Gold - hourly history (audit trail)
        {
            "table_id": f"{GOLD_DATABASE}.{GOLD_AGG_HOURLY_HISTORY}",
            "database_name": GOLD_DATABASE,
            "table_name": GOLD_AGG_HOURLY_HISTORY,
            "layer": "gold",
            "engine": "MergeTree",
            "description": "Audit trail: snapshots of hourly aggregates before re-aggregation",
            "owner": "data-engineering",
            "partition_key": "toYYYYMM(event_date)",
            "order_key": "(event_date, event_hour, category, source, _snapshot_ts)",
            "tags": ["history", "gold-audit", "audit"],
        },
        # Gold - daily
        {
            "table_id": f"{GOLD_DATABASE}.{GOLD_RPT_DAILY_SUMMARY}",
            "database_name": GOLD_DATABASE,
            "table_name": GOLD_RPT_DAILY_SUMMARY,
            "layer": "gold",
            "engine": "ReplacingMergeTree",
            "description": "Daily business KPI report",
            "owner": "data-engineering",
            "partition_key": "toYYYYMM(event_date)",
            "order_key": "(event_date)",
            "tags": ["aggregated", "daily", "kpi"],
        },
        # Gold - daily history (audit trail)
        {
            "table_id": f"{GOLD_DATABASE}.{GOLD_RPT_DAILY_HISTORY}",
            "database_name": GOLD_DATABASE,
            "table_name": GOLD_RPT_DAILY_HISTORY,
            "layer": "gold",
            "engine": "MergeTree",
            "description": "Audit trail: snapshots of daily summary before re-aggregation",
            "owner": "data-engineering",
            "partition_key": "toYYYYMM(event_date)",
            "order_key": "(event_date, _snapshot_ts)",
            "tags": ["history", "gold-audit", "audit"],
        },
    ]

    hook = _hook()
    for t in tables:
        hook.execute(
            f"INSERT INTO {CATALOG_TABLE_REGISTRY} "
            f"(table_id, database_name, table_name, layer, engine, "
            f"description, owner, partition_key, order_key, tags) VALUES",
            [(
                t["table_id"], t["database_name"], t["table_name"],
                t["layer"], t["engine"], t["description"], t["owner"],
                t["partition_key"], t["order_key"], t["tags"],
            )],
        )

    logger.info("Catalog: registered %d tables in table_registry", len(tables))


# ── Column Registration ──────────────────────────────────────────────────────

def _col(db: str, tbl: str, name: str, dtype: str, desc: str,
         pk: int = 0, ok: int = 0, tags: Optional[List[str]] = None) -> dict:
    """Helper to build a column_registry entry."""
    return {
        "column_id": f"{db}.{tbl}.{name}",
        "table_id": f"{db}.{tbl}",
        "column_name": name,
        "data_type": dtype,
        "is_nullable": 0,
        "is_partition_key": pk,
        "is_order_key": ok,
        "description": desc,
        "tags": tags or [],
    }


def register_all_columns() -> None:
    """Register column-level metadata for key medallion tables (idempotent).

    Uses ReplacingMergeTree on column_id so re-runs update rather than
    duplicate.
    """
    B, S, G = BRONZE_DATABASE, SILVER_DATABASE, GOLD_DATABASE
    columns = [
        # ── Bronze: stg_events ────────────────────────────────────────────
        _col(B, BRONZE_STG_EVENTS, "event_id",   "String",   "Unique event identifier", ok=1),
        _col(B, BRONZE_STG_EVENTS, "user_name",  "String",   "User who triggered the event"),
        _col(B, BRONZE_STG_EVENTS, "category",   "LowCardinality(String)", "Event category"),
        _col(B, BRONZE_STG_EVENTS, "source",     "LowCardinality(String)", "Traffic source / channel"),
        _col(B, BRONZE_STG_EVENTS, "value",      "Float64",  "Monetary value of the event", tags=["metric"]),
        _col(B, BRONZE_STG_EVENTS, "event_time", "DateTime", "Event timestamp from source", pk=1),
        _col(B, BRONZE_STG_EVENTS, "batch_id",   "String",   "Ingestion batch identifier", ok=1, tags=["lineage"]),

        # ── Silver: fact_events ───────────────────────────────────────────
        _col(S, SILVER_FACT_EVENTS, "event_id",   "String",   "Deduplicated event identifier", ok=1),
        _col(S, SILVER_FACT_EVENTS, "user_name",  "String",   "Cleaned user name"),
        _col(S, SILVER_FACT_EVENTS, "category",   "LowCardinality(String)", "Normalised category"),
        _col(S, SILVER_FACT_EVENTS, "source",     "LowCardinality(String)", "Normalised source"),
        _col(S, SILVER_FACT_EVENTS, "value",      "Float64",  "Clamped monetary value (>=0)", tags=["metric"]),
        _col(S, SILVER_FACT_EVENTS, "event_date", "Date",     "Derived date from event_time", pk=1),
        _col(S, SILVER_FACT_EVENTS, "event_hour", "UInt8",    "Derived hour (0-23)"),
        _col(S, SILVER_FACT_EVENTS, "is_revenue_event", "UInt8", "1 if category=purchase AND value>0", tags=["metric", "derived"]),
        _col(S, SILVER_FACT_EVENTS, "batch_id",   "String",   "Source batch for lineage", tags=["lineage"]),

        # ── Silver: quarantine_events ─────────────────────────────────────
        _col(S, SILVER_QUARANTINE_EVENTS, "event_id",         "String",   "Raw event_id (may be empty)"),
        _col(S, SILVER_QUARANTINE_EVENTS, "rejection_reason", "String",   "Why this row was rejected", tags=["data-quality"]),
        _col(S, SILVER_QUARANTINE_EVENTS, "batch_id",         "String",   "Source batch for lineage", ok=1, tags=["lineage"]),

        # ── Silver: dim_users (SCD2) ──────────────────────────────────────
        _col(S, SILVER_DIM_USERS, "user_name",        "String",   "Business key", ok=1),
        _col(S, SILVER_DIM_USERS, "primary_category",  "String",   "Most frequent category"),
        _col(S, SILVER_DIM_USERS, "total_events",      "UInt64",   "Lifetime event count", tags=["metric"]),
        _col(S, SILVER_DIM_USERS, "total_value",        "Float64",  "Lifetime monetary value", tags=["metric"]),
        _col(S, SILVER_DIM_USERS, "is_current",         "UInt8",    "1 if current version (SCD2)", tags=["scd2"]),
        _col(S, SILVER_DIM_USERS, "valid_from",         "DateTime", "SCD2 version start", ok=1, pk=1, tags=["scd2"]),
        _col(S, SILVER_DIM_USERS, "valid_to",           "DateTime", "SCD2 version end", tags=["scd2"]),

        # ── Gold: agg_events_hourly ───────────────────────────────────────
        _col(G, GOLD_AGG_EVENTS_HOURLY, "event_date",     "Date",    "Aggregation date", pk=1, ok=1),
        _col(G, GOLD_AGG_EVENTS_HOURLY, "event_hour",     "UInt8",   "Aggregation hour (0-23)", ok=1),
        _col(G, GOLD_AGG_EVENTS_HOURLY, "category",       "LowCardinality(String)", "Event category", ok=1),
        _col(G, GOLD_AGG_EVENTS_HOURLY, "source",         "LowCardinality(String)", "Traffic source", ok=1),
        _col(G, GOLD_AGG_EVENTS_HOURLY, "event_count",    "UInt64",  "Number of events", tags=["kpi"]),
        _col(G, GOLD_AGG_EVENTS_HOURLY, "revenue_events", "UInt64",  "Purchase events", tags=["kpi"]),
        _col(G, GOLD_AGG_EVENTS_HOURLY, "total_value",    "Float64", "Total monetary value", tags=["kpi"]),

        # ── Gold: rpt_daily_summary ───────────────────────────────────────
        _col(G, GOLD_RPT_DAILY_SUMMARY, "event_date",     "Date",    "Report date", pk=1, ok=1),
        _col(G, GOLD_RPT_DAILY_SUMMARY, "total_events",   "UInt64",  "Total events for the day", tags=["kpi"]),
        _col(G, GOLD_RPT_DAILY_SUMMARY, "unique_users",   "UInt64",  "Distinct users", tags=["kpi"]),
        _col(G, GOLD_RPT_DAILY_SUMMARY, "revenue_events", "UInt64",  "Purchase events", tags=["kpi"]),
        _col(G, GOLD_RPT_DAILY_SUMMARY, "total_revenue",  "Float64", "Total monetary revenue", tags=["kpi"]),
        _col(G, GOLD_RPT_DAILY_SUMMARY, "avg_value",      "Float64", "Average value per event", tags=["kpi"]),
    ]

    hook = _hook()
    for c in columns:
        hook.execute(
            f"INSERT INTO {CATALOG_COLUMN_REGISTRY} "
            f"(column_id, table_id, column_name, data_type, is_nullable, "
            f"is_partition_key, is_order_key, description, tags) VALUES",
            [(
                c["column_id"], c["table_id"], c["column_name"],
                c["data_type"], c["is_nullable"],
                c["is_partition_key"], c["is_order_key"],
                c["description"], c["tags"],
            )],
        )

    logger.info("Catalog: registered %d columns in column_registry", len(columns))


# ── Lineage Registration ─────────────────────────────────────────────────────

def register_lineage() -> None:
    """Register all data lineage relationships between tables."""
    lineage = [
        {
            "lineage_id": f"{BRONZE_DATABASE}.{BRONZE_STG_EVENTS}->{SILVER_DATABASE}.{SILVER_FACT_EVENTS}",
            "source_table_id": f"{BRONZE_DATABASE}.{BRONZE_STG_EVENTS}",
            "target_table_id": f"{SILVER_DATABASE}.{SILVER_FACT_EVENTS}",
            "transformation": "trim, lowercase, clamp negatives, derive event_date/hour/is_revenue_event",
            "sql_file": "silver/sql/transform.sql",
            "layer_from": "bronze",
            "layer_to": "silver",
        },
        {
            "lineage_id": f"{BRONZE_DATABASE}.{BRONZE_STG_EVENTS}->{SILVER_DATABASE}.{SILVER_QUARANTINE_EVENTS}",
            "source_table_id": f"{BRONZE_DATABASE}.{BRONZE_STG_EVENTS}",
            "target_table_id": f"{SILVER_DATABASE}.{SILVER_QUARANTINE_EVENTS}",
            "transformation": "Quarantine: capture rows with empty event_id or user_name",
            "sql_file": "silver/sql/quarantine.sql",
            "layer_from": "bronze",
            "layer_to": "silver",
        },
        {
            "lineage_id": f"{SILVER_DATABASE}.{SILVER_FACT_EVENTS}->{SILVER_DATABASE}.{SILVER_DIM_USERS}",
            "source_table_id": f"{SILVER_DATABASE}.{SILVER_FACT_EVENTS}",
            "target_table_id": f"{SILVER_DATABASE}.{SILVER_DIM_USERS}",
            "transformation": "SCD Type 2: track user attribute changes over time",
            "sql_file": "silver/sql/update_dim_users.sql",
            "layer_from": "silver",
            "layer_to": "silver",
        },
        {
            "lineage_id": f"{SILVER_DATABASE}.{SILVER_FACT_EVENTS}->{GOLD_DATABASE}.{GOLD_AGG_EVENTS_HOURLY}",
            "source_table_id": f"{SILVER_DATABASE}.{SILVER_FACT_EVENTS}",
            "target_table_id": f"{GOLD_DATABASE}.{GOLD_AGG_EVENTS_HOURLY}",
            "transformation": "Aggregate by date, hour, category, source",
            "sql_file": "gold/sql/transform_hourly.sql",
            "layer_from": "silver",
            "layer_to": "gold",
        },
        {
            "lineage_id": f"{SILVER_DATABASE}.{SILVER_FACT_EVENTS}->{GOLD_DATABASE}.{GOLD_RPT_DAILY_SUMMARY}",
            "source_table_id": f"{SILVER_DATABASE}.{SILVER_FACT_EVENTS}",
            "target_table_id": f"{GOLD_DATABASE}.{GOLD_RPT_DAILY_SUMMARY}",
            "transformation": "Daily KPIs: total events, unique users, revenue",
            "sql_file": "gold/sql/transform_daily.sql",
            "layer_from": "silver",
            "layer_to": "gold",
        },
        {
            "lineage_id": f"{GOLD_DATABASE}.{GOLD_AGG_EVENTS_HOURLY}->{GOLD_DATABASE}.{GOLD_AGG_HOURLY_HISTORY}",
            "source_table_id": f"{GOLD_DATABASE}.{GOLD_AGG_EVENTS_HOURLY}",
            "target_table_id": f"{GOLD_DATABASE}.{GOLD_AGG_HOURLY_HISTORY}",
            "transformation": "Audit: snapshot before each re-aggregation",
            "sql_file": "gold/sql/snapshot_hourly_to_history.sql",
            "layer_from": "gold",
            "layer_to": "gold",
        },
        {
            "lineage_id": f"{GOLD_DATABASE}.{GOLD_RPT_DAILY_SUMMARY}->{GOLD_DATABASE}.{GOLD_RPT_DAILY_HISTORY}",
            "source_table_id": f"{GOLD_DATABASE}.{GOLD_RPT_DAILY_SUMMARY}",
            "target_table_id": f"{GOLD_DATABASE}.{GOLD_RPT_DAILY_HISTORY}",
            "transformation": "Audit: snapshot before each re-aggregation",
            "sql_file": "gold/sql/snapshot_daily_to_history.sql",
            "layer_from": "gold",
            "layer_to": "gold",
        },
    ]

    hook = _hook()
    for ln in lineage:
        hook.execute(
            f"INSERT INTO {CATALOG_DATA_LINEAGE} "
            f"(lineage_id, source_table_id, target_table_id, "
            f"transformation, sql_file, layer_from, layer_to) VALUES",
            [(
                ln["lineage_id"], ln["source_table_id"], ln["target_table_id"],
                ln["transformation"], ln["sql_file"],
                ln["layer_from"], ln["layer_to"],
            )],
        )

    logger.info("Catalog: registered %d lineage relationships", len(lineage))


# ── Pipeline Run Tracking ────────────────────────────────────────────────────

def start_pipeline_run(
    run_id: str, dag_id: str, batch_id: str, execution_date: datetime,
) -> None:
    """Log the start of a pipeline run."""
    _hook().execute(
        f"INSERT INTO {CATALOG_PIPELINE_RUNS} "
        f"(run_id, dag_id, batch_id, execution_date, status) VALUES",
        [(run_id, dag_id, batch_id, execution_date, "running")],
    )
    logger.info("Catalog: started pipeline run %s", run_id)


def complete_pipeline_run(
    run_id: str,
    dag_id: str,
    batch_id: str,
    execution_date: datetime,
    bronze_rows: int = 0,
    silver_rows: int = 0,
    gold_hourly_rows: int = 0,
    gold_daily_rows: int = 0,
    dim_users_new: int = 0,
    dim_users_updated: int = 0,
    started_at: Optional[str] = None,
    status: str = "success",
) -> None:
    """Log the completion of a pipeline run with row counts."""
    started = (
        datetime.strptime(started_at, "%Y-%m-%d %H:%M:%S")
        if started_at
        else datetime.now(timezone.utc).replace(tzinfo=None)
    )
    _hook().execute(
        f"INSERT INTO {CATALOG_PIPELINE_RUNS} "
        f"(run_id, dag_id, batch_id, execution_date, "
        f"bronze_rows, silver_rows, gold_hourly_rows, gold_daily_rows, "
        f"dim_users_new, dim_users_updated, status, started_at, completed_at) VALUES",
        [(
            run_id, dag_id, batch_id, execution_date,
            bronze_rows, silver_rows, gold_hourly_rows, gold_daily_rows,
            dim_users_new, dim_users_updated, status, started,
            datetime.now(timezone.utc).replace(tzinfo=None),
        )],
    )
    logger.info("Catalog: completed pipeline run %s — status=%s", run_id, status)

