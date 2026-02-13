"""
Airflow task callables (glue layer).

Thin wrappers that connect Airflow context (XCom, run_id)
to the generator and each medallion-layer repository.
No SQL, no ClickHouse imports, no business logic here.

Every task is wrapped with TaskBenchmark to capture timing,
throughput, and memory metrics for cross-framework comparison.

Includes tasks for:
  - Catalog: metadata registration, lineage, pipeline run tracking
  - Quarantine: reject invalid rows before silver transformation (separate task)
  - Gold audit trail: snapshots of reported data before re-aggregation
  - SCD Type 2: dim_users maintenance
  - S3 export: gold data to S3 in Parquet format
  - Aurora load: gold data replicated to Aurora PostgreSQL
  - Delivery validation: cross-target comparison (ClickHouse gold vs Aurora)
  - DAG failure callback: marks pipeline_runs as 'failed' if any task fails
"""

import logging
from typing import Any, Dict

from clickhouse_etl.generator import generate_events

from clickhouse_etl.bronze import repository as bronze_repo
from clickhouse_etl.silver import repository as silver_repo
from clickhouse_etl.gold import repository as gold_repo
from clickhouse_etl.catalog import repository as catalog_repo
from clickhouse_etl.aurora import repository as aurora_repo
from clickhouse_etl import quality
from clickhouse_etl.benchmarks import TaskBenchmark

logger = logging.getLogger(__name__)


# ── Helpers: reduce repeated XCom pulls ───────────────────────────────────────

def _batch_id(context: Dict[str, Any]) -> str:
    """Pull batch_id from load_bronze XCom (used by most tasks)."""
    return context["ti"].xcom_pull(key="batch_id", task_ids="load_bronze")


def _event_date(context: Dict[str, Any]) -> str:
    """Derive event_date string from the execution interval start."""
    return context["data_interval_start"].strftime("%Y-%m-%d")


# ── Schema setup ──────────────────────────────────────────────────────────────

def ensure_all_tables(**context) -> None:
    """Create every database and table across all layers + catalog + Aurora (idempotent)."""
    with TaskBenchmark(context, "ensure_all_tables", "setup"):
        catalog_repo.ensure_tables()
        bronze_repo.ensure_table()
        silver_repo.ensure_table()
        gold_repo.ensure_tables()
        aurora_repo.ensure_tables()
        logger.info("All schemas ensured (catalog / bronze / silver / gold / aurora)")


# ── Catalog: register metadata & start run ────────────────────────────────────

def register_catalog(**context) -> None:
    """Register all tables, columns, and lineage in the catalog (idempotent)."""
    with TaskBenchmark(context, "register_catalog", "catalog"):
        catalog_repo.register_all_tables()
        catalog_repo.register_all_columns()
        catalog_repo.register_lineage()
        logger.info("Catalog: all tables, columns, and lineage registered")


def start_pipeline_run(**context) -> None:
    """Log the start of a pipeline run in the catalog."""
    from datetime import datetime, timezone
    with TaskBenchmark(context, "start_pipeline_run", "catalog"):
        started_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        catalog_repo.start_pipeline_run(
            run_id=context["run_id"],
            dag_id=context["dag"].dag_id,
            batch_id=context["run_id"],
            execution_date=context["data_interval_start"],
        )
        context["ti"].xcom_push(key="started_at", value=started_at)


# ── Bronze: extract & raw ingest (no XCom for data) ──────────────────────────

def load_bronze(**context) -> None:
    """
    Generate events and write directly to bronze.stg_events.

    Only batch_id is pushed to XCom (a tiny string).
    Data never passes through Airflow's metadata DB.
    """
    with TaskBenchmark(context, "load_bronze", "bronze") as bm:
        execution_start = context["data_interval_start"]
        batch_id = context["run_id"]

        if bronze_repo.batch_exists(batch_id):
            count = bronze_repo.get_batch_count(batch_id)
            logger.info(
                "Bronze: batch %s already exists (%d rows). Skipping.",
                batch_id, count,
            )
            context["ti"].xcom_push(key="batch_id", value=batch_id)
            context["ti"].xcom_push(key="bronze_rows", value=count)
            bm.rows_out = count
            return

        rows = generate_events(execution_start, batch_id)
        inserted = bronze_repo.insert_raw_events(rows)
        logger.info("Bronze: generated & loaded %d rows for batch %s", inserted, batch_id)

        context["ti"].xcom_push(key="batch_id", value=batch_id)
        context["ti"].xcom_push(key="bronze_rows", value=inserted)
        bm.rows_out = inserted


# ── Silver: quarantine invalid rows ───────────────────────────────────────────

def quarantine_invalid_rows(**context) -> None:
    """Quarantine rows that fail validation before silver transformation.

    Rows with empty event_id or user_name are captured in
    silver.quarantine_events with a rejection_reason — they are never
    silently dropped. Runs as a dedicated task so quarantine counts,
    timing, and status are independently visible in the Airflow UI.
    """
    with TaskBenchmark(context, "quarantine_invalid_rows", "silver") as bm:
        batch_id = _batch_id(context)
        bm.rows_in = context["ti"].xcom_pull(key="bronze_rows", task_ids="load_bronze") or 0

        quarantined = silver_repo.quarantine_batch(batch_id)
        context["ti"].xcom_push(key="quarantined_rows", value=quarantined)
        bm.rows_out = quarantined


# ── Silver: clean & deduplicate ───────────────────────────────────────────────

def transform_silver(**context) -> None:
    """Transform bronze.stg_events → silver.fact_events for the current batch.

    Only valid rows (those that passed quarantine) are transformed.
    """
    with TaskBenchmark(context, "transform_silver", "silver") as bm:
        batch_id = _batch_id(context)
        bm.rows_in = context["ti"].xcom_pull(key="bronze_rows", task_ids="load_bronze") or 0

        silver_repo.transform_batch(batch_id)
        count = silver_repo.get_batch_count(batch_id)
        context["ti"].xcom_push(key="silver_rows", value=count)
        bm.rows_out = count
        logger.info("Silver: %d cleaned rows for batch %s", count, batch_id)


# ── Gold Audit Trail: snapshot before re-aggregation ──────────────────────────

def snapshot_gold_history(**context) -> None:
    """Take audit snapshots of gold tables before re-aggregation."""
    with TaskBenchmark(context, "snapshot_gold_history", "gold"):
        batch_id = _batch_id(context)
        gold_repo.snapshot_hourly_to_history(batch_id)
        gold_repo.snapshot_daily_to_history(batch_id)
        logger.info("Gold Audit: snapshots complete for batch %s", batch_id)


# ── SCD Type 2: maintain user dimension ───────────────────────────────────────

def update_dim_users(**context) -> None:
    """Apply SCD Type 2 logic to silver.dim_users from current batch."""
    with TaskBenchmark(context, "update_dim_users", "silver") as bm:
        batch_id = _batch_id(context)
        new_count, updated_count = silver_repo.update_dim_users(batch_id)
        context["ti"].xcom_push(key="dim_users_new", value=new_count)
        context["ti"].xcom_push(key="dim_users_updated", value=updated_count)
        bm.rows_out = new_count + updated_count
        logger.info(
            "SCD2: dim_users — %d new, %d updated for batch %s",
            new_count, updated_count, batch_id,
        )


# ── Gold: aggregate business KPIs ─────────────────────────────────────────────

def _aggregate_gold(context: Dict[str, Any], agg_fn, count_fn, xcom_key: str, label: str) -> None:
    """Shared logic for gold aggregation tasks."""
    with TaskBenchmark(context, f"aggregate_gold_{label}", "gold") as bm:
        batch_id = _batch_id(context)
        agg_fn(batch_id)
        rows = count_fn(_event_date(context))
        context["ti"].xcom_push(key=xcom_key, value=rows)
        bm.rows_out = rows
        logger.info("Gold: %s aggregation complete — %d rows for batch %s", label, rows, batch_id)


def aggregate_gold_hourly(**context) -> None:
    """Aggregate silver.fact_events → gold.agg_events_hourly."""
    _aggregate_gold(context, gold_repo.aggregate_hourly, gold_repo.get_hourly_row_count, "gold_hourly_rows", "hourly")


def aggregate_gold_daily(**context) -> None:
    """Aggregate silver.fact_events → gold.rpt_daily_summary."""
    _aggregate_gold(context, gold_repo.aggregate_daily, gold_repo.get_daily_row_count, "gold_daily_rows", "daily")


# ── S3 Export: gold → S3 Parquet ──────────────────────────────────────────────

def export_gold_to_s3(**context) -> None:
    """Export gold tables to S3 as Parquet (Hive-compatible partitioning).

    Reads deduplicated gold data from ClickHouse, writes Parquet via
    Airflow S3Hook (using the pod's IAM role — no credentials in code).

    Hourly table: partitioned by event_date + event_hour
      → each file is immutable, written once per hour
    Daily table: partitioned by event_date only
      → progressive snapshot, overwritten as the day accumulates
    """
    with TaskBenchmark(context, "export_gold_to_s3", "export"):
        event_date = _event_date(context)
        event_hour = context["data_interval_start"].hour

        hourly_path = gold_repo.export_hourly_to_s3(event_date, event_hour)
        daily_path = gold_repo.export_daily_to_s3(event_date)

        logger.info(
            "S3 Export complete for event_date=%s hour=%02d:\n  %s\n  %s",
            event_date, event_hour, hourly_path, daily_path,
        )


# ── Aurora: replicate gold to PostgreSQL ──────────────────────────────────────

def load_aurora(**context) -> None:
    """Incremental load: UPSERT gold aggregates into Aurora PostgreSQL.

    Hourly — reads only the current hour from gold, UPSERTs by PK.
             Previous hours are never read or modified.
    Daily  — UPSERTs the single row for today's date.

    Uses INSERT … ON CONFLICT … DO UPDATE (true UPSERT).
    No DELETE.  No data gap.  No full reload.
    Credentials stored in Airflow Connection — nothing in code.
    """
    with TaskBenchmark(context, "load_aurora", "export") as bm:
        event_date = _event_date(context)
        event_hour = context["data_interval_start"].hour

        hourly_rows = aurora_repo.upsert_hourly(event_date, event_hour)
        daily_rows = aurora_repo.upsert_daily(event_date)
        bm.rows_out = hourly_rows + daily_rows

        logger.info(
            "Aurora: upserted %d hourly (hour=%02d) + %d daily rows for event_date=%s",
            hourly_rows, event_hour, daily_rows, event_date,
        )


# ── Delivery validation (ClickHouse gold vs Aurora) ──────────────────────

def validate_delivery(**context) -> None:
    """Cross-target validation: compare ClickHouse gold vs Aurora.

    Checks row counts, event totals, and monetary values for the current
    (event_date, event_hour).  Logs every check to catalog.validation_log
    (append-only — full audit trail of every validation).

    Raises RuntimeError and stops the pipeline if any metric doesn't match.
    """
    with TaskBenchmark(context, "validate_delivery", "validation"):
        event_date = _event_date(context)
        event_hour = context["data_interval_start"].hour

        result = quality.validate_delivery(
            run_id=context["run_id"],
            event_date=event_date,
            event_hour=event_hour,
        )
        logger.info(
            "Delivery validation PASSED for event_date=%s hour=%02d",
            event_date, event_hour,
        )


# ── Quality gate ──────────────────────────────────────────────────────────────

def verify_pipeline(**context) -> None:
    """Single round-trip verification across all layers."""
    with TaskBenchmark(context, "verify_pipeline", "validation"):
        batch_id = _batch_id(context)
        event_date = _event_date(context)

        summary = quality.get_pipeline_summary(batch_id, event_date)
        logger.info("═══ Pipeline Summary (batch: %s) ═══", batch_id)
        for line in summary:
            logger.info("  %s", line)


# ── Catalog: complete run ─────────────────────────────────────────────────────

def complete_pipeline_run(**context) -> None:
    """Log pipeline completion with row counts in the catalog."""
    ti = context["ti"]
    run_id = context["run_id"]

    bronze_rows = ti.xcom_pull(key="bronze_rows", task_ids="load_bronze") or 0
    quarantined_rows = ti.xcom_pull(key="quarantined_rows", task_ids="quarantine_invalid_rows") or 0
    silver_rows = ti.xcom_pull(key="silver_rows", task_ids="transform_silver") or 0
    gold_hourly = ti.xcom_pull(key="gold_hourly_rows", task_ids="aggregate_gold_hourly") or 0
    gold_daily = ti.xcom_pull(key="gold_daily_rows", task_ids="aggregate_gold_daily") or 0
    dim_new = ti.xcom_pull(key="dim_users_new", task_ids="update_dim_users") or 0
    dim_updated = ti.xcom_pull(key="dim_users_updated", task_ids="update_dim_users") or 0
    started_at = ti.xcom_pull(key="started_at", task_ids="start_pipeline_run")

    catalog_repo.complete_pipeline_run(
        run_id=run_id,
        dag_id=context["dag"].dag_id,
        batch_id=run_id,
        execution_date=context["data_interval_start"],
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        gold_hourly_rows=gold_hourly,
        gold_daily_rows=gold_daily,
        dim_users_new=dim_new,
        dim_users_updated=dim_updated,
        started_at=started_at,
        status="success",
    )
    if quarantined_rows:
        logger.warning(
            "Catalog: pipeline run %s — %d rows quarantined (rejected)",
            run_id, quarantined_rows,
        )
    logger.info("Catalog: pipeline run %s logged as success", run_id)


# ── DAG-level failure callback ────────────────────────────────────────────

def on_dag_failure(context: Dict[str, Any]) -> None:
    """Mark the pipeline run as 'failed' in the catalog.

    Attached to the DAG via on_failure_callback so that every run reaches
    a terminal status (success or failed) — even when a mid-pipeline task
    crashes before complete_pipeline_run executes.
    """
    run_id = context.get("run_id", "unknown")
    dag_id = context.get("dag", None)
    dag_id = dag_id.dag_id if dag_id else "unknown"

    try:
        catalog_repo.complete_pipeline_run(
            run_id=run_id,
            dag_id=dag_id,
            batch_id=run_id,
            execution_date=context.get("data_interval_start"),
            status="failed",
        )
        logger.error("Catalog: pipeline run %s marked as FAILED", run_id)
    except Exception:
        logger.exception(
            "Catalog: could not mark run %s as failed (catalog may be unreachable)",
            run_id,
        )
