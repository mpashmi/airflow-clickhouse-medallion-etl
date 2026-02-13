"""
DAG: clickhouse_incremental_etl
================================
Hourly medallion-architecture ETL pipeline with **catalog**, **quarantine**,
**gold audit trail**, **SCD Type 2**, **S3 export**, **Aurora replication**,
and **delivery validation**.

    ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────────┐   ┌──────────────────┐
    │ ensure   │──>│ register │──>│ start    │──>│ load         │──>│ quarantine       │
    │ tables   │   │ catalog  │   │ run      │   │ bronze       │   │ invalid_rows     │
    └──────────┘   └──────────┘   └──────────┘   └──────────────┘   └────────┬─────────┘
         DDL         Metadata      Run log          Source+Ingest     Reject bad rows
                                                                             │
                     ┌───────────────────────────────────────────────────────┘
                     ▼
              ┌──────────────────┐
              │ transform_silver │  (clean & dedupe valid rows)
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │ update_dim_users │  (SCD Type 2)
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │ snapshot_gold    │  (audit: snapshot before re-aggregation)
              └────────┬─────────┘
                       ▼
    ┌───────────────────────┐   ┌───────────────────────┐
    │ agg_gold_hourly       │   │ agg_gold_daily        │  (parallel)
    └───────────┬───────────┘   └───────────┬───────────┘
                └───────────┬───────────────┘
                            ▼
              ┌──────────────────┐
              │ verify_pipeline  │  (quality gate)
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │ export_s3        │  (S3 Parquet)
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │ load_aurora      │  (BI delivery)
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │ validate_delivery│  (ClickHouse gold == Aurora?)
              └────────┬─────────┘
                       ▼
              ┌──────────────────┐
              │ complete_run     │
              └──────────────────┘

Features
--------
- **Catalog**       : Table registry, column metadata, data lineage, pipeline runs, validation log.
- **Gold Audit**    : History tables for gold (append-only snapshots of reported data).
- **SCD Type 2**    : ``dim_users`` tracks user attribute changes over time.
- **S3 Export**     : Gold tables exported to S3 as Parquet, partitioned by event_date + event_hour.
- **Aurora Load**   : Gold aggregates replicated to Aurora PostgreSQL for BI/reporting (incremental UPSERT).
- **Validation**    : Cross-target comparison (ClickHouse gold vs Aurora) with persistent log.
- **Quarantine**    : Invalid rows (empty event_id / user_name) captured in ``quarantine_events``
                      with rejection reasons — nothing silently dropped.
- **Bronze**        : Raw events in ``stg_events`` — append-only, full audit trail.
                      Bronze is immutable — it IS your time travel for raw/silver data.
- **Silver**        : Cleaned & deduplicated in ``fact_events`` + ``dim_users``.
- **Gold**          : ``agg_events_hourly`` + ``rpt_daily_summary`` — run in parallel.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.config import get_alert_email

from clickhouse_etl.tasks import (
    ensure_all_tables,
    register_catalog,
    start_pipeline_run,
    load_bronze,
    quarantine_invalid_rows,
    transform_silver,
    snapshot_gold_history,
    update_dim_users,
    aggregate_gold_hourly,
    aggregate_gold_daily,
    export_gold_to_s3,
    load_aurora,
    validate_delivery,
    verify_pipeline,
    complete_pipeline_run,
    on_dag_failure,
)

# ── Default args (applied to every task) ──────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "email": [get_alert_email()],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="clickhouse_incremental_etl",
    default_args=default_args,
    start_date=datetime(2026, 2, 13),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["clickhouse", "etl", "medallion", "catalog", "scd2",
          "gold-audit", "s3-export", "aurora", "validation", "quarantine"],
    doc_md=__doc__,
    on_failure_callback=on_dag_failure,
) as dag:

    # ── Setup ─────────────────────────────────────────────────────────────────
    t_ensure = PythonOperator(
        task_id="ensure_all_tables",
        python_callable=ensure_all_tables,
    )

    t_register_catalog = PythonOperator(
        task_id="register_catalog",
        python_callable=register_catalog,
    )

    t_start_run = PythonOperator(
        task_id="start_pipeline_run",
        python_callable=start_pipeline_run,
    )

    # ── Bronze ────────────────────────────────────────────────────────────────
    t_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=load_bronze,
    )

    # ── Silver: quarantine invalid rows first ────────────────────────────────
    t_quarantine = PythonOperator(
        task_id="quarantine_invalid_rows",
        python_callable=quarantine_invalid_rows,
    )

    # ── Silver: transform valid rows ──────────────────────────────────────────
    t_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_silver,
    )

    # ── SCD Type 2: dim_users ─────────────────────────────────────────────────
    t_dim_users = PythonOperator(
        task_id="update_dim_users",
        python_callable=update_dim_users,
    )

    # ── Gold Audit: snapshot before re-aggregation ──────────────────────────
    t_snapshot_gold = PythonOperator(
        task_id="snapshot_gold_history",
        python_callable=snapshot_gold_history,
    )

    # ── Gold ──────────────────────────────────────────────────────────────────
    t_gold_hourly = PythonOperator(
        task_id="aggregate_gold_hourly",
        python_callable=aggregate_gold_hourly,
    )

    t_gold_daily = PythonOperator(
        task_id="aggregate_gold_daily",
        python_callable=aggregate_gold_daily,
    )

    # ── S3 Export ──────────────────────────────────────────────────────────────
    t_export_s3 = PythonOperator(
        task_id="export_gold_to_s3",
        python_callable=export_gold_to_s3,
    )

    # ── Aurora Load ───────────────────────────────────────────────────────────
    t_load_aurora = PythonOperator(
        task_id="load_aurora",
        python_callable=load_aurora,
    )

    # ── Delivery Validation (ClickHouse gold ↔ Aurora) ────────────────────────
    t_validate = PythonOperator(
        task_id="validate_delivery",
        python_callable=validate_delivery,
    )

    # ── Quality gate & catalog completion ─────────────────────────────────────
    t_verify = PythonOperator(
        task_id="verify_pipeline",
        python_callable=verify_pipeline,
    )

    t_complete_run = PythonOperator(
        task_id="complete_pipeline_run",
        python_callable=complete_pipeline_run,
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    # Setup: DDL → catalog registration → log run start
    t_ensure >> t_register_catalog >> t_start_run

    # ETL: bronze → quarantine → silver
    t_start_run >> t_bronze >> t_quarantine >> t_silver

    # After silver: SCD2 dimension update
    t_silver >> t_dim_users

    # After dimension update: snapshot gold before re-aggregation
    t_dim_users >> t_snapshot_gold

    # Gold aggregations run in parallel
    t_snapshot_gold >> [t_gold_hourly, t_gold_daily]

    # After gold: verify pipeline first (quality gate before any export)
    [t_gold_hourly, t_gold_daily] >> t_verify

    # After verify passes: export to S3 (primary data lake)
    t_verify >> t_export_s3

    # After S3: replicate to Aurora (BI delivery)
    t_export_s3 >> t_load_aurora

    # After Aurora: validate delivery (ClickHouse gold == Aurora)
    t_load_aurora >> t_validate

    # Final: log run completion (only if validation passes)
    t_validate >> t_complete_run
