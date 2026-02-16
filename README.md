# Airflow + ClickHouse — Enterprise Medallion ETL Pipeline

Production-grade hourly ETL pipeline built with **Apache Airflow 2.7.2**, **ClickHouse**, and **Aurora PostgreSQL**,
following the medallion architecture (Bronze → Silver → Gold) with built-in data governance, audit trails, and multi-target delivery.

**5 databases | 16 tables | 15 tasks | 23 SQL files | 7 lineage flows | 0 credentials in code**

---

## KPIs Produced

Two sets of business KPIs, refreshed every hour and delivered to **ClickHouse**, **S3 (Parquet)**, and **Aurora PostgreSQL**:

| KPI | Table | Granularity | Metrics |
|-----|-------|-------------|---------|
| **Hourly Operational** | `agg_events_hourly` | `(event_date, event_hour, category, source)` | Event count, revenue events, total monetary value |
| **Daily Executive** | `rpt_daily_summary` | `(event_date)` | Total events, unique users, revenue events, total revenue, avg value |

| Delivery Target | Format | Consumer |
|-----------------|--------|----------|
| **ClickHouse** (gold) | Columnar tables | Analysts, ad hoc queries |
| **S3** (Parquet) | Hive-partitioned files | Athena, Spark, Trino |
| **Aurora PostgreSQL** | Relational tables (UPSERT) | Tableau, Grafana, Metabase |

After every run, `validate_delivery` compares all KPIs between ClickHouse and Aurora — results logged to `catalog.validation_log`.

---

## Architecture

```
                         ┌────────────────────────────────────┐
                         │         Metadata Catalog             │
                         │  table_registry  · column_registry  │
                         │  data_lineage    · pipeline_runs    │
                         │  validation_log  · benchmark_log    │
                         └────────────────────────────────────┘

  ┌──────────────┐       ┌──────────────────────┐       ┌───────────────────────┐
  │   BRONZE     │       │       SILVER         │       │        GOLD           │
  │              │       │                      │       │                       │
  │ stg_events   │──────>│ fact_events          │──────>│ agg_events_hourly     │──┐
  │ (MergeTree)  │       │ (ReplacingMergeTree) │       │ (ReplacingMergeTree)  │  │
  │              │       │                      │       │                       │  │
  │ append-only  │       │ dim_users (SCD2)      │       │ rpt_daily_summary     │──┤
  │ = audit trail│       │ (ReplacingMergeTree) │       │ (ReplacingMergeTree)  │  │
  │              │       │                      │       │                       │  │
  │              │       │ quarantine_events    │       │ *_history (audit)     │  │
  └──────────────┘       └──────────────────────┘       └───────────────────────┘  │
                                                                                   │
              ┌──────────────────────────────┐      ┌──────────────────────────┐   │
              │        S3 (Parquet)          │      │   Aurora PostgreSQL      │<──┘
              │  Athena / Spark / Trino      │      │   BI / Reporting tools   │
              └──────────────────────────────┘      └──────────────────────────┘
```

---

## Pipeline (15 Tasks)

```
ensure_all_tables → register_catalog → start_pipeline_run → load_bronze → quarantine_invalid_rows
                                                                                    │
                                                                             transform_silver
                                                                                    │
                                                                             update_dim_users (SCD2)
                                                                                    │
                                                                             snapshot_gold_history
                                                                                    │
                                                                          ┌─────────┴──────────┐
                                                                   agg_gold_hourly     agg_gold_daily
                                                                          └─────────┬──────────┘
                                                                                    │
                                                                             verify_pipeline
                                                                                    │
                                                                             export_gold_to_s3
                                                                                    │
                                                                             load_aurora
                                                                                    │
                                                                          validate_delivery
                                                                                    │
                                                                          complete_pipeline_run
```

| # | Task | What it does |
|---|------|-------------|
| 1 | `ensure_all_tables` | Creates all databases, tables, and schemas idempotently |
| 2 | `register_catalog` | Registers table metadata, column metadata, and 7 lineage relationships |
| 3 | `start_pipeline_run` | Logs run start in `pipeline_runs` |
| 4 | `load_bronze` | Generates events → `stg_events` (append-only, idempotent) |
| 5 | `quarantine_invalid_rows` | Captures rejected rows (empty IDs) → `quarantine_events` |
| 6 | `transform_silver` | Cleans, deduplicates valid rows → `fact_events` |
| 7 | `update_dim_users` | SCD Type 2: tracks user attribute changes over time |
| 8 | `snapshot_gold_history` | Audit snapshot of gold tables before re-aggregation |
| 9-10 | `aggregate_gold_*` | Hourly + daily aggregations (parallel) |
| 11 | `verify_pipeline` | Quality gate: cross-layer row count verification |
| 12 | `export_gold_to_s3` | Gold → S3 Parquet (Hive-partitioned) |
| 13 | `load_aurora` | Gold → Aurora PostgreSQL (incremental UPSERT) |
| 14 | `validate_delivery` | Cross-target check: ClickHouse gold == Aurora |
| 15 | `complete_pipeline_run` | Logs run completion with row counts |

---

## 16 Tables

| # | Database | Table | Engine | Purpose |
|---|----------|-------|--------|---------|
| 1-6 | `catalog` | `table_registry`, `column_registry`, `data_lineage`, `pipeline_runs`, `validation_log`, `benchmark_log` | Replacing/MergeTree | Metadata governance |
| 7 | `bronze` | `stg_events` | MergeTree | Raw staging (append-only) |
| 8-10 | `silver` | `fact_events`, `dim_users`, `quarantine_events` | Replacing/MergeTree | Cleaned facts + SCD2 + rejected rows |
| 11-14 | `gold` | `agg_events_hourly`, `rpt_daily_summary` + `*_history` | Replacing/MergeTree | Aggregates + audit snapshots |
| 15-16 | Aurora | `agg_events_hourly`, `rpt_daily_summary` | PostgreSQL | BI replicas (UPSERT) |

---

## Project Structure

```
dags/
├── clickhouse_incremental_etl_dag.py    # DAG definition (orchestration only)
├── common/
│   └── config.py                        # Centralised constants + Airflow Variables
└── clickhouse_etl/
    ├── db.py                            # Shared DB helpers
    ├── quality.py                       # Cross-layer verification + delivery validation
    ├── benchmarks.py                    # Per-task performance instrumentation
    ├── generator.py                     # Event data source
    ├── tasks.py                         # Task callables (thin glue — no SQL)
    ├── catalog/   (repository.py + 6 SQL)
    ├── bronze/    (repository.py + 1 SQL)
    ├── silver/    (repository.py + 6 SQL)
    ├── gold/      (repository.py + 8 SQL)
    └── aurora/    (repository.py + 2 SQL)
```

**23 SQL files** — every SQL statement lives in `.sql` files, rendered at runtime via `db.render_sql()`. No SQL in Python strings.

### Why This Many Files?

This is the **repository pattern** — the standard in Django, Spring Boot, dbt, and any Airflow project at scale. Each layer owns its own database logic; tasks never touch SQL directly.

**The alternative** (1-2 large files) would mean 2,000+ lines mixing ClickHouse, Aurora, S3, validation, and Airflow context — impossible to test, debug, or maintain independently.

**How to navigate in 2 minutes:**

1. Open `clickhouse_incremental_etl_dag.py` — see all 15 tasks in order
2. Click any task → goes to `tasks.py` → one function, clear name, no SQL
3. That function calls a `repository.py` → actual database logic
4. SQL lives in `.sql` files next to each repository → readable without Python

| File | Role | Why separate |
|------|------|-------------|
| `config.py` | All constants in one place | Change a table name once, not in 10 files |
| `db.py` | Shared DB helpers | Without it, 50+ lines repeated across 5 repos |
| `tasks.py` | Airflow glue (XCom, context) | Zero business logic — swap Airflow for anything |
| `quality.py` | Cross-layer validation | Crosses all layers — can't live inside any one repo |
| `*/repository.py` | One per layer | Test, deploy, debug each layer independently |

---

## Key Design Decisions

| Decision | Why |
|----------|-----|
| **Repository pattern** | Each layer owns its DB logic. Tasks never touch SQL directly |
| **SQL in `.sql` files** | Readable, diffable, testable in isolation |
| **`ReplacingMergeTree`** | Idempotent writes — safe re-runs without duplicates |
| **Hook caching (`lru_cache`)** | One ClickHouseHook per database, reused across calls |
| **`lru_cache` Variable getters** | Resolved at task runtime, not DAG parse time |
| **Bronze is append-only** | Raw data = audit trail. No separate backup needed |
| **Gold re-aggregates full window** | Corrections are automatically included |
| **`on_failure_callback`** | Every run reaches terminal state (success/failed) |
| **Quarantine table** | Invalid rows captured with rejection reason — nothing silently dropped. Placed between bronze and silver as a separate DAG task for independent visibility, retries, and benchmarking. Bronze must accept everything (ground truth); gold is too late (aggregated). Silver boundary is the only correct place to reject at row level. |
| **Zero credentials in code** | All secrets in Airflow Connections/Variables |

---

## Data Lineage

```
bronze.stg_events ──┬──> silver.fact_events ──> gold.agg_events_hourly ──> *_history
                    │                     └──> gold.rpt_daily_summary ──> *_history
                    ├──> silver.dim_users (SCD2)
                    └──> silver.quarantine_events (rejected rows)
```

All 7 lineage relationships are registered in `catalog.data_lineage` and queryable.

---

## Benchmarking

Every task is wrapped with `TaskBenchmark` — a context manager that records duration, throughput (`rows/sec`), and peak memory to `catalog.benchmark_log`. Framework-agnostic: implement the same pipeline in any orchestrator and call `record_benchmark(framework="your-tool")` with the same task names for side-by-side comparison.

```sql
SELECT framework, task_name, avg(duration_ms) AS avg_ms, avg(rows_per_second) AS avg_throughput
FROM catalog.benchmark_log WHERE status = 'success'
GROUP BY framework, task_name ORDER BY task_name, framework;
```

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/mpashmi/airflow-clickhouse-medallion-etl.git
cd airflow-clickhouse-medallion-etl
```

### 2. Airflow Variables (Admin → Variables)

Create these 5 variables in the Airflow UI:

| Variable | Example Value | Purpose |
|----------|---------------|---------|
| `clickhouse_conn_id` | `clickhouse_default` | Connection ID for ClickHouse |
| `aurora_conn_id` | `aurora_medallion_etl` | Connection ID for Aurora PostgreSQL |
| `alert_email` | `team@company.com` | Email for task failure alerts |
| `gold_export_s3_bucket` | `my-data-lake-bucket` | S3 bucket for Parquet export |
| `gold_export_s3_prefix` | `medallion/gold` | S3 key prefix inside the bucket |

### 3. Airflow Connections (Admin → Connections)

| Connection ID | Type | Host | Port | Schema | Auth |
|---------------|------|------|------|--------|------|
| `clickhouse_default` | ClickHouse | your-clickhouse-host | `9000` | `default` | Username + password |
| `aurora_medallion_etl` | Postgres | your-aurora-endpoint.rds.amazonaws.com | `5432` | `postgres` | Username + password |

S3 access uses the **pod IAM role** (EKS) — no S3 credentials needed in Airflow.

### 4. Deploy

```powershell
.\deploy.ps1 -DryRun    # preview what will be uploaded
.\deploy.ps1             # upload DAGs to S3 (never deletes existing files)
```

The DAG runs `@hourly`. All databases, tables, and schemas are created automatically on the first run.

---

## Tech Stack

| Component | Version | Role |
|-----------|---------|------|
| Apache Airflow | 2.7.2 | Orchestration, scheduling, alerting |
| ClickHouse | 26.1.2 | Columnar OLAP — bronze, silver, gold, catalog |
| Aurora PostgreSQL | — | BI delivery — gold replicas for Tableau/Grafana/Metabase |
| S3 + Parquet | — | Data lake — Hive-compatible for Athena/Spark/Trino |
| pyarrow | 14.0+ | Parquet serialisation for S3 export |
| Kubernetes (EKS) | — | Runtime — Airflow + ClickHouse deployment |

## Requirements

```
apache-airflow==2.7.2
apache-airflow-providers-postgres>=5.7.0
apache-airflow-providers-amazon>=8.0.0
airflow-clickhouse-plugin>=1.3.0
clickhouse-driver>=0.2.6
pyarrow>=14.0.0
```
