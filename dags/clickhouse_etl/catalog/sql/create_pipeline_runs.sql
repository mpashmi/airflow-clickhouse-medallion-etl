-- Catalog | create_pipeline_runs — Execution log for every pipeline run.
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           String          COMMENT 'Unique run identifier (Airflow run_id)',
    dag_id           String          COMMENT 'Airflow DAG ID',
    batch_id         String          COMMENT 'Batch identifier for lineage',
    execution_date   DateTime        COMMENT 'Airflow logical execution date',
    bronze_rows      UInt64 DEFAULT 0 COMMENT 'Rows loaded to bronze',
    silver_rows      UInt64 DEFAULT 0 COMMENT 'Rows transformed to silver',
    gold_hourly_rows UInt64 DEFAULT 0 COMMENT 'Rows aggregated to gold hourly',
    gold_daily_rows  UInt64 DEFAULT 0 COMMENT 'Rows aggregated to gold daily',
    dim_users_new    UInt64 DEFAULT 0 COMMENT 'New user dimension records',
    dim_users_updated UInt64 DEFAULT 0 COMMENT 'Updated user dimension records (SCD2)',
    status           String          COMMENT 'Pipeline status: running / success / failed',
    started_at       DateTime DEFAULT now() COMMENT 'Pipeline start time',
    completed_at     Nullable(DateTime) COMMENT 'Pipeline completion time',
    _updated_at      DateTime DEFAULT now() COMMENT 'Last update timestamp'
)
ENGINE = ReplacingMergeTree(_updated_at)
ORDER BY (run_id)
COMMENT 'Catalog: pipeline execution log with row counts and timing'
