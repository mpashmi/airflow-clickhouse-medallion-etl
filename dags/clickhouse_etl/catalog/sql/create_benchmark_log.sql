-- Catalog | create_benchmark_log — Per-task performance metrics (append-only).
CREATE TABLE IF NOT EXISTS benchmark_log
(
    run_id              String          COMMENT 'Pipeline run identifier',
    framework           String          COMMENT 'Orchestrator name (e.g. airflow, dagster, prefect)',
    framework_version   String          COMMENT 'e.g. 2.7.2',
    task_name           String          COMMENT 'Task identifier (same across frameworks)',
    layer               String          COMMENT 'Medallion layer: setup | bronze | silver | gold | export | validation | catalog',
    status              String          COMMENT 'success | failed',

    -- Timing
    started_at          DateTime        COMMENT 'Task start timestamp (UTC)',
    ended_at            DateTime        COMMENT 'Task end timestamp (UTC)',
    duration_ms         UInt64          COMMENT 'Wall-clock duration in milliseconds',

    -- Throughput
    rows_in             UInt64          COMMENT 'Rows read / received (0 if N/A)',
    rows_out            UInt64          COMMENT 'Rows written / produced (0 if N/A)',
    rows_per_second     Float64         COMMENT 'rows_out / duration_seconds (0 if duration=0)',

    -- Resource usage
    peak_memory_mb      Float64         COMMENT 'Peak RSS of the worker process in MB',

    -- Context
    event_date          Date            COMMENT 'Pipeline execution date',
    event_hour          UInt8           COMMENT 'Pipeline execution hour (0-23)',
    batch_id            String          COMMENT 'Batch identifier for lineage',
    extra               String          COMMENT 'JSON blob for framework-specific metadata',

    _recorded_at        DateTime        DEFAULT now() COMMENT 'When this row was inserted'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (framework, run_id, task_name)
COMMENT 'Benchmark log: per-task performance metrics for cross-framework comparison'
