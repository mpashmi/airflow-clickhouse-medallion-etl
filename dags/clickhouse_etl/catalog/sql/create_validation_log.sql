-- Catalog | create_validation_log — Cross-target delivery validation (ClickHouse gold vs Aurora).
CREATE TABLE IF NOT EXISTS validation_log (
    run_id              String          COMMENT 'Airflow run_id',
    event_date          Date            COMMENT 'Date being validated',
    event_hour          UInt8           COMMENT 'Hour being validated',
    ch_hourly_rows      UInt64          COMMENT 'ClickHouse: agg_events_hourly row count for this hour',
    ch_hourly_events    UInt64          COMMENT 'ClickHouse: sum(event_count) for this hour',
    ch_hourly_value     Float64         COMMENT 'ClickHouse: sum(total_value) for this hour',
    ch_daily_rows       UInt64          COMMENT 'ClickHouse: rpt_daily_summary row count for this date',
    ch_daily_events     UInt64          COMMENT 'ClickHouse: total_events for this date',
    aurora_hourly_rows  UInt64          COMMENT 'Aurora: agg_events_hourly row count for this hour',
    aurora_hourly_events UInt64         COMMENT 'Aurora: sum(event_count) for this hour',
    aurora_hourly_value Float64         COMMENT 'Aurora: sum(total_value) for this hour',
    aurora_daily_rows   UInt64          COMMENT 'Aurora: rpt_daily_summary row count for this date',
    aurora_daily_events UInt64          COMMENT 'Aurora: total_events for this date',
    hourly_rows_match   UInt8           COMMENT '1 if hourly row counts match',
    hourly_events_match UInt8           COMMENT '1 if hourly event count totals match',
    hourly_value_match  UInt8           COMMENT '1 if hourly total_value match (0.01 tolerance)',
    daily_rows_match    UInt8           COMMENT '1 if daily row counts match',
    daily_events_match  UInt8           COMMENT '1 if daily event counts match',
    overall_status      String          COMMENT 'pass or fail',
    validated_at        DateTime DEFAULT now() COMMENT 'Validation timestamp'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (run_id, event_date, event_hour)
COMMENT 'Catalog: cross-target delivery validation (ClickHouse gold vs Aurora)'
