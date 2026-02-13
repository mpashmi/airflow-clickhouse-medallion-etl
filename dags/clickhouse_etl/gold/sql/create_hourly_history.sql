-- Gold | create_hourly_history — Audit trail: hourly aggregate snapshots (append-only).
CREATE TABLE IF NOT EXISTS agg_events_hourly_history (
    event_date     Date                    COMMENT 'Aggregation date',
    event_hour     UInt8                   COMMENT 'Aggregation hour (0-23)',
    category       LowCardinality(String)  COMMENT 'Event category',
    source         LowCardinality(String)  COMMENT 'Traffic source',
    event_count    UInt64                  COMMENT 'Number of events',
    revenue_events UInt64                  COMMENT 'Number of purchase events',
    total_value    Float64                 COMMENT 'Sum of monetary values',
    _updated_at    DateTime                COMMENT 'Original aggregation timestamp',
    _snapshot_ts   DateTime DEFAULT now()  COMMENT 'Audit: when this snapshot was taken',
    _snapshot_batch String                 COMMENT 'Batch that triggered this snapshot'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_hour, category, source, _snapshot_ts)
COMMENT 'Gold audit: snapshots of hourly aggregates before re-aggregation (append-only)'
