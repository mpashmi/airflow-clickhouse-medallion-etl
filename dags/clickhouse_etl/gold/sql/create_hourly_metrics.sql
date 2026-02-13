-- Gold | create_hourly_metrics — Hourly event aggregates by category and source.
CREATE TABLE IF NOT EXISTS agg_events_hourly (
    event_date     Date                    COMMENT 'Aggregation date',
    event_hour     UInt8                   COMMENT 'Aggregation hour (0-23)',
    category       LowCardinality(String)  COMMENT 'Event category',
    source         LowCardinality(String)  COMMENT 'Traffic source',
    event_count    UInt64                  COMMENT 'Number of events',
    revenue_events UInt64                  COMMENT 'Number of purchase events',
    total_value    Float64                 COMMENT 'Sum of monetary values',
    _updated_at    DateTime DEFAULT now()  COMMENT 'Last aggregation timestamp'
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_hour, category, source)
COMMENT 'Gold: hourly event aggregates — idempotent via ReplacingMergeTree'
