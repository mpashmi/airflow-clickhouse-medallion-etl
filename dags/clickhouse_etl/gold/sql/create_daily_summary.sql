-- Gold | create_daily_summary — Daily business KPI report.
CREATE TABLE IF NOT EXISTS rpt_daily_summary (
    event_date     Date    COMMENT 'Report date',
    total_events   UInt64  COMMENT 'Total events for the day',
    unique_users   UInt64  COMMENT 'Distinct users for the day',
    revenue_events UInt64  COMMENT 'Number of revenue-generating events',
    total_revenue  Float64 COMMENT 'Total monetary value of revenue events',
    avg_value      Float64 COMMENT 'Average event value across all events',
    _updated_at    DateTime DEFAULT now() COMMENT 'Last aggregation timestamp'
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date)
COMMENT 'Gold: daily business summary — idempotent via ReplacingMergeTree'
