-- Gold | create_daily_history — Audit trail: daily summary snapshots (append-only).
CREATE TABLE IF NOT EXISTS rpt_daily_summary_history (
    event_date     Date    COMMENT 'Report date',
    total_events   UInt64  COMMENT 'Total events for the day',
    unique_users   UInt64  COMMENT 'Distinct users for the day',
    revenue_events UInt64  COMMENT 'Number of revenue-generating events',
    total_revenue  Float64 COMMENT 'Total monetary value of revenue events',
    avg_value      Float64 COMMENT 'Average event value',
    _updated_at    DateTime                COMMENT 'Original aggregation timestamp',
    _snapshot_ts   DateTime DEFAULT now()  COMMENT 'Audit: when this snapshot was taken',
    _snapshot_batch String                 COMMENT 'Batch that triggered this snapshot'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, _snapshot_ts)
COMMENT 'Gold audit: snapshots of daily summary before re-aggregation (append-only)'
