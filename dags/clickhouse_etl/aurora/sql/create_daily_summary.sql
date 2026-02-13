-- Aurora | create_daily_summary — Gold daily KPIs replicated from ClickHouse.
CREATE TABLE IF NOT EXISTS medallion_etl.rpt_daily_summary (
    event_date     DATE             NOT NULL,
    total_events   BIGINT           NOT NULL DEFAULT 0,
    unique_users   BIGINT           NOT NULL DEFAULT 0,
    revenue_events BIGINT           NOT NULL DEFAULT 0,
    total_revenue  DOUBLE PRECISION NOT NULL DEFAULT 0,
    avg_value      DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at     TIMESTAMP        NOT NULL DEFAULT now(),
    PRIMARY KEY (event_date)
);

COMMENT ON TABLE medallion_etl.rpt_daily_summary
    IS 'Gold replica: daily business KPIs from ClickHouse (incremental upsert)';
COMMENT ON COLUMN medallion_etl.rpt_daily_summary.event_date     IS 'Event date (primary key)';
COMMENT ON COLUMN medallion_etl.rpt_daily_summary.total_events   IS 'Total events for the day';
COMMENT ON COLUMN medallion_etl.rpt_daily_summary.unique_users   IS 'Distinct users active on this day';
COMMENT ON COLUMN medallion_etl.rpt_daily_summary.revenue_events IS 'Events flagged as revenue-generating';
COMMENT ON COLUMN medallion_etl.rpt_daily_summary.total_revenue  IS 'Sum of monetary value from revenue events';
COMMENT ON COLUMN medallion_etl.rpt_daily_summary.avg_value      IS 'Average value per event';
COMMENT ON COLUMN medallion_etl.rpt_daily_summary.updated_at     IS 'Last upsert timestamp';
