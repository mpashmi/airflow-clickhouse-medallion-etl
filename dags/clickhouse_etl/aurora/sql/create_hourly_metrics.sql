-- Aurora | create_hourly_metrics — Gold hourly aggregates replicated from ClickHouse.
CREATE TABLE IF NOT EXISTS medallion_etl.agg_events_hourly (
    event_date     DATE             NOT NULL,
    event_hour     SMALLINT         NOT NULL,
    category       VARCHAR(50)      NOT NULL,
    source         VARCHAR(50)      NOT NULL,
    event_count    BIGINT           NOT NULL DEFAULT 0,
    revenue_events BIGINT           NOT NULL DEFAULT 0,
    total_value    DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at     TIMESTAMP        NOT NULL DEFAULT now(),
    PRIMARY KEY (event_date, event_hour, category, source)
);

COMMENT ON TABLE medallion_etl.agg_events_hourly
    IS 'Gold replica: hourly event aggregates from ClickHouse (incremental upsert)';
COMMENT ON COLUMN medallion_etl.agg_events_hourly.event_date     IS 'Event date (partition key)';
COMMENT ON COLUMN medallion_etl.agg_events_hourly.event_hour     IS 'Hour of day (0-23)';
COMMENT ON COLUMN medallion_etl.agg_events_hourly.category       IS 'Event category';
COMMENT ON COLUMN medallion_etl.agg_events_hourly.source         IS 'Traffic source';
COMMENT ON COLUMN medallion_etl.agg_events_hourly.event_count    IS 'Total events in this hour/category/source';
COMMENT ON COLUMN medallion_etl.agg_events_hourly.revenue_events IS 'Events flagged as revenue-generating';
COMMENT ON COLUMN medallion_etl.agg_events_hourly.total_value    IS 'Sum of monetary value';
COMMENT ON COLUMN medallion_etl.agg_events_hourly.updated_at     IS 'Last upsert timestamp';
