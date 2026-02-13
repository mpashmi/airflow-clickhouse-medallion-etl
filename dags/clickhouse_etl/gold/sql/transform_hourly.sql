-- Gold | transform_hourly — Silver→gold: hourly aggregates by category/source.
-- Params: {batch_id}, {silver_db}, {silver_table}
INSERT INTO agg_events_hourly (
    event_date,
    event_hour,
    category,
    source,
    event_count,
    revenue_events,
    total_value
)
SELECT
    event_date,
    event_hour,
    category,
    source,
    count()                AS event_count,
    sum(is_revenue_event)  AS revenue_events,
    sum(value)             AS total_value
FROM {silver_db}.{silver_table} FINAL
WHERE (event_date, event_hour) IN (
    SELECT DISTINCT event_date, event_hour
    FROM {silver_db}.{silver_table} FINAL
    WHERE batch_id = '{batch_id}'
)
GROUP BY
    event_date,
    event_hour,
    category,
    source
