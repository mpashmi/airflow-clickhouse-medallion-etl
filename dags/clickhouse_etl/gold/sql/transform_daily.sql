-- Gold | transform_daily — Silver→gold: daily business KPI summary.
-- Params: {batch_id}, {silver_db}, {silver_table}
INSERT INTO rpt_daily_summary (
    event_date,
    total_events,
    unique_users,
    revenue_events,
    total_revenue,
    avg_value
)
SELECT
    event_date,
    count()                                     AS total_events,
    uniqExact(user_name)                        AS unique_users,
    sum(is_revenue_event)                       AS revenue_events,
    sum(if(is_revenue_event = 1, value, 0))     AS total_revenue,
    avg(value)                                  AS avg_value
FROM {silver_db}.{silver_table} FINAL
WHERE event_date IN (
    SELECT DISTINCT event_date
    FROM {silver_db}.{silver_table} FINAL
    WHERE batch_id = '{batch_id}'
)
GROUP BY
    event_date
