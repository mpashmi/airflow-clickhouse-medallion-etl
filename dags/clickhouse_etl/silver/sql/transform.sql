-- Silver | transform — Bronze→silver: clean, validate, derive fields.
-- Params: {batch_id}, {bronze_db}, {bronze_table}
INSERT INTO fact_events (
    event_id,
    user_name,
    category,
    source,
    value,
    event_time,
    event_date,
    event_hour,
    is_revenue_event,
    batch_id
)
SELECT
    event_id,
    lower(trim(user_name))                        AS user_name,
    lower(trim(category))                          AS category,
    lower(trim(source))                            AS source,
    if(value < 0, 0, value)                        AS value,
    event_time,
    toDate(event_time)                             AS event_date,
    toHour(event_time)                             AS event_hour,
    if(category = 'purchase' AND value > 0, 1, 0)  AS is_revenue_event,
    batch_id
FROM {bronze_db}.{bronze_table}
WHERE batch_id  = '{batch_id}'
  AND event_id  != ''
  AND user_name  != ''
