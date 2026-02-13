-- Silver | quarantine — Capture rejected rows from bronze→silver transformation.
-- Params: {batch_id}, {bronze_db}, {bronze_table}
INSERT INTO quarantine_events (
    event_id,
    user_name,
    category,
    source,
    value,
    event_time,
    batch_id,
    rejection_reason
)
SELECT
    event_id,
    user_name,
    category,
    source,
    value,
    event_time,
    batch_id,
    multiIf(
        event_id  = '' AND user_name = '', 'empty_event_id,empty_user_name',
        event_id  = '',                    'empty_event_id',
        user_name = '',                    'empty_user_name',
        'unknown'
    ) AS rejection_reason
FROM {bronze_db}.{bronze_table}
WHERE batch_id = '{batch_id}'
  AND (event_id = '' OR user_name = '')
