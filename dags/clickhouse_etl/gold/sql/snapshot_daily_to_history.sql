-- Gold | snapshot_daily_to_history — Audit snapshot of daily summary.
-- Params: {batch_id}, {gold_db}, {gold_table}, {history_table}, {silver_db}, {silver_table}
INSERT INTO {gold_db}.{history_table} (
    event_date, total_events, unique_users,
    revenue_events, total_revenue, avg_value,
    _updated_at, _snapshot_batch
)
SELECT
    event_date, total_events, unique_users,
    revenue_events, total_revenue, avg_value,
    _updated_at, '{batch_id}' AS _snapshot_batch
FROM {gold_db}.{gold_table} FINAL
WHERE event_date IN (
    SELECT DISTINCT event_date
    FROM {silver_db}.{silver_table} FINAL
    WHERE batch_id = '{batch_id}'
)
