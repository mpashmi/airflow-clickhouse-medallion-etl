-- Silver | update_dim_users — SCD Type 2: close changed records, upsert current.
-- Params: {batch_id}, {silver_db}, {dim_table}, {fact_table}

-- ── Step 1: Close changed records ────────────────────────────────────────────
INSERT INTO {silver_db}.{dim_table} (
    user_key, user_name, first_seen_date, last_seen_date,
    primary_category, primary_source,
    total_events, total_value, is_revenue_user,
    valid_from, valid_to, is_current
)
SELECT
    d.user_key,
    d.user_name,
    d.first_seen_date,
    d.last_seen_date,
    d.primary_category,
    d.primary_source,
    d.total_events,
    d.total_value,
    d.is_revenue_user,
    d.valid_from,
    now()           AS valid_to,
    0               AS is_current
FROM (
    SELECT user_key, user_name, first_seen_date, last_seen_date,
           primary_category, primary_source,
           total_events, total_value, is_revenue_user, valid_from
    FROM {silver_db}.{dim_table} FINAL
    WHERE is_current = 1
) AS d
INNER JOIN (
    SELECT
        user_name,
        topK(1)(category)[1]  AS new_primary_category,
        topK(1)(source)[1]    AS new_primary_source
    FROM {silver_db}.{fact_table}
    WHERE batch_id = '{batch_id}'
    GROUP BY user_name
) AS incoming ON d.user_name = incoming.user_name
WHERE d.primary_category != incoming.new_primary_category
   OR d.primary_source != incoming.new_primary_source;

-- ── Step 2: Upsert current records ──────────────────────────────────────────
INSERT INTO {silver_db}.{dim_table} (
    user_key, user_name, first_seen_date, last_seen_date,
    primary_category, primary_source,
    total_events, total_value, is_revenue_user,
    valid_from, valid_to, is_current
)
SELECT
    concat(incoming.user_name, '_', toString(now())) AS user_key,
    incoming.user_name,
    if(existing.user_name != '', existing.first_seen_date, incoming.min_date) AS first_seen_date,
    incoming.max_date                                                          AS last_seen_date,
    incoming.primary_category,
    incoming.primary_source,
    if(existing.user_name != '', existing.total_events, 0) + incoming.batch_events AS total_events,
    if(existing.user_name != '', existing.total_value, 0)  + incoming.batch_value  AS total_value,
    if(existing.user_name != '',
       if(existing.is_revenue_user = 1 OR incoming.has_revenue = 1, 1, 0),
       incoming.has_revenue)                                                        AS is_revenue_user,
    if(existing.user_name != '',
       if(existing.primary_category != incoming.primary_category
          OR existing.primary_source != incoming.primary_source,
          now(), existing.valid_from),
       now())                                                                       AS valid_from,
    toDateTime('9999-12-31 00:00:00')                                               AS valid_to,
    1                                                                               AS is_current
FROM (
    SELECT
        user_name,
        min(event_date)                AS min_date,
        max(event_date)                AS max_date,
        topK(1)(category)[1]           AS primary_category,
        topK(1)(source)[1]             AS primary_source,
        count()                        AS batch_events,
        sum(value)                     AS batch_value,
        max(is_revenue_event)          AS has_revenue
    FROM {silver_db}.{fact_table}
    WHERE batch_id = '{batch_id}'
    GROUP BY user_name
) AS incoming
LEFT JOIN (
    SELECT user_name, first_seen_date, primary_category, primary_source,
           total_events, total_value, is_revenue_user, valid_from
    FROM {silver_db}.{dim_table} FINAL
    WHERE is_current = 1
) AS existing ON incoming.user_name = existing.user_name
