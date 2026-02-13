-- Silver | create_dim_users — SCD Type 2 user dimension with full change history.
CREATE TABLE IF NOT EXISTS dim_users (
    user_key          String          COMMENT 'Surrogate key: user_name + valid_from hash',
    user_name         String          COMMENT 'Natural key: cleaned user name',
    first_seen_date   Date            COMMENT 'Date user was first observed',
    last_seen_date    Date            COMMENT 'Date user was last observed',
    primary_category  String          COMMENT 'Most frequent event category',
    primary_source    String          COMMENT 'Most frequent traffic source',
    total_events      UInt64          COMMENT 'Cumulative event count',
    total_value       Float64         COMMENT 'Cumulative monetary value',
    is_revenue_user   UInt8           COMMENT '1 if user has any purchase events',
    valid_from        DateTime        COMMENT 'SCD2: version start timestamp',
    valid_to          DateTime        COMMENT 'SCD2: version end (9999-12-31 = current)',
    is_current        UInt8           COMMENT 'SCD2: 1 = active version, 0 = historical',
    _updated_at       DateTime DEFAULT now() COMMENT 'Last update timestamp'
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(valid_from)
ORDER BY (user_name, valid_from)
COMMENT 'Silver: SCD Type 2 user dimension — full change history'
