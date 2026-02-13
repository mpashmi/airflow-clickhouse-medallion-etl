-- Silver | create_cleaned_events — Cleaned, deduplicated event facts (ReplacingMergeTree).
CREATE TABLE IF NOT EXISTS fact_events (
    event_id         String                  COMMENT 'Unique event identifier (dedup key)',
    user_name        String                  COMMENT 'Cleaned user name (lowercased, trimmed)',
    category         LowCardinality(String)  COMMENT 'Standardised event category',
    source           LowCardinality(String)  COMMENT 'Standardised originating channel',
    value            Float64                 COMMENT 'Validated monetary value (>= 0)',
    event_time       DateTime                COMMENT 'Event timestamp',
    event_date       Date                    COMMENT 'Derived: date part of event_time',
    event_hour       UInt8                   COMMENT 'Derived: hour part of event_time',
    is_revenue_event UInt8                   COMMENT 'Derived: 1 if purchase with value > 0',
    batch_id         String                  COMMENT 'Lineage: source batch identifier',
    _loaded_at       DateTime DEFAULT now()  COMMENT 'Silver load timestamp'
)
ENGINE = ReplacingMergeTree(_loaded_at)
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (event_id)
COMMENT 'Silver: cleaned & deduplicated event facts'
