-- Silver | create_quarantine_events — Rows rejected during bronze→silver transformation.
CREATE TABLE IF NOT EXISTS quarantine_events (
    event_id         String                  COMMENT 'Raw event_id (may be empty — that is a rejection reason)',
    user_name        String                  COMMENT 'Raw user_name (may be empty — that is a rejection reason)',
    category         String                  COMMENT 'Raw event category',
    source           String                  COMMENT 'Raw originating channel',
    value            Float64                 COMMENT 'Raw monetary value',
    event_time       DateTime                COMMENT 'Event timestamp from source',
    batch_id         String                  COMMENT 'Source batch identifier for lineage',
    rejection_reason String                  COMMENT 'Why this row was rejected (e.g. empty_event_id, empty_user_name)',
    _quarantined_at  DateTime DEFAULT now()  COMMENT 'When the row was quarantined'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (batch_id, event_id, user_name)
COMMENT 'Silver: rows rejected during bronze-to-silver transformation'
