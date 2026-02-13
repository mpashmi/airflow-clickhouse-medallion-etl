-- Bronze | create_raw_events — Raw events as received from source (append-only).
CREATE TABLE IF NOT EXISTS stg_events (
    event_id     String          COMMENT 'Source event identifier',
    user_name    String          COMMENT 'Raw user name from source',
    category     String          COMMENT 'Raw event category',
    source       String          COMMENT 'Raw originating channel',
    value        Float64         COMMENT 'Raw monetary value',
    event_time   DateTime        COMMENT 'Event timestamp from source',
    batch_id     String          COMMENT 'Airflow run_id for lineage',
    _ingested_at DateTime DEFAULT now() COMMENT 'Bronze ingest timestamp'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (batch_id, event_id)
COMMENT 'Bronze: raw staging events — append-only, no transformations'
