-- Catalog | create_column_registry — Column-level metadata for all registered tables.
CREATE TABLE IF NOT EXISTS column_registry (
    column_id        String          COMMENT 'Unique ID: database.table.column',
    table_id         String          COMMENT 'FK to table_registry.table_id',
    column_name      String          COMMENT 'Column name',
    data_type        String          COMMENT 'ClickHouse data type',
    is_nullable      UInt8           COMMENT '1 if Nullable, 0 otherwise',
    is_partition_key UInt8           COMMENT '1 if part of partition key',
    is_order_key     UInt8           COMMENT '1 if part of ORDER BY key',
    description      String          COMMENT 'Human-readable column purpose',
    tags             Array(String)   COMMENT 'Classification tags (e.g. PII, metric)',
    updated_at       DateTime DEFAULT now() COMMENT 'Last metadata update'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (column_id)
COMMENT 'Catalog: column-level metadata for all tables'
