-- Catalog | create_table_registry — Central metadata store for all medallion tables.
CREATE TABLE IF NOT EXISTS table_registry (
    table_id         String          COMMENT 'Unique ID: database.table_name',
    database_name    String          COMMENT 'ClickHouse database',
    table_name       String          COMMENT 'ClickHouse table',
    layer            String          COMMENT 'Medallion layer: bronze / silver / gold',
    engine           String          COMMENT 'ClickHouse table engine',
    description      String          COMMENT 'Human-readable table purpose',
    owner            String          COMMENT 'Team or person responsible',
    partition_key    String          COMMENT 'Partition expression',
    order_key        String          COMMENT 'ORDER BY expression',
    tags             Array(String)   COMMENT 'Classification tags',
    created_at       DateTime DEFAULT now() COMMENT 'Registration timestamp',
    updated_at       DateTime DEFAULT now() COMMENT 'Last metadata update'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (table_id)
COMMENT 'Catalog: metadata registry for all medallion tables'
