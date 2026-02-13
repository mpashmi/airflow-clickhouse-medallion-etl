-- Catalog | create_data_lineage — Tracks data flow between tables across medallion layers.
CREATE TABLE IF NOT EXISTS data_lineage (
    lineage_id       String          COMMENT 'Unique ID: source_table_id -> target_table_id',
    source_table_id  String          COMMENT 'FK to table_registry: upstream table',
    target_table_id  String          COMMENT 'FK to table_registry: downstream table',
    transformation   String          COMMENT 'Description of the transformation applied',
    sql_file         String          COMMENT 'Path to the SQL file that performs the transform',
    layer_from       String          COMMENT 'Source medallion layer',
    layer_to         String          COMMENT 'Target medallion layer',
    updated_at       DateTime DEFAULT now() COMMENT 'Last metadata update'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (lineage_id)
COMMENT 'Catalog: data lineage tracking across medallion layers'
