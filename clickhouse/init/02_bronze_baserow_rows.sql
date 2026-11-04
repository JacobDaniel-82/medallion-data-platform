CREATE TABLE IF NOT EXISTS medallion_warehouse.bronze_baserow_rows
(
    table_name LowCardinality(String),
    row_id UInt64,
    payload String,
    extracted_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (table_name, row_id);