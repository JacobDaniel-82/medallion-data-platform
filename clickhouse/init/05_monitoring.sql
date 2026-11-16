-- Monitoring: one row per check per run, plus a view with the latest status of each check.
CREATE TABLE IF NOT EXISTS medallion_warehouse.pipeline_check_results
(
    checked_at DateTime64(3, 'UTC') DEFAULT now64(3),
    run_id String,
    domain LowCardinality(String),
    layer LowCardinality(String),
    check_name String,
    check_type LowCardinality(String),
    status LowCardinality(String),
    value Float64,
    warn_threshold Nullable(Float64),
    error_threshold Nullable(Float64),
    message String
)
ENGINE = MergeTree
ORDER BY (checked_at, domain, check_name)
TTL toDateTime(checked_at) + INTERVAL 30 DAY;

CREATE VIEW IF NOT EXISTS medallion_warehouse.pipeline_health_latest AS
SELECT
    domain,
    layer,
    check_name,
    check_type,
    argMax(status, checked_at) AS status,
    argMax(value, checked_at) AS value,
    argMax(message, checked_at) AS message,
    max(checked_at) AS last_checked_at
FROM medallion_warehouse.pipeline_check_results
GROUP BY domain, layer, check_name, check_type;