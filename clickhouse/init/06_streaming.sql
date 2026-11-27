-- Streaming outputs of the Flink job. Late events are parked here and never loaded into bronze.
CREATE TABLE IF NOT EXISTS medallion_warehouse.late_events
(
    received_at DateTime64(3, 'UTC') DEFAULT now64(3),
    reason LowCardinality(String),
    symbol String,
    `interval` String,
    open_time DateTime64(3, 'UTC'),
    watermark_ms Int64,
    payload String
)
ENGINE = MergeTree
ORDER BY (received_at, symbol)
TTL toDateTime(received_at) + INTERVAL 90 DAY;

-- 5-minute rollups. A re-fired window inserts a new row with a higher version and replaces the old one on merge.
CREATE TABLE IF NOT EXISTS medallion_warehouse.candles_5m
(
    symbol String,
    `interval` String,
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    candle_count UInt8,
    emitted_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (symbol, `interval`, window_start);

-- Always read the newest version of each window, even before background merges have run.
CREATE VIEW IF NOT EXISTS medallion_warehouse.candles_5m_latest AS
SELECT
    symbol,
    `interval`,
    window_start,
    argMax(open_price, version) AS win_open,
    argMax(high_price, version) AS win_high,
    argMax(low_price, version) AS win_low,
    argMax(close_price, version) AS win_close,
    argMax(volume, version) AS win_volume,
    argMax(candle_count, version) AS win_candles,
    max(version) AS win_version
FROM medallion_warehouse.candles_5m
GROUP BY symbol, `interval`, window_start;