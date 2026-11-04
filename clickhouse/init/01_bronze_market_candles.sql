CREATE DATABASE IF NOT EXISTS medallion_warehouse;

CREATE TABLE IF NOT EXISTS medallion_warehouse.bronze_market_candles
(
    symbol String,
    `interval` String,
    open_time DateTime64(3, 'UTC'),
    close_time DateTime64(3, 'UTC'),
    open_price Float64,
    high_price Float64,
    low_price Float64,
    close_price Float64,
    volume Float64,
    number_of_trades UInt64,
    is_final UInt8,
    source String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, `interval`, open_time, source);