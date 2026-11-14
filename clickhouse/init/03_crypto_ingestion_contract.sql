-- Ingestion contract enforcement for the crypto candles (contracts/binance_klines_*.yaml).
-- Landing table: raw values as received. The Null engine stores nothing and only fires the views below.
CREATE TABLE IF NOT EXISTS medallion_warehouse.landing_market_candles
(
    source LowCardinality(String),
    symbol String,
    `interval` String,
    open_time_ms Int64,
    close_time_ms Int64,
    open_price_raw String,
    high_price_raw String,
    low_price_raw String,
    close_price_raw String,
    volume_raw String,
    number_of_trades Int64
)
ENGINE = Null;

-- Checked table: the same rows plus the list of rules each row failed. Also Null, it only fires the routing views.
CREATE TABLE IF NOT EXISTS medallion_warehouse.checked_market_candles
(
    source LowCardinality(String),
    symbol String,
    `interval` String,
    open_time_ms Int64,
    close_time_ms Int64,
    open_price_raw String,
    high_price_raw String,
    low_price_raw String,
    close_price_raw String,
    volume_raw String,
    number_of_trades Int64,
    failed_rules Array(String)
)
ENGINE = Null;

-- Quarantine: rejected records with the reason, kept for 90 days.
CREATE TABLE IF NOT EXISTS medallion_warehouse.quarantine_market_candles
(
    quarantined_at DateTime64(3, 'UTC') DEFAULT now64(3),
    contract LowCardinality(String),
    source LowCardinality(String),
    symbol String,
    `interval` String,
    open_time_ms Int64,
    close_time_ms Int64,
    open_price_raw String,
    high_price_raw String,
    low_price_raw String,
    close_price_raw String,
    volume_raw String,
    number_of_trades Int64,
    failed_rules Array(String)
)
ENGINE = MergeTree
ORDER BY (quarantined_at, source)
TTL toDateTime(quarantined_at) + INTERVAL 90 DAY;

-- The rules. Each failed rule adds its name to failed_rules. The names match record_rules and fields in the contracts.
CREATE MATERIALIZED VIEW IF NOT EXISTS medallion_warehouse.mv_candles_check
TO medallion_warehouse.checked_market_candles AS
SELECT
    source,
    symbol,
    `interval`,
    open_time_ms,
    close_time_ms,
    open_price_raw,
    high_price_raw,
    low_price_raw,
    close_price_raw,
    volume_raw,
    number_of_trades,
    arrayFilter(x -> x != '', [
        if(open_time_ms > 0 AND modulo(open_time_ms, 60000) = 0, '', 'open_time_valid'),
        if(o IS NOT NULL AND isFinite(o) AND o > 0, '', 'open_price_positive'),
        if(h IS NOT NULL AND isFinite(h) AND h > 0, '', 'high_price_positive'),
        if(l IS NOT NULL AND isFinite(l) AND l > 0, '', 'low_price_positive'),
        if(c IS NOT NULL AND isFinite(c) AND c > 0, '', 'close_price_positive'),
        if(v IS NOT NULL AND isFinite(v) AND v >= 0, '', 'volume_non_negative'),
        if(number_of_trades >= 0, '', 'trades_non_negative'),
        if(o IS NOT NULL AND h IS NOT NULL AND l IS NOT NULL AND h >= l, '', 'high_not_below_low'),
        if(o IS NOT NULL AND h IS NOT NULL AND c IS NOT NULL AND h >= greatest(o, c), '', 'high_covers_open_close'),
        if(o IS NOT NULL AND l IS NOT NULL AND c IS NOT NULL AND l <= least(o, c), '', 'low_covers_open_close'),
        if(close_time_ms - open_time_ms = 59999, '', 'one_minute_window'),
        if(close_time_ms < toUnixTimestamp64Milli(now64(3)), '', 'candle_is_closed')
    ]) AS failed_rules
FROM
(
    SELECT
        *,
        toFloat64OrNull(open_price_raw) AS o,
        toFloat64OrNull(high_price_raw) AS h,
        toFloat64OrNull(low_price_raw) AS l,
        toFloat64OrNull(close_price_raw) AS c,
        toFloat64OrNull(volume_raw) AS v
    FROM medallion_warehouse.landing_market_candles
);

-- Route 1: records that passed every rule go to bronze.
CREATE MATERIALIZED VIEW IF NOT EXISTS medallion_warehouse.mv_candles_to_bronze
TO medallion_warehouse.bronze_market_candles AS
SELECT
    symbol,
    `interval`,
    fromUnixTimestamp64Milli(open_time_ms, 'UTC') AS open_time,
    fromUnixTimestamp64Milli(close_time_ms, 'UTC') AS close_time,
    toFloat64OrZero(open_price_raw) AS open_price,
    toFloat64OrZero(high_price_raw) AS high_price,
    toFloat64OrZero(low_price_raw) AS low_price,
    toFloat64OrZero(close_price_raw) AS close_price,
    toFloat64OrZero(volume_raw) AS volume,
    toUInt64(number_of_trades) AS number_of_trades,
    toUInt8(1) AS is_final,
    source
FROM medallion_warehouse.checked_market_candles
WHERE empty(failed_rules);

-- Route 2: records that failed any rule go to quarantine with the reasons.
CREATE MATERIALIZED VIEW IF NOT EXISTS medallion_warehouse.mv_candles_to_quarantine
TO medallion_warehouse.quarantine_market_candles AS
SELECT
    if(source = 'batch', 'binance_klines_batch', 'binance_klines_live') AS contract,
    source,
    symbol,
    `interval`,
    open_time_ms,
    close_time_ms,
    open_price_raw,
    high_price_raw,
    low_price_raw,
    close_price_raw,
    volume_raw,
    number_of_trades,
    failed_rules
FROM medallion_warehouse.checked_market_candles
WHERE notEmpty(failed_rules);