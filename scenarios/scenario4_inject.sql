-- Scenario 4 part A: 12 impossible candles (high below low) arriving through the live path.
-- They should all be quarantined, and the live reject rate should breach the 5 percent limit.
INSERT INTO medallion_warehouse.landing_market_candles
SELECT 'nifi', 'BTCUSDT', '1m',
       toInt64(toUnixTimestamp(toStartOfMinute(now()))) * 1000 - 60000 * toInt64(number + 10),
       toInt64(toUnixTimestamp(toStartOfMinute(now()))) * 1000 - 60000 * toInt64(number + 10) + 59999,
       '100', '90', '110', '100', '1.5', 10
FROM numbers(12);