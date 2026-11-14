import urllib.parse

QUERY = """INSERT INTO medallion_warehouse.landing_market_candles
(source, symbol, `interval`, open_time_ms, close_time_ms, open_price_raw, high_price_raw, low_price_raw, close_price_raw, volume_raw, number_of_trades)
SELECT 'nifi', 'BTCUSDT', '1m', open_time, close_time, o, h, l, c, v, trades
FROM input('open_time Int64, o String, h String, l String, c String, v String, close_time Int64, qv String, trades Int64, tb String, tq String, ig String')
FORMAT JSONCompactEachRow"""

# NiFi posts one JSON array per candle. The query itself goes in the URL.
print("http://crypto-clickhouse:8123/?query=" + urllib.parse.quote(" ".join(QUERY.split())))