import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import clickhouse_connect

SYMBOL = "BTCUSDT"
INTERVAL = "1m"
BATCH_DAYS = 30
BATCH_SIZE = 1000

BINANCE_URL = "https://api.binance.com/api/v3/klines"


def fetch_klines(start_ms, end_ms):
    params = urllib.parse.urlencode({
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": BATCH_SIZE,
    })

    request = urllib.request.Request(
        f"{BINANCE_URL}?{params}",
        headers={"User-Agent": "crypto-medallion-pipeline/1.0"},
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def main():
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="medallion_warehouse",
    )

    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = end - timedelta(days=BATCH_DAYS)

    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    rows = []
    cursor = start_ms

    print(f"Fetching {SYMBOL} {INTERVAL} candles from {start} to {end}")

    while cursor < end_ms:
        candles = fetch_klines(cursor, end_ms)

        if not candles:
            break

        for candle in candles:
            rows.append((
                SYMBOL,
                INTERVAL,
                datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc),
                datetime.fromtimestamp(candle[6] / 1000, tz=timezone.utc),
                float(candle[1]),
                float(candle[2]),
                float(candle[3]),
                float(candle[4]),
                float(candle[5]),
                int(candle[8]),
                1,
                "batch",
            ))

        cursor = candles[-1][0] + 60_000

        if len(candles) < BATCH_SIZE:
            break

        time.sleep(0.2)

    if not rows:
        raise RuntimeError("No candles returned by Binance.")


    client.command(
        """
        ALTER TABLE bronze_market_candles
        DELETE WHERE symbol = {symbol:String}
          AND interval = {interval:String}
          AND source = 'batch'
        SETTINGS mutations_sync = 1
        """,
        parameters={
            "symbol": SYMBOL,
            "interval": INTERVAL,
        },
    )

    client.insert(
        "bronze_market_candles",
        rows,
        column_names=[
            "symbol", "interval", "open_time", "close_time",
            "open_price", "high_price", "low_price", "close_price",
            "volume", "number_of_trades", "is_final", "source",
        ],
    )

    print(f"Loaded {len(rows)} candle records into ClickHouse Bronze.")


if __name__ == "__main__":
    main()
