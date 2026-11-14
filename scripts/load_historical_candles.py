import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import clickhouse_connect
import yaml

SYMBOL = "BTCUSDT"
INTERVAL = "1m"
BATCH_DAYS = 30
BATCH_SIZE = 1000

BINANCE_URL = "https://api.binance.com/api/v3/klines"
CONTRACT_PATH = Path(os.getenv("CONTRACTS_DIR", "/opt/airflow/contracts")) / "binance_klines_batch.yaml"

LANDING_COLUMNS = [
    "source", "symbol", "interval", "open_time_ms", "close_time_ms",
    "open_price_raw", "high_price_raw", "low_price_raw", "close_price_raw",
    "volume_raw", "number_of_trades",
]


def load_contract_limits():
    with open(CONTRACT_PATH, encoding="utf-8") as f:
        contract = yaml.safe_load(f)
    sla = contract["sla"]
    return sla["volume"]["min_rows_per_load"], sla["quarantine"]["max_reject_rate"]


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
    min_rows, max_reject_rate = load_contract_limits()

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
    # Stop 1 ms before the current minute so the still-forming candle is never requested.
    end_ms = int(end.timestamp() * 1000) - 1

    rows = []
    cursor = start_ms

    print(f"Fetching {SYMBOL} {INTERVAL} candles from {start} to {end}")

    while cursor < end_ms:
        candles = fetch_klines(cursor, end_ms)

        if not candles:
            break

        for candle in candles:
            # Land the values exactly as Binance sent them. The contract rules check them.
            rows.append((
                "batch", SYMBOL, INTERVAL,
                int(candle[0]), int(candle[6]),
                str(candle[1]), str(candle[2]), str(candle[3]), str(candle[4]), str(candle[5]),
                int(candle[8]),
            ))

        cursor = candles[-1][0] + 60_000

        if len(candles) < BATCH_SIZE:
            break

        time.sleep(0.2)

    if not rows:
        raise RuntimeError("No candles returned by Binance.")

    # Use the database clock so the new rows can be told apart from the old batch rows.
    load_start = client.query("SELECT now64(3)").result_rows[0][0]

    # The old batch rows stay in place until the new load has passed the contract.
    client.insert("landing_market_candles", rows, column_names=LANDING_COLUMNS)

    quarantined = client.query(
        "SELECT count() FROM quarantine_market_candles "
        "WHERE source = 'batch' AND quarantined_at >= {ts:DateTime64(3)}",
        parameters={"ts": load_start},
    ).result_rows[0][0]

    loaded = len(rows) - quarantined
    reject_rate = quarantined / len(rows)
    print(f"Fetched {len(rows)}, accepted {loaded}, quarantined {quarantined} (reject rate {reject_rate:.4%}).")

    problems = []
    if reject_rate > max_reject_rate:
        problems.append(f"reject rate {reject_rate:.4%} is above the contract limit {max_reject_rate:.2%}")
    if loaded < min_rows:
        problems.append(f"only {loaded} valid rows, the contract requires at least {min_rows}")

    if problems:
        # Remove what this run loaded and keep the previous batch data untouched.
        client.command(
            "ALTER TABLE bronze_market_candles DELETE "
            "WHERE source = 'batch' AND ingested_at >= {ts:DateTime64(3)} SETTINGS mutations_sync = 1",
            parameters={"ts": load_start},
        )
        raise RuntimeError("Contract breach, load rolled back: " + "; ".join(problems))

    client.command(
        "ALTER TABLE bronze_market_candles DELETE "
        "WHERE source = 'batch' AND ingested_at < {ts:DateTime64(3)} SETTINGS mutations_sync = 1",
        parameters={"ts": load_start},
    )

    print(f"Loaded {loaded} candles into ClickHouse Bronze. Previous batch rows replaced.")


if __name__ == "__main__":
    main()