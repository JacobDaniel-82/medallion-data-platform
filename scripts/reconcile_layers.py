import os
import subprocess
import sys

import clickhouse_connect

TOPIC = "binance.klines.1m"
DLQ_TOPIC = "binance.klines.1m.dlq"
KB = "/opt/kafka/bin"


def kafka(*args):
    result = subprocess.run(
        ["docker", "exec", "crypto-kafka", *args],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=180,
    )
    return result.stdout


def offsets(topic):
    def one(flag):
        out = kafka(f"{KB}/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092",
                    "--topic", topic, "--time", str(flag)).strip()
        return int(out.split(":")[-1]) if out else 0
    return one(-2), one(-1)


def topic_keys(topic):
    start, end = offsets(topic)
    count = end - start
    if count <= 0:
        return 0, []
    out = kafka(f"{KB}/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092",
                "--topic", topic, "--from-beginning", "--max-messages", str(count),
                "--property", "print.key=true", "--timeout-ms", "30000")
    return count, [line.split("\t", 1)[0] for line in out.splitlines() if line.strip()]


def main():
    ch = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="medallion_warehouse",
    )

    def rows(sql):
        return ch.query(sql).result_rows

    def key(s, i, ms):
        return f"{s}|{i}|{int(ms)}"

    total, keys = topic_keys(TOPIC)
    keyed = [k for k in keys if k != "null"]
    kafka_keys = set(keyed)
    dlq_start, dlq_end = offsets(DLQ_TOPIC)

    bronze = [key(*r) for r in rows(
        "SELECT symbol, `interval`, toUnixTimestamp64Milli(open_time) FROM bronze_market_candles WHERE source = 'flink'")]
    quarantined = {key(*r) for r in rows(
        "SELECT symbol, `interval`, open_time_ms FROM quarantine_market_candles WHERE source = 'flink'")}
    late = {key(*r) for r in rows(
        "SELECT symbol, `interval`, toUnixTimestamp64Milli(open_time) FROM late_events")}
    silver = [key(*r) for r in rows(
        "SELECT symbol, `interval`, toUnixTimestamp64Milli(open_time) FROM silver_market_candles")]
    windows, window_candles = rows("SELECT count(), sum(win_candles) FROM candles_5m_latest")[0]

    bronze_set, silver_set = set(bronze), set(silver)
    missing = kafka_keys - (bronze_set | quarantined | late)
    unexpected = bronze_set - kafka_keys
    silver_missing = bronze_set - silver_set

    print(f"{'LAYER':<30}{'EVENTS':>8}{'DISTINCT':>10}{'EXTRA COPIES':>14}{'MISSING':>9}")
    print(f"{'Kafka topic (all messages)':<30}{total:>8}{len(kafka_keys):>10}{len(keyed) - len(kafka_keys):>14}{'-':>9}")
    print(f"{'  of which unparseable':<30}{total - len(keyed):>8}")
    print(f"{'Dead-letter topic':<30}{dlq_end - dlq_start:>8}")
    print(f"{'Bronze (source=flink)':<30}{len(bronze):>8}{len(bronze_set):>10}{len(bronze) - len(bronze_set):>14}{len(missing):>9}")
    print(f"{'Quarantine (flink)':<30}{len(quarantined):>8}")
    print(f"{'Late events (parked)':<30}{len(late):>8}")
    print(f"{'Silver (all sources)':<30}{len(silver):>8}{len(silver_set):>10}{len(silver) - len(silver_set):>14}{len(silver_missing):>9}")
    print(f"{'5-minute windows':<30}{windows:>8}  (sum of candles in windows: {window_candles})")
    print()
    print("Missing = candle keys in Kafka that are not in bronze, quarantine or late events.")
    print("Silver missing = bronze keys not in silver (run dbt first, silver is only current after a dbt run).")
    if unexpected:
        print(f"WARNING: {len(unexpected)} bronze keys do not exist in Kafka.")
    if missing:
        print(f"RESULT: {len(missing)} events missing, for example {sorted(missing)[:3]}")
        sys.exit(1)
    print("RESULT: no events missing between Kafka and bronze.")


if __name__ == "__main__":
    main()