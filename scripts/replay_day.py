import argparse
import os
import sys
from datetime import date, datetime, timezone
from envfile import load_env

load_env()
import clickhouse_connect

ARCHIVE = "http://minio:9000/archive/binance/klines"   # as seen from inside the Docker network


def main():
    parser = argparse.ArgumentParser(description="Reload one archived UTC day into bronze (source = flink).")
    parser.add_argument("day", help="UTC day, YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="compare only, change nothing")
    args = parser.parse_args()

    day = date.fromisoformat(args.day)
    if day >= datetime.now(timezone.utc).date():
        sys.exit("Refusing to replay today or a future day: the live job is still writing it.")
    day_start_ms = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)
    next_day_ms = day_start_ms + 86_400_000

    user, secret = os.environ["MINIO_ROOT_USER"], os.environ["MINIO_ROOT_PASSWORD"]
    if "'" in user + secret:
        sys.exit("Quote characters in the MinIO credentials are not supported by this script.")

    ch = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database="medallion_warehouse",
    )

    def s3(pattern, structure=None):
        extra = f", '{structure}'" if structure else ""
        return f"s3('{ARCHIVE}/{pattern}', '{user}', '{secret}', 'Parquet'{extra})"

    STRUCTURE = (
        "kafka_offset Int64, symbol String, `interval` String, open_time_ms Int64, close_time_ms Int64, "
        "open_price String, high_price String, low_price String, close_price String, volume String, "
        "number_of_trades Int64"
    )

    day_files = f"dt={args.day}/*.parquet"
    try:
        newest = ch.query(f"SELECT max(open_time_ms) FROM {s3('dt=*/*.parquet')}").result_rows[0][0]
        archive_rows, archive_keys = ch.query(
            f"SELECT count(), uniqExact(symbol, `interval`, open_time_ms) FROM {s3(day_files)}"
        ).result_rows[0]
    except Exception as exc:
        sys.exit(f"Could not read the archive for {args.day}: {str(exc)[:300]}")

    if newest is None or newest < next_day_ms:
        sys.exit(f"The archive does not yet contain anything after {args.day}, so the day may be incomplete. Try again later.")

    def bronze_stats():
        return ch.query(
            "SELECT count(), uniqExact(open_time) FROM bronze_market_candles "
            "WHERE source = 'flink' AND toDate(open_time) = {d:Date}",
            parameters={"d": args.day},
        ).result_rows[0]

    before_rows, before_distinct = bronze_stats()
    print(f"Day {args.day}")
    print(f"  archive:        {archive_rows} messages, {archive_keys} distinct candles")
    print(f"  bronze before:  {before_rows} rows, {before_distinct} distinct candles, {before_rows - before_distinct} extra copies")
    if args.dry_run:
        print("Dry run, nothing changed.")
        return

    started = ch.query("SELECT now64(3)").result_rows[0][0]
    ch.command(
        "INSERT INTO landing_market_candles (source, symbol, `interval`, open_time_ms, close_time_ms, "
        "open_price_raw, high_price_raw, low_price_raw, close_price_raw, volume_raw, number_of_trades) "
        "SELECT 'flink', symbol, `interval`, open_time_ms, close_time_ms, open_price, high_price, low_price, "
        f"close_price, volume, number_of_trades FROM {s3(day_files, STRUCTURE)} "
        "ORDER BY kafka_offset LIMIT 1 BY symbol, `interval`, open_time_ms"
    )

    new_rows = ch.query(
        "SELECT count() FROM bronze_market_candles WHERE source = 'flink' "
        "AND toDate(open_time) = {d:Date} AND ingested_at >= {t:DateTime64(3)}",
        parameters={"d": args.day, "t": started},
    ).result_rows[0][0]
    quarantined = ch.query(
        "SELECT count() FROM quarantine_market_candles WHERE source = 'flink' AND quarantined_at >= {t:DateTime64(3)}",
        parameters={"t": started},
    ).result_rows[0][0]
    print(f"  reloaded:       {new_rows} rows into bronze, {quarantined} rejected by the contract")

    if new_rows + quarantined != archive_keys:
        ch.command(
            "ALTER TABLE bronze_market_candles DELETE WHERE source = 'flink' "
            "AND toDate(open_time) = {d:Date} AND ingested_at >= {t:DateTime64(3)} SETTINGS mutations_sync = 1",
            parameters={"d": args.day, "t": started},
        )
        sys.exit(f"ROLLED BACK: expected {archive_keys} candles but got {new_rows + quarantined}. The previous rows are untouched.")

    ch.command(
        "ALTER TABLE bronze_market_candles DELETE WHERE source = 'flink' "
        "AND toDate(open_time) = {d:Date} AND ingested_at < {t:DateTime64(3)} SETTINGS mutations_sync = 1",
        parameters={"d": args.day, "t": started},
    )
    after_rows, after_distinct = bronze_stats()
    print(f"  bronze after:   {after_rows} rows, {after_distinct} distinct candles, {after_rows - after_distinct} extra copies")
    verdict = "MATCH" if after_distinct == before_distinct else f"DIFFERENT: {before_distinct} distinct before, {after_distinct} after"
    print(f"RESULT: distinct candles {verdict}. Now run dbt so silver and gold pick up the reloaded day.")


if __name__ == "__main__":
    main()