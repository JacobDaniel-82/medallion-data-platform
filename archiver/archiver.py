import io
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TOPIC = os.environ["KAFKA_TOPIC"]
BUCKET = os.environ.get("ARCHIVE_BUCKET", "archive")
PREFIX = os.environ.get("ARCHIVE_PREFIX", "binance/klines")
FLUSH_SECONDS = int(os.environ.get("FLUSH_SECONDS", "300"))
FLUSH_MESSAGES = int(os.environ.get("FLUSH_MESSAGES", "1000"))
PARTITION = 0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("archiver")

class _DropHeaderWarning(logging.Filter):
    def filter(self, record):
        return not record.getMessage().startswith("Failed to parse headers")


logging.getLogger("urllib3.connectionpool").addFilter(_DropHeaderWarning())

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["MINIO_ENDPOINT"],
    aws_access_key_id=os.environ["MINIO_ROOT_USER"],
    aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    region_name="us-east-1",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

SCHEMA = pa.schema([
    ("kafka_partition", pa.int32()),
    ("kafka_offset", pa.int64()),
    ("kafka_timestamp_ms", pa.int64()),
    ("key", pa.string()),
    ("value", pa.string()),
    ("symbol", pa.string()),
    ("interval", pa.string()),
    ("open_time_ms", pa.int64()),
    ("close_time_ms", pa.int64()),
    ("open_price", pa.string()),
    ("high_price", pa.string()),
    ("low_price", pa.string()),
    ("close_price", pa.string()),
    ("volume", pa.string()),
    ("number_of_trades", pa.int64()),
    ("produced_at_ms", pa.int64()),
])


def ensure_bucket():
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)
        log.info("created bucket %s", BUCKET)


def list_keys(prefix):
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def committed_ranges():
    ranges = []
    for key in list_keys(f"{PREFIX}/_committed/"):
        m = re.search(rf"p{PARTITION}-(\d+)-(\d+)$", key)
        if m:
            ranges.append((int(m.group(1)), int(m.group(2))))
    return sorted(ranges)


def to_row(msg):
    value = msg.value()
    raw = value.decode("utf-8", errors="replace") if value is not None else ""
    key = msg.key().decode("utf-8", errors="replace") if msg.key() is not None else None
    row = {
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
        "kafka_timestamp_ms": msg.timestamp()[1],
        "key": key,
        "value": raw,
        "symbol": None, "interval": None, "open_time_ms": None, "close_time_ms": None,
        "open_price": None, "high_price": None, "low_price": None, "close_price": None,
        "volume": None, "number_of_trades": None, "produced_at_ms": None,
    }
    try:
        e = json.loads(raw)
        row.update(
            symbol=str(e["symbol"]), interval=str(e["interval"]),
            open_time_ms=int(e["open_time"]), close_time_ms=int(e["close_time"]),
            open_price=str(e["open_price"]), high_price=str(e["high_price"]),
            low_price=str(e["low_price"]), close_price=str(e["close_price"]),
            volume=str(e["volume"]), number_of_trades=int(e["number_of_trades"]),
            produced_at_ms=int(e["produced_at"]),
        )
    except Exception:
        pass  # unparseable messages are archived raw, with empty parsed columns
    return row


def day_of(row):
    if row["open_time_ms"] is None:
        return "unparsed"
    try:
        return datetime.fromtimestamp(row["open_time_ms"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    except (OverflowError, OSError, ValueError):
        return "unparsed"


def flush(batch):
    start, end = batch[0]["kafka_offset"], batch[-1]["kafka_offset"]
    by_day = defaultdict(list)
    for row in batch:
        by_day[day_of(row)].append(row)
    for day, rows in by_day.items():
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), buf, compression="snappy")
        s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/dt={day}/p{PARTITION}-{start}-{end}.parquet", Body=buf.getvalue())
    # The marker comes last: only a range with a marker counts as archived.
    s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/_committed/p{PARTITION}-{start}-{end}", Body=b"")
    log.info("archived offsets %d-%d (%d messages, %d day folders)", start, end, len(batch), len(by_day))


def main():
    ensure_bucket()
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "archiver-unused",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, PARTITION), timeout=20)
    ranges = committed_ranges()
    if ranges:
        start = ranges[-1][1] + 1
        if start < low:
            log.error("messages up to offset %d have expired from Kafka but the archive ends at %d", low - 1, start - 1)
            sys.exit(2)
    else:
        start = low
    log.info("starting at offset %d (topic offsets %d to %d)", start, low, high)

    consumer.assign([TopicPartition(TOPIC, PARTITION, start)])
    next_offset = start
    batch, started = [], None

    while True:
        msg = consumer.poll(1.0)
        if msg is not None and msg.error():
            log.warning("consumer error: %s", msg.error())
        elif msg is not None:
            if msg.offset() != next_offset:
                raise RuntimeError(f"offset gap: expected {next_offset}, got {msg.offset()}")
            batch.append(to_row(msg))
            next_offset += 1
            started = started or time.time()
        if batch and (len(batch) >= FLUSH_MESSAGES or time.time() - started >= FLUSH_SECONDS):
            flush(batch)
            batch, started = [], None


if __name__ == "__main__":
    main()