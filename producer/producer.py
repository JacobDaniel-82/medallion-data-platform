import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition
from jsonschema import Draft202012Validator

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TOPIC = os.environ["KAFKA_TOPIC"]
DLQ_TOPIC = os.environ["KAFKA_DLQ_TOPIC"]
SYMBOL = os.environ.get("SYMBOL", "BTCUSDT")
INTERVAL = "1m"
BACKFILL_MINUTES = int(os.environ.get("INITIAL_BACKFILL_MINUTES", "10"))
SCHEMA_PATH = os.environ["SCHEMA_PATH"]

BINANCE_URL = "https://api.binance.com/api/v3/klines"
MINUTE_MS = 60_000
PAGE = 1000

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("producer")

with open(SCHEMA_PATH, encoding="utf-8") as f:
    VALIDATOR = Draft202012Validator(json.load(f))

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,
    "acks": "all",
    "linger.ms": 20,
})

delivery_errors = []


def on_delivery(err, msg):
    if err is not None:
        delivery_errors.append(str(err))


def now_ms():
    return int(time.time() * 1000)


def last_published_open_time():
    """Kafka is the only state: the highest open_time among the newest messages of the topic."""
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "producer-resume-" + uuid.uuid4().hex[:8],
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    try:
        low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=15)
        if high <= low:
            return None
        consumer.assign([TopicPartition(TOPIC, 0, max(low, high - 50))])
        newest = None
        deadline = time.time() + 30
        while time.time() < deadline:
            msg = consumer.poll(2)
            if msg is None or msg.error():
                continue
            try:
                value = int(json.loads(msg.value())["open_time"])
                newest = value if newest is None else max(newest, value)
            except Exception:
                pass  # invalid messages, for example injected tests, are skipped
            if msg.offset() >= high - 1:
                break
        return newest
    finally:
        consumer.close()


def fetch(start_ms):
    params = urllib.parse.urlencode({"symbol": SYMBOL, "interval": INTERVAL, "startTime": start_ms, "limit": PAGE})
    request = urllib.request.Request(
        f"{BINANCE_URL}?{params}", headers={"User-Agent": "medallion-producer/1.0"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def to_event(k):
    # Values stay exactly as Binance sends them, the same rule as the landing table.
    return {
        "schema_version": "1.0.0",
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "open_time": int(k[0]),
        "close_time": int(k[6]),
        "open_price": str(k[1]),
        "high_price": str(k[2]),
        "low_price": str(k[3]),
        "close_price": str(k[4]),
        "volume": str(k[5]),
        "number_of_trades": int(k[8]),
        "produced_at": now_ms(),
    }


def publish(event):
    key = f"{event['symbol']}|{event['interval']}|{event['open_time']}".encode()
    errors = list(VALIDATOR.iter_errors(event))
    if errors:
        dead = {"error": "; ".join(e.message for e in errors)[:500], "event": event, "source": "producer"}
        producer.produce(DLQ_TOPIC, key=key, value=json.dumps(dead).encode(), callback=on_delivery)
        return False
    producer.produce(
        TOPIC, key=key,
        value=json.dumps(event, separators=(",", ":")).encode(),
        headers=[("schema_version", b"1.0.0")],
        callback=on_delivery,
    )
    return True


def main():
    log.info("starting: topic=%s symbol=%s", TOPIC, SYMBOL)
    last_open = None
    backoff = 5
    while True:
        try:
            if last_open is None:
                last_open = last_published_open_time()
                if last_open is None:
                    last_open = (now_ms() // MINUTE_MS) * MINUTE_MS - (BACKFILL_MINUTES + 1) * MINUTE_MS
                    log.info("topic is empty, starting %d minutes back", BACKFILL_MINUTES)
                else:
                    log.info("resuming after the candle that opened at %d", last_open)

            klines = fetch(last_open + MINUTE_MS)
            now = now_ms()
            closed = [k for k in klines if int(k[6]) < now]   # never publish the forming candle

            del delivery_errors[:]
            sent = sum(1 for k in closed if publish(to_event(k)))
            remaining = producer.flush(30)
            if remaining or delivery_errors:
                raise RuntimeError(f"delivery failed: {remaining} unsent, errors={delivery_errors[:2]}")

            if closed:
                last_open = int(closed[-1][0])
            log.info("published %d candles, dead-lettered %d, last open_time %s",
                     sent, len(closed) - sent, last_open)
            backoff = 5

            if len(klines) >= PAGE:
                continue                                      # still catching up on history
            time.sleep(max(1.0, 60 - (time.time() % 60) + 3))  # next minute plus 3 seconds
        except Exception as exc:
            log.error("cycle failed: %s", exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            last_open = None                                  # re-read Kafka, the source of truth


if __name__ == "__main__":
    main()