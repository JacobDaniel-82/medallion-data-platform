import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal

import clickhouse_connect
from jsonschema import Draft202012Validator

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (
    KeyedProcessFunction,
    MapFunction,
    OutputTag,
    ProcessFunction,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetResetStrategy,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

TOPIC = "binance.klines.1m"
DLQ_TOPIC = "binance.klines.1m.dlq"
BOOTSTRAP = "kafka:9092"

WINDOW_MS = 5 * 60 * 1000
ALLOWED_LATENESS_MS = 10 * 60 * 1000
WATERMARK_DELAY_SECONDS = 60
STATE_RETENTION_MS = 24 * 3600 * 1000

INVALID = OutputTag("invalid", Types.STRING())
DUPLICATE = OutputTag("duplicate", Types.STRING())
LATE = OutputTag("late", Types.STRING())
LATE_WINDOW = OutputTag("late_window", Types.PICKLED_BYTE_ARRAY())


def to_dt(ms):
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


class ParseAndValidate(ProcessFunction):
    def open(self, runtime_context):
        with open(os.environ["SCHEMA_PATH"], encoding="utf-8") as f:
            self.validator = Draft202012Validator(json.load(f))

    @staticmethod
    def dead(raw, error):
        return json.dumps({
            "error": error,
            "raw": str(raw)[:2000],
            "source": "flink",
            "failed_at": int(time.time() * 1000),
        })

    def process_element(self, value, ctx):
        try:
            event = json.loads(value)
        except Exception as exc:
            yield INVALID, self.dead(value, f"not valid JSON: {exc}")
            return
        errors = [e.message for e in self.validator.iter_errors(event)]
        if errors:
            yield INVALID, self.dead(value, "; ".join(errors)[:500])
            return
        yield event


class EventTime(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value["open_time"])


class DedupeAndRoute(KeyedProcessFunction):
    """Duplicates are dropped. New events whose 5-minute window is already closed go to late_events."""

    def open(self, runtime_context):
        self.seen = runtime_context.get_state(ValueStateDescriptor("seen", Types.BOOLEAN()))

    def process_element(self, value, ctx):
        ts = int(value["open_time"])
        if self.seen.value():
            yield DUPLICATE, f"duplicate dropped: {value['symbol']}|{value['interval']}|{ts}"
            return

        watermark = ctx.timer_service().current_watermark()
        window_end = ts - (ts % WINDOW_MS) + WINDOW_MS
        if watermark >= window_end - 1 + ALLOWED_LATENESS_MS:
            yield LATE, json.dumps({"reason": "too_late", "watermark_ms": watermark, "event": value})
            return

        self.seen.update(True)
        ctx.timer_service().register_event_time_timer(ts + STATE_RETENTION_MS)
        yield value

    def on_timer(self, timestamp, ctx):
        self.seen.clear()
        return []


class Rollup(ProcessWindowFunction):
    """Recomputes the whole window every time it fires, so a late candle inside the allowed period updates it."""

    def process(self, key, context, elements):
        candles = sorted(elements, key=lambda e: int(e["open_time"]))
        first, last = candles[0], candles[-1]
        first_open = int(first["open_time"])
        window_start = first_open - (first_open % WINDOW_MS)
        yield {
            "symbol": first["symbol"],
            "interval": first["interval"],
            "window_start_ms": window_start,
            "window_end_ms": window_start + WINDOW_MS,
            "open": first["open_price"],
            "high": str(max(Decimal(c["high_price"]) for c in candles)),
            "low": str(min(Decimal(c["low_price"]) for c in candles)),
            "close": last["close_price"],
            "volume": str(sum(Decimal(c["volume"]) for c in candles)),
            "candle_count": len(candles),
            "emitted_at_ms": int(time.time() * 1000),
        }

    def clear(self, context):
        pass


class ClickHouseSink(MapFunction):
    def open(self, runtime_context):
        self.client = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_HOST"],
            port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
            username=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ["CLICKHOUSE_PASSWORD"],
            database="medallion_warehouse",
        )

    def close(self):
        self.client.close()


class CandleInsert(ClickHouseSink):
    COLUMNS = [
        "source", "symbol", "interval", "open_time_ms", "close_time_ms",
        "open_price_raw", "high_price_raw", "low_price_raw", "close_price_raw",
        "volume_raw", "number_of_trades",
    ]

    def map(self, e):
        # The insert completes before this record counts as processed, so a crash replays it.
        self.client.insert(
            "landing_market_candles",
            [[
                "flink", e["symbol"], e["interval"], int(e["open_time"]), int(e["close_time"]),
                e["open_price"], e["high_price"], e["low_price"], e["close_price"], e["volume"],
                int(e["number_of_trades"]),
            ]],
            column_names=self.COLUMNS,
        )
        return f"inserted {e['symbol']}|{e['interval']}|{e['open_time']}"


class RollupInsert(ClickHouseSink):
    def map(self, r):
        self.client.insert(
            "candles_5m",
            [[
                r["symbol"], r["interval"], to_dt(r["window_start_ms"]), to_dt(r["window_end_ms"]),
                float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]), float(r["volume"]),
                int(r["candle_count"]), to_dt(r["emitted_at_ms"]), int(r["emitted_at_ms"]),
            ]],
            column_names=[
                "symbol", "interval", "window_start", "window_end", "open_price", "high_price",
                "low_price", "close_price", "volume", "candle_count", "emitted_at", "version",
            ],
        )
        return f"rollup {r['symbol']} window={r['window_start_ms']} candles={r['candle_count']}"


class LateInsert(ClickHouseSink):
    def map(self, raw):
        item = json.loads(raw)
        e = item["event"]
        self.client.insert(
            "late_events",
            [[item["reason"], e["symbol"], e["interval"], to_dt(e["open_time"]), int(item["watermark_ms"]), json.dumps(e)]],
            column_names=["reason", "symbol", "interval", "open_time", "watermark_ms", "payload"],
        )
        return f"late event parked: {e['symbol']}|{e['interval']}|{e['open_time']} ({item['reason']})"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("flink-klines")
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST))
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    dlq_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(DLQ_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    watermarks = lambda: (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_DELAY_SECONDS))
        .with_timestamp_assigner(EventTime())
    )

    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-klines")
    parsed = raw.process(ParseAndValidate(), output_type=Types.PICKLED_BYTE_ARRAY()).uid("parse")
    invalid = parsed.get_side_output(INVALID)

    stamped = parsed.assign_timestamps_and_watermarks(watermarks())
    deduped = (
        stamped.key_by(lambda e: f"{e['symbol']}|{e['interval']}|{e['open_time']}", key_type=Types.STRING())
        .process(DedupeAndRoute(), output_type=Types.PICKLED_BYTE_ARRAY())
        .uid("dedupe")
    )
    duplicates = deduped.get_side_output(DUPLICATE)
    late_events = deduped.get_side_output(LATE)

    inserted = deduped.map(CandleInsert(), output_type=Types.STRING())

    windowed = (
        deduped.assign_timestamps_and_watermarks(watermarks())
        .key_by(lambda e: f"{e['symbol']}|{e['interval']}", key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .allowed_lateness(ALLOWED_LATENESS_MS)
        .side_output_late_data(LATE_WINDOW)
        .process(Rollup(), output_type=Types.PICKLED_BYTE_ARRAY())
        .uid("rollup")
    )
    rollups = windowed.map(RollupInsert(), output_type=Types.STRING())

    # Backstop: anything the window operator itself still rejects as late is parked too.
    late_from_window = windowed.get_side_output(LATE_WINDOW).map(
        lambda e: json.dumps({"reason": "window_closed", "watermark_ms": -1, "event": e}),
        output_type=Types.STRING(),
    )
    parked = late_events.union(late_from_window).map(LateInsert(), output_type=Types.STRING())

    inserted.print()
    rollups.print()
    parked.print()
    duplicates.print()
    invalid.sink_to(dlq_sink)

    env.execute("klines-stream")


if __name__ == "__main__":
    main()