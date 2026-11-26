import json
import os
import time

import clickhouse_connect
from jsonschema import Draft202012Validator

from pyflink.common import Duration, Types, WatermarkStrategy
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
from pyflink.datastream.state import ValueStateDescriptor

TOPIC = "binance.klines.1m"
DLQ_TOPIC = "binance.klines.1m.dlq"
BOOTSTRAP = "kafka:9092"
STATE_RETENTION_MS = 24 * 3600 * 1000

INVALID = OutputTag("invalid", Types.STRING())
DUPLICATE = OutputTag("duplicate", Types.STRING())


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


class Dedupe(KeyedProcessFunction):
    def open(self, runtime_context):
        self.seen = runtime_context.get_state(ValueStateDescriptor("seen", Types.BOOLEAN()))

    def process_element(self, value, ctx):
        if self.seen.value():
            yield DUPLICATE, f"duplicate dropped: {value['symbol']}|{value['interval']}|{value['open_time']}"
        else:
            self.seen.update(True)
            ctx.timer_service().register_event_time_timer(ctx.timestamp() + STATE_RETENTION_MS)
            yield value

    def on_timer(self, timestamp, ctx):
        self.seen.clear()
        return []


class ClickHouseInsert(MapFunction):
    COLUMNS = [
        "source", "symbol", "interval", "open_time_ms", "close_time_ms",
        "open_price_raw", "high_price_raw", "low_price_raw", "close_price_raw",
        "volume_raw", "number_of_trades",
    ]

    def open(self, runtime_context):
        self.client = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_HOST"],
            port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
            username=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ["CLICKHOUSE_PASSWORD"],
            database="medallion_warehouse",
        )

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

    def close(self):
        self.client.close()


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

    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-klines")
    parsed = raw.process(ParseAndValidate(), output_type=Types.PICKLED_BYTE_ARRAY())
    invalid = parsed.get_side_output(INVALID)

    stamped = parsed.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(60))
        .with_timestamp_assigner(EventTime())
    )
    deduped = (
        stamped.key_by(lambda e: f"{e['symbol']}|{e['interval']}|{e['open_time']}", key_type=Types.STRING())
        .process(Dedupe(), output_type=Types.PICKLED_BYTE_ARRAY())
    )
    duplicates = deduped.get_side_output(DUPLICATE)
    inserted = deduped.map(ClickHouseInsert(), output_type=Types.STRING())

    inserted.print()
    duplicates.print()
    invalid.sink_to(dlq_sink)

    env.execute("klines-dedupe")


if __name__ == "__main__":
    main()