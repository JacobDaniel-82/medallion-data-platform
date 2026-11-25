from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("binance.klines.1m")
    .set_group_id("flink-probe")
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-probe")
stream.map(lambda value: value[:90]).print()

env.execute("probe")