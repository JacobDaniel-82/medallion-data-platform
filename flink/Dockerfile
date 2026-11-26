FROM flink:1.20-java17

USER root
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    python3 --version

# Install the PyFlink that matches the cluster's exact version.
RUN FLINK_VER=$(/opt/flink/bin/flink --version | sed -n 's/^Version: \([0-9.]*\).*/\1/p' | head -1) && \
    echo "Detected Flink $FLINK_VER" && \
    pip3 install --no-cache-dir apache-flink==$FLINK_VER

# Kafka connector for Flink 1.20.
RUN curl -fL -o /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar

# Schema validation and the ClickHouse client for the job.
RUN pip3 install --no-cache-dir jsonschema==4.23.0 clickhouse-connect
RUN pip3 show clickhouse-connect | head -2

# Folder for checkpoints. The named volume inherits this ownership.
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints

USER flink