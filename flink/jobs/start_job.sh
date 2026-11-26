#!/bin/sh
# Submit the klines job. If a retained checkpoint exists, restore state and Kafka position from it.
LATEST=$(ls -dt /opt/flink/checkpoints/*/chk-* 2>/dev/null | head -1)
if [ -n "$LATEST" ] && [ -f "$LATEST/_metadata" ]; then
  echo "Restoring from $LATEST"
  flink run -d -s "file://$LATEST" -py /opt/jobs/klines_job.py
else
  echo "No checkpoint found, starting fresh"
  flink run -d -py /opt/jobs/klines_job.py
fi