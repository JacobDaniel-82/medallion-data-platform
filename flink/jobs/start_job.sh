#!/bin/sh
# Submit the klines job. Refuses to start a second copy, and restores from the newest checkpoint if one exists.
if flink list 2>/dev/null | grep -E "\((RUNNING|RESTARTING)\)" > /dev/null; then
  echo "A klines job is already running, not submitting another one:"
  flink list 2>/dev/null | grep -E "\((RUNNING|RESTARTING)\)"
  exit 0
fi
LATEST=$(ls -dt /opt/flink/checkpoints/*/chk-* 2>/dev/null | head -1)
if [ -n "$LATEST" ] && [ -f "$LATEST/_metadata" ]; then
  echo "Restoring from $LATEST"
  flink run -d -s "file://$LATEST" -py /opt/jobs/klines_job.py
else
  echo "No checkpoint found, starting fresh"
  flink run -d -py /opt/jobs/klines_job.py
fi