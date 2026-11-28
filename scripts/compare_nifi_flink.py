import argparse
import os
from datetime import datetime, timezone

import clickhouse_connect

from envfile import load_env

load_env()

parser = argparse.ArgumentParser(description="Compare NiFi and Flink candles in bronze from a start time (UTC).")
parser.add_argument("since", help="UTC start, e.g. '2026-11-27 15:30:00' (the first candle both pipelines saw)")
since = datetime.strptime(parser.parse_args().since, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

ch = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.environ["CLICKHOUSE_PASSWORD"],
    database="medallion_warehouse",
)

result = ch.query(
    "SELECT source, open_time, any(open_price), any(high_price), any(low_price), any(close_price), "
    "any(volume), any(number_of_trades) FROM bronze_market_candles "
    "WHERE source IN ('nifi', 'flink') AND open_time >= {s:DateTime64(3)} GROUP BY source, open_time",
    parameters={"s": since},
).result_rows

data = {"nifi": {}, "flink": {}}
for source, open_time, *values in result:
    data[source][open_time] = tuple(values)

if not data["nifi"] or not data["flink"]:
    raise SystemExit(f"Need rows from both pipelines. nifi={len(data['nifi'])}, flink={len(data['flink'])}")

upper = min(max(data["nifi"]), max(data["flink"]))   # compare only up to the newest minute both could have seen
nifi = {t: v for t, v in data["nifi"].items() if t <= upper}
flink = {t: v for t, v in data["flink"].items() if t <= upper}

only_nifi = sorted(set(nifi) - set(flink))
only_flink = sorted(set(flink) - set(nifi))
mismatched = sorted(t for t in set(nifi) & set(flink) if nifi[t] != flink[t])

print(f"Compared candles from {since:%Y-%m-%d %H:%M} to {upper:%Y-%m-%d %H:%M} UTC")
print(f"  NiFi:  {len(nifi)} candles")
print(f"  Flink: {len(flink)} candles")
print(f"  only in NiFi:  {len(only_nifi)} {[f'{t:%H:%M}' for t in only_nifi[:5]]}")
print(f"  only in Flink: {len(only_flink)} {[f'{t:%H:%M}' for t in only_flink[:5]]}")
print(f"  same candle, different values: {len(mismatched)}")
for t in mismatched[:3]:
    print(f"    {t:%H:%M} nifi={nifi[t]} flink={flink[t]}")
print("RESULT: " + ("numbers match" if not (only_nifi or only_flink or mismatched) else "DIFFERENCES FOUND"))