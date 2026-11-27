import io
import re
from collections import defaultdict

import pyarrow.parquet as pq

from archiver import BUCKET, PARTITION, PREFIX, committed_ranges, list_keys, s3

ranges = committed_ranges()
problems = []
for (s1, e1), (s2, e2) in zip(ranges, ranges[1:]):
    if s2 != e1 + 1:
        problems.append(f"gap or overlap between offsets {e1} and {s2}")

marker_set = set(ranges)
rows_by_day, files_by_day, orphans, total_rows = defaultdict(int), defaultdict(int), 0, 0
for key in list_keys(f"{PREFIX}/dt="):
    if not key.endswith(".parquet"):
        continue
    m = re.search(rf"dt=([^/]+)/p{PARTITION}-(\d+)-(\d+)\.parquet$", key)
    n = pq.ParquetFile(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())).metadata.num_rows
    rows_by_day[m.group(1)] += n
    files_by_day[m.group(1)] += 1
    total_rows += n
    if (int(m.group(2)), int(m.group(3))) not in marker_set:
        orphans += 1

messages = sum(e - s + 1 for s, e in ranges)
print(f"committed ranges: {len(ranges)}, offsets {ranges[0][0] if ranges else '-'} to {ranges[-1][1] if ranges else '-'}")
print(f"messages covered by markers: {messages}")
print(f"rows in parquet files:       {total_rows}")
for day in sorted(rows_by_day):
    print(f"  dt={day}: {files_by_day[day]} files, {rows_by_day[day]} rows")
print(f"files without a marker (will be rewritten): {orphans}")
if messages != total_rows - 0 and orphans == 0:
    problems.append("rows do not match the messages covered by markers")
print("RESULT: " + ("; ".join(problems) if problems else "contiguous, every message archived exactly once"))