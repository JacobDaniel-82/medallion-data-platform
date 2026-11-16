import os
import sys
import uuid
from pathlib import Path

import alert
import clickhouse_connect
import yaml

CONTRACTS = Path(os.getenv("CONTRACTS_DIR", "/opt/airflow/contracts"))
UNIT_SECONDS = {"second": 1, "seconds": 1, "minute": 60, "minutes": 60, "hour": 3600, "hours": 3600}

PRICE_JUMP_PCT = 2.0
GAP_WARN_MINUTES = 0
GAP_ERROR_MINUTES = 30
RECONCILE_WARN_PCT = 0.0
RECONCILE_ERROR_PCT = 0.1


def contract(name):
    with open(CONTRACTS / f"{name}.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def to_seconds(text):
    number, unit = str(text).split()
    return int(number) * UNIT_SECONDS[unit]


client = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST", "localhost"),
    port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
    username=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.environ["CLICKHOUSE_PASSWORD"],
    database="medallion_warehouse",
)

run_id = uuid.uuid4().hex[:12]
results = []


def scalar(sql):
    return client.query(sql).result_rows[0][0]


def add(domain, layer, name, ctype, value, warn, error, message, lower_is_bad=False, gate=False):
    value = float(value)
    status = "ok"
    if lower_is_bad:
        if error is not None and value < error:
            status = "error"
    else:
        if error is not None and value > error:
            status = "error"
        elif warn is not None and value > warn:
            status = "warn"
    results.append({
        "domain": domain, "layer": layer, "name": name, "type": ctype, "status": status,
        "value": value, "warn": warn, "error": error, "message": message, "gate": gate,
    })
    print(f"[{status.upper():5}] {domain}/{layer}/{name}: {value:g} - {message}")


def freshness(domain, layer, name, sql, warn_s, error_s):
    seconds = scalar(sql)
    add(domain, layer, name, "freshness", seconds, warn_s, error_s, f"{seconds / 60:.1f} minutes since the latest record")


def reconcile(domain, layer, name, left_sql, right_sql, label):
    left, right = scalar(left_sql), scalar(right_sql)
    if left:
        diff_pct = 100.0 * abs(left - right) / left
    else:
        diff_pct = 0.0 if right == 0 else 100.0
    add(domain, layer, name, "reconciliation", diff_pct, RECONCILE_WARN_PCT, RECONCILE_ERROR_PCT, f"{label}: {left} vs {right}")


def reject_rate(rejected, accepted):
    total = rejected + accepted
    return 100.0 * rejected / total if total else 0.0


live = contract("binance_klines_live")["sla"]
batch = contract("binance_klines_batch")["sla"]
shop = contract("baserow_ecommerce")["sla"]

# ---- Freshness ----
freshness("crypto", "bronze", "bronze_live_freshness",
          "SELECT dateDiff('second', max(ingested_at), now64(3)) FROM bronze_market_candles WHERE source = 'nifi'",
          to_seconds(live["freshness"]["warn_after"]), to_seconds(live["freshness"]["error_after"]))
freshness("crypto", "bronze", "bronze_batch_freshness",
          "SELECT dateDiff('second', max(ingested_at), now64(3)) FROM bronze_market_candles WHERE source = 'batch'",
          to_seconds(batch["freshness"]["warn_after"]), to_seconds(batch["freshness"]["error_after"]))
freshness("baserow", "bronze", "bronze_baserow_freshness",
          "SELECT dateDiff('second', max(extracted_at), now64(3)) FROM bronze_baserow_rows",
          to_seconds(shop["freshness"]["warn_after"]), to_seconds(shop["freshness"]["error_after"]))

LAST_BUILD = ("SELECT dateDiff('second', max(modification_time), now()) FROM system.parts "
              "WHERE database = 'medallion_warehouse' AND table = '{t}' AND active")
for dom, lay, tbl in [("crypto", "silver", "silver_market_candles"), ("crypto", "gold", "gold_hourly_market_stats"),
                      ("baserow", "silver", "silver_orders"), ("baserow", "gold", "gold_order_facts")]:
    freshness(dom, lay, f"{tbl}_last_build", LAST_BUILD.format(t=tbl),
              to_seconds(batch["freshness"]["warn_after"]), to_seconds(batch["freshness"]["error_after"]))

# ---- Volume ----
n = scalar("SELECT uniqExact(open_time) FROM bronze_market_candles "
           "WHERE source = 'nifi' AND open_time >= now64(3) - INTERVAL 1 HOUR")
add("crypto", "bronze", "bronze_live_candles_last_hour", "volume", n, None, live["volume"]["min_candles_per_hour"],
    f"{n} distinct live candles in the last hour", lower_is_bad=True)

n = scalar("SELECT count() FROM bronze_market_candles WHERE source = 'batch'")
add("crypto", "bronze", "bronze_batch_rows", "volume", n, None, batch["volume"]["min_rows_per_load"],
    f"{n} batch candles in bronze", lower_is_bad=True, gate=True)

counts = dict(client.query("SELECT table_name, count() FROM bronze_baserow_rows GROUP BY table_name").result_rows)
for table, minimum in shop["volume"]["min_rows"].items():
    n = counts.get(table, 0)
    add("baserow", "bronze", f"bronze_{table}_rows", "volume", n, None, minimum,
        f"{n} {table} rows in bronze", lower_is_bad=True, gate=True)

# ---- Quarantine rates ----
rej = scalar("SELECT count() FROM quarantine_market_candles "
             "WHERE source = 'nifi' AND quarantined_at >= now64(3) - INTERVAL 1 HOUR")
acc = scalar("SELECT count() FROM bronze_market_candles "
             "WHERE source = 'nifi' AND ingested_at >= now64(3) - INTERVAL 1 HOUR")
add("crypto", "bronze", "live_reject_rate_pct_1h", "quarantine", reject_rate(rej, acc), None,
    live["quarantine"]["max_reject_rate"] * 100, f"{rej} rejected, {acc} accepted in the last hour")

rej = scalar("SELECT count() FROM quarantine_market_candles "
             "WHERE source = 'batch' AND quarantined_at >= now64(3) - INTERVAL 24 HOUR")
acc = scalar("SELECT count() FROM bronze_market_candles "
             "WHERE source = 'batch' AND ingested_at >= now64(3) - INTERVAL 24 HOUR")
add("crypto", "bronze", "batch_reject_rate_pct_24h", "quarantine", reject_rate(rej, acc), None,
    batch["quarantine"]["max_reject_rate"] * 100, f"{rej} rejected, {acc} accepted in the last 24 hours", gate=True)

rej = scalar("SELECT count() FROM quarantine_baserow_rows "
             "WHERE extracted_at = (SELECT max(extracted_at) FROM bronze_baserow_rows)")
acc = scalar("SELECT count() FROM bronze_baserow_rows")
add("baserow", "bronze", "baserow_reject_rate_pct_latest_load", "quarantine", reject_rate(rej, acc), None,
    shop["quarantine"]["max_reject_rate"] * 100, f"{rej} rejected, {acc} accepted in the latest load", gate=True)

# ---- Reconciliation between layers ----
reconcile("crypto", "silver", "bronze_to_silver_candles",
          "SELECT uniqExact(symbol, `interval`, open_time) FROM bronze_market_candles "
          "WHERE open_time <= (SELECT max(open_time) FROM silver_market_candles)",
          "SELECT count() FROM silver_market_candles WHERE open_time >= (SELECT min(open_time) FROM bronze_market_candles)",
          "distinct bronze candles vs silver candles")
reconcile("crypto", "gold", "silver_to_gold_candles",
          "SELECT count() FROM silver_market_candles",
          "SELECT sum(candle_count) FROM gold_hourly_market_stats",
          "silver candles vs candles counted in the hourly gold table")
reconcile("baserow", "silver", "bronze_to_silver_orders",
          "SELECT count() FROM bronze_baserow_rows WHERE table_name = 'orders'",
          "SELECT count() FROM silver_orders",
          "bronze orders vs silver orders")
reconcile("baserow", "gold", "silver_to_gold_orders",
          "SELECT count() FROM silver_orders",
          "SELECT count() FROM gold_order_facts",
          "silver orders vs gold order facts")

# ---- Anomalies ----
n = scalar("""
    SELECT countIf(abs(r) > %s) FROM (
        SELECT (close_price / lagInFrame(close_price, 1, close_price) OVER (ORDER BY open_time) - 1) * 100 AS r
        FROM (SELECT open_time, argMax(close_price, ingested_at) AS close_price
              FROM bronze_market_candles
              WHERE open_time >= now64(3) - INTERVAL 24 HOUR
              GROUP BY open_time))
""" % PRICE_JUMP_PCT)
add("crypto", "bronze", "price_jumps_over_2pct_24h", "anomaly", n, 0, None,
    f"{n} one-minute moves above {PRICE_JUMP_PCT}% in the last 24 hours")

n = scalar("""
    SELECT 360 - uniqExact(open_time) FROM bronze_market_candles
    WHERE open_time >= toStartOfMinute(now64(3)) - INTERVAL 363 MINUTE
      AND open_time < toStartOfMinute(now64(3)) - INTERVAL 3 MINUTE
""")
add("crypto", "bronze", "candle_gap_minutes_6h", "anomaly", n, GAP_WARN_MINUTES, GAP_ERROR_MINUTES,
    f"{n} missing candle minutes in the last 6 hours", gate=True)

# ---- Alerts: notify on status changes (and on request with --remind) ----
previous = {
    (d, n): s
    for d, n, s in client.query(
        "SELECT domain, check_name, argMax(status, checked_at) FROM pipeline_check_results GROUP BY domain, check_name"
    ).result_rows
}
remind = "--remind" in sys.argv
lines = []
for r in results:
    before = previous.get((r["domain"], r["name"]), "ok")
    now = r["status"]
    if now != "ok" and (before != now or remind):
        lines.append(f"[{now.upper()}] {r['domain']}/{r['name']}: {r['message']}")
    elif now == "ok" and before in ("warn", "error"):
        lines.append(f"[RECOVERED] {r['domain']}/{r['name']}: {r['message']}")
if lines:
    alert.send("Medallion pipeline checks\n" + "\n".join(lines))

# ---- Save results ----
client.insert(
    "pipeline_check_results",
    [(run_id, r["domain"], r["layer"], r["name"], r["type"], r["status"], r["value"], r["warn"], r["error"], r["message"])
     for r in results],
    column_names=["run_id", "domain", "layer", "check_name", "check_type", "status", "value",
                  "warn_threshold", "error_threshold", "message"],
)

# ---- Exit code: any error fails the task. With --gate, only gate checks count. ----
gate_mode = "--gate" in sys.argv
domain_filter = next((a for a in sys.argv[1:] if a in ("crypto", "baserow")), None)
relevant = [r for r in results
            if (not domain_filter or r["domain"] == domain_filter) and (r["gate"] or not gate_mode)]
failed = [r for r in relevant if r["status"] == "error"]
print(f"{len(results)} checks run, {sum(r['status'] == 'warn' for r in results)} warnings, "
      f"{sum(r['status'] == 'error' for r in results)} errors.")
if failed:
    print("FAILED: " + ", ".join(f"{r['domain']}/{r['name']}" for r in failed))
    sys.exit(1)