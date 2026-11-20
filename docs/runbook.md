# Runbook

First stop for every alert: the Pipeline Health tab in Superset, or run:
`docker exec crypto-airflow-scheduler python /opt/airflow/scripts/run_checks.py`

| Alert | What it means | Check | Fix |
|---|---|---|---|
| `bronze_live_freshness` / `bronze_live_candles_last_hour` | NiFi is not inserting candles | `docker ps`, NiFi UI (group running? funnel queue?) | Start NiFi or the group. Gaps heal with the next daily batch. The volume check stays red up to an hour |
| `bronze_batch_freshness`, `*_last_build` | A daily DAG did not run (laptop off, DAG paused) | Airflow UI, DAG pause state | Unpause and trigger the DAG |
| `[PIPELINE FAILED]` / `[PIPELINE RETRY]` | A task failed | Airflow, task Logs, first Error line | Fix the cause, trigger the DAG again. The previous data is untouched |
| `monitor cannot reach ClickHouse` | Database down | `docker ps`, `docker start crypto-clickhouse` | Start it, and the recovery message follows |
| `live_reject_rate`, `baserow_reject_rate` | Many records failing the contract | Quarantine tables, grouped by `failed_rules` | Fix the source. The loader already rolled back a bad load |
| `bronze_to_silver_*`, `silver_to_gold_*` | Layers disagree | Was dbt run after the last load? | Run the DAG so dbt rebuilds |
| `candle_gap_minutes_6h` | Candles missing | Which minutes (bronze, grouped by minute) | Trigger `crypto_medallion_pipeline`, the batch refills them |
| `price_jumps_over_2pct_24h` | A one-minute move above 2% | Compare with Binance for that minute | Warning only. A real crash needs no action |
| dbt test failure | A model broke a rule | `dbt test`, then look at the failing rows | Fix the model or the source data |

Show rejected records and why:
`SELECT table_name, row_id, arrayStringConcat(failed_rules, ',') AS reason FROM medallion_warehouse.quarantine_baserow_rows ORDER BY quarantined_at DESC LIMIT 20`

Reset to green after testing: remove test rows, then run both daily DAGs and the checks.
- `ALTER TABLE medallion_warehouse.bronze_baserow_rows DELETE WHERE row_id >= 900000`
- `ALTER TABLE medallion_warehouse.bronze_market_candles DELETE WHERE source = 'smoketest'`
- Run `crypto_medallion_pipeline`, then `baserow_medallion_pipeline`, then `run_checks.py`.