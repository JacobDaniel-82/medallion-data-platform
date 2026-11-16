--health_checks

sql
SELECT domain, layer, check_name, check_type, status,
       multiIf(status = 'error', 2, status = 'warn', 1, 0) AS status_level,
       message, last_checked_at
FROM medallion_warehouse.pipeline_health_latest

--2. health_status_counts

sql
SELECT status, count() AS checks
FROM medallion_warehouse.pipeline_health_latest
GROUP BY status

--3. health_live_freshness_trend

sql
SELECT checked_at, value AS seconds_since_last_live_insert
FROM medallion_warehouse.pipeline_check_results
WHERE check_name = 'bronze_live_freshness'

--4. health_quarantine_by_hour

sql
SELECT toStartOfHour(quarantined_at) AS hour, source AS dataset, count() AS rejected
FROM medallion_warehouse.quarantine_market_candles
GROUP BY hour, dataset
UNION ALL
SELECT toStartOfHour(quarantined_at) AS hour, concat('baserow_', table_name) AS dataset, count() AS rejected
FROM medallion_warehouse.quarantine_baserow_rows
GROUP BY hour, dataset

--5. health_quarantine_reasons

sql
SELECT contract, arrayJoin(failed_rules) AS reason, count() AS rejected
FROM medallion_warehouse.quarantine_baserow_rows
GROUP BY contract, reason
UNION ALL
SELECT contract, arrayJoin(failed_rules) AS reason, count() AS rejected
FROM medallion_warehouse.quarantine_market_candles
GROUP BY contract, reason