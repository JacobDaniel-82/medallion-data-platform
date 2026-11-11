-- Medallion Warehouse dashboard: ClickHouse queries used as Superset virtual datasets.
-- Each block starts with "-- DATASET: <name>". Paste one block at a time into SQL Lab.

-- DATASET: crypto_kpis
SELECT round(argMax(close_price, open_time), 2) AS latest_price,
       round(max(high_price), 2) AS high_24h,
       round(min(low_price), 2) AS low_24h,
       round(sum(volume), 1) AS volume_24h,
       round((argMax(close_price, open_time) / argMin(open_price, open_time) - 1) * 100, 2) AS change_24h_pct
FROM medallion_warehouse.silver_market_candles
WHERE open_time >= (SELECT max(open_time) FROM medallion_warehouse.silver_market_candles) - INTERVAL 24 HOUR

-- DATASET: crypto_price_ema
SELECT hour_start,
       round(hour_close, 2) AS close_price,
       round(exponentialTimeDecayedAvg(6)(hour_close, toFloat64(toUnixTimestamp(hour_start)) / 3600) OVER (PARTITION BY symbol ORDER BY hour_start), 2) AS ema_6h,
       round(exponentialTimeDecayedAvg(24)(hour_close, toFloat64(toUnixTimestamp(hour_start)) / 3600) OVER (PARTITION BY symbol ORDER BY hour_start), 2) AS ema_24h
FROM medallion_warehouse.gold_hourly_market_stats

-- DATASET: crypto_change_24h_asof
SELECT h.hour_start AS hour_start,
       round(h.hour_close, 2) AS close_now,
       round(p.hour_close, 2) AS close_24h_ago,
       round((h.hour_close / p.hour_close - 1) * 100, 3) AS change_24h_pct
FROM (SELECT symbol, hour_start, hour_close, hour_start - INTERVAL 24 HOUR AS prev_ts
      FROM medallion_warehouse.gold_hourly_market_stats) AS h
ASOF JOIN medallion_warehouse.gold_hourly_market_stats AS p
  ON h.symbol = p.symbol AND h.prev_ts >= p.hour_start

-- DATASET: crypto_volume_quantiles
SELECT hour_of_day,
       round(q[1], 2) AS p50_volume,
       round(q[2], 2) AS p95_volume,
       round(q[3], 2) AS p99_volume
FROM (SELECT toHour(open_time) AS hour_of_day,
             quantilesTDigest(0.5, 0.95, 0.99)(volume) AS q
      FROM medallion_warehouse.silver_market_candles
      GROUP BY hour_of_day)

-- DATASET: crypto_return_histogram
SELECT round(bin.1, 3) AS return_from_pct,
       round(bin.2, 3) AS return_to_pct,
       round(bin.3) AS candles
FROM (SELECT arrayJoin(histogram(30)(ret)) AS bin
      FROM (SELECT (close_price / open_price - 1) * 100 AS ret
            FROM medallion_warehouse.silver_market_candles))

-- DATASET: crypto_hourly_completeness
SELECT hour_start, candle_count
FROM medallion_warehouse.gold_hourly_market_stats
ORDER BY hour_start WITH FILL STEP INTERVAL 1 HOUR

-- DATASET: crypto_daily_volume_sparkbar
SELECT day, volume_by_hour_of_day, day_volume
FROM medallion_warehouse.gold_daily_volume_profile

-- DATASET: crypto_volume_price_correlation
SELECT round(corr(total_volume, abs(hour_close - hour_open)), 3) AS volume_vs_price_move_corr
FROM medallion_warehouse.gold_hourly_market_stats

-- DATASET: ecom_kpis
SELECT round(sumIf(net_amount, counts_as_revenue = 1)) AS net_revenue,
       count() AS orders,
       uniqIf(customer_code, counts_as_revenue = 1) AS buyers,
       round(sumIf(net_amount, counts_as_revenue = 1) / countIf(counts_as_revenue = 1), 2) AS avg_order_value,
       round(countIf(status = 'Returned') / count() * 100, 2) AS return_rate_pct
FROM medallion_warehouse.gold_order_facts

-- DATASET: ecom_weekly_trend
SELECT toStartOfWeek(order_date) AS week,
       count() AS orders,
       countIf(status = 'Returned') AS returned_orders,
       round(countIf(status = 'Returned') / count() * 100, 2) AS return_rate_pct,
       uniqIf(customer_code, counts_as_revenue = 1) AS buyers,
       round(sumIf(net_amount, counts_as_revenue = 1)) AS net_revenue
FROM medallion_warehouse.gold_order_facts
GROUP BY week

-- DATASET: ecom_top3_products_per_category
SELECT category, product_name,
       round(sum(net_amount), 2) AS net_revenue,
       sum(quantity) AS units
FROM medallion_warehouse.gold_order_facts
WHERE counts_as_revenue = 1
GROUP BY category, product_name
ORDER BY category, net_revenue DESC
LIMIT 3 BY category

-- DATASET: ecom_top_categories_per_country
SELECT country,
       arrayJoin(topKWeighted(3)(category, toUInt64(quantity))) AS top_category
FROM medallion_warehouse.gold_order_facts
WHERE counts_as_revenue = 1
GROUP BY country

-- DATASET: ecom_order_value_quantiles
SELECT category,
       round(q[1], 2) AS p25,
       round(q[2], 2) AS median,
       round(q[3], 2) AS p75,
       round(q[4], 2) AS p95
FROM (SELECT category,
             quantilesTDigest(0.25, 0.5, 0.75, 0.95)(net_amount) AS q
      FROM medallion_warehouse.gold_order_facts
      WHERE counts_as_revenue = 1
      GROUP BY category)

-- DATASET: ecom_customer_retention
SELECT cohort_week,
       sum(r[1]) AS week_0,
       sum(r[2]) AS week_1,
       sum(r[3]) AS week_2,
       sum(r[4]) AS week_3
FROM (SELECT customer_code, cohort_week,
             retention(weeks_since = 0, weeks_since = 1, weeks_since = 2, weeks_since = 3) AS r
      FROM (SELECT customer_code,
                   min(toStartOfWeek(order_date)) OVER (PARTITION BY customer_code) AS cohort_week,
                   dateDiff('week', cohort_week, toStartOfWeek(order_date)) AS weeks_since
            FROM medallion_warehouse.gold_order_facts
            WHERE counts_as_revenue = 1)
      GROUP BY customer_code, cohort_week)
GROUP BY cohort_week