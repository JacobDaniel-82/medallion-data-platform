{{ config(materialized='table', engine='MergeTree()', order_by='day') }}

select
    toDate(hour_start) as day,
    sparkbar(24, 0, 23)(toHour(hour_start), total_volume) as volume_by_hour_of_day,
    round(sum(total_volume), 1) as day_volume
from {{ ref('gold_hourly_market_stats') }}
group by day