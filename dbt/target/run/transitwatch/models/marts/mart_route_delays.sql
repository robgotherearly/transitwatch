
  
    

  create  table "transitwatch"."public_marts"."mart_route_delays__dbt_tmp"
  
  
    as
  
  (
    -- dbt/models/marts/mart_route_delays.sql
-- Per-route delay summary for each service date.
-- Uses window functions for percentile calculations.
-- Answers: which routes are most delayed, and by how much?



with trip_updates as (
    select *
    from "transitwatch"."public_staging"."stg_trip_updates"
    where arrival_delay_s is not null
),

route_stats as (
    select
        route_id,
        service_date,
        direction_id,

        count(*)                                            as total_updates,

        -- Delay buckets
        count(*) filter (where arrival_delay_s > 60)        as delayed_count,
        count(*) filter (where arrival_delay_s < -30)       as early_count,
        count(*) filter (
            where arrival_delay_s between -60 and 60
        )                                                   as on_time_count,

        -- Delay statistics
        round(avg(arrival_delay_s)::numeric, 2)             as avg_delay_s,
        round(
            percentile_cont(0.5) within group (
                order by arrival_delay_s
            )::numeric, 2
        )                                                   as p50_delay_s,
        round(
            percentile_cont(0.95) within group (
                order by arrival_delay_s
            )::numeric, 2
        )                                                   as p95_delay_s,
        max(arrival_delay_s)                                as max_delay_s

    from trip_updates
    where route_id is not null
    group by route_id, service_date, direction_id
),

with_rates as (
    select
        *,
        round(
            delayed_count::numeric / nullif(total_updates, 0) * 100,
            2
        )                                                   as delay_rate_pct,

        -- Rank routes by avg delay within each service date
        rank() over (
            partition by service_date
            order by avg_delay_s desc
        )                                                   as delay_rank

    from route_stats
)

select
    route_id,
    service_date,
    direction_id,
    total_updates,
    delayed_count,
    early_count,
    on_time_count,
    avg_delay_s,
    p50_delay_s,
    p95_delay_s,
    max_delay_s,
    delay_rate_pct,
    delay_rank,
    now()                                                   as updated_at
from with_rates
  );
  