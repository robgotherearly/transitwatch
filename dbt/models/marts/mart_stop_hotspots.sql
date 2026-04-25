-- dbt/models/marts/mart_stop_hotspots.sql
-- Identifies the most delayed stops per route per day.
-- Answers: which stations are chronic delay hotspots?
-- Uses DENSE_RANK() to rank stops within each route+date.

{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with trip_updates as (
    select *
    from {{ ref('stg_trip_updates') }}
    where arrival_delay_s is not null
      and stop_id         is not null
      and route_id        is not null
      and service_date    is not null
),

stop_stats as (
    select
        stop_id,
        service_date,
        route_id,
        count(*) filter (
            where arrival_delay_s > 60
        )                                               as delay_count,
        round(avg(arrival_delay_s)::numeric, 2)         as avg_delay_s,
        round(
            percentile_cont(0.95) within group (
                order by arrival_delay_s
            )::numeric, 2
        )                                               as p95_delay_s,
        max(arrival_delay_s)                            as max_delay_s,
        count(*)                                        as total_observations
    from trip_updates
    group by stop_id, service_date, route_id
),

ranked as (
    select
        *,
        -- Rank stops within each route+date by avg delay
        dense_rank() over (
            partition by route_id, service_date
            order by avg_delay_s desc
        )                                               as delay_rank,

        -- Also rank globally across all routes that day
        dense_rank() over (
            partition by service_date
            order by avg_delay_s desc
        )                                               as global_delay_rank

    from stop_stats
    -- Only include stops with enough observations to be meaningful
    where total_observations >= 3
)

select
    stop_id,
    service_date,
    route_id,
    avg_delay_s,
    p95_delay_s,
    max_delay_s,
    delay_count,
    total_observations,
    delay_rank,
    global_delay_rank,
    now()                                               as updated_at
from ranked
order by service_date desc, global_delay_rank
