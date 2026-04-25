-- dbt/models/marts/mart_hourly_performance.sql
-- Hourly aggregation of delay performance across all routes.
-- Answers: what time of day is the subway worst?
-- Uses LAG() to compute hour-over-hour delay change.



with trip_updates as (
    select *
    from "transitwatch"."public_staging"."stg_trip_updates"
    where arrival_delay_s is not null
      and service_date    is not null
),

hourly_raw as (
    select
        service_date,
        extract(hour from fetched_at)::int          as hour_of_day,
        count(*)                                    as total_updates,
        count(*) filter (
            where arrival_delay_s > 60
        )                                           as delayed_count,
        round(avg(arrival_delay_s)::numeric, 2)     as avg_delay_s
    from trip_updates
    group by service_date, extract(hour from fetched_at)::int
),

-- Find worst route per hour using a lateral subquery pattern
worst_routes as (
    select distinct on (tu.service_date, extract(hour from tu.fetched_at)::int)
        tu.service_date,
        extract(hour from tu.fetched_at)::int       as hour_of_day,
        tu.route_id                                 as worst_route_id,
        avg(tu.arrival_delay_s)                     as route_avg_delay
    from trip_updates tu
    where tu.arrival_delay_s is not null
    group by tu.service_date, extract(hour from tu.fetched_at)::int, tu.route_id
    order by
        tu.service_date,
        extract(hour from tu.fetched_at)::int,
        avg(tu.arrival_delay_s) desc
),

joined as (
    select
        h.service_date,
        h.hour_of_day,
        h.total_updates,
        h.delayed_count,
        h.avg_delay_s,
        w.worst_route_id,

        -- Hour-over-hour delay change using LAG
        lag(h.avg_delay_s) over (
            partition by h.service_date
            order by h.hour_of_day
        )                                           as prev_hour_avg_delay_s,

        round(
            h.avg_delay_s - lag(h.avg_delay_s) over (
                partition by h.service_date
                order by h.hour_of_day
            ), 2
        )                                           as delay_change_s

    from hourly_raw h
    left join worst_routes w
        on h.service_date = w.service_date
        and h.hour_of_day = w.hour_of_day
)

select
    service_date,
    hour_of_day,
    total_updates,
    delayed_count,
    avg_delay_s,
    worst_route_id,
    prev_hour_avg_delay_s,
    delay_change_s,
    now()                                           as updated_at
from joined
order by service_date, hour_of_day