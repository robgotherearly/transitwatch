-- dbt/models/staging/stg_trip_updates.sql
-- Cleans and deduplicates raw_trip_updates.
-- One row per unique trip+stop+fetch combination.
-- Runs as a VIEW — always reflects latest raw data.

{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select *
    from {{ source('transitwatch', 'raw_trip_updates') }}
    where fetched_at >= now() - interval '{{ var("lookback_hours", 2) }} hours'
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by trip_id, stop_id, start_date, start_time
            order by fetched_at desc
        ) as rn
    from source
    where
        trip_id     is not null
        and stop_id is not null
),

cleaned as (
    select
        md5(
            coalesce(trip_id, '') || '|' ||
            coalesce(stop_id, '') || '|' ||
            coalesce(start_date, '') || '|' ||
            coalesce(start_time, '')
        )                                               as surrogate_key,

        trip_id,
        route_id,
        direction_id,

        case
            when start_date ~ '^\d{8}$'
            then to_date(start_date, 'YYYYMMDD')
            else null
        end                                             as service_date,

        stop_id,
        stop_sequence,

        -- Default null delays to 0 (on time) instead of dropping them
        -- Only null out extreme values that are clearly bad data
        case
            when arrival_delay is null then 0
            when arrival_delay between -3600 and 7200 then arrival_delay
            else 0
        end                                             as arrival_delay_s,

        case
            when departure_delay is null then 0
            when departure_delay between -3600 and 7200 then departure_delay
            else 0
        end                                             as departure_delay_s,

        schedule_relationship,
        fetched_at

    from deduplicated
    where rn = 1
)

select * from cleaned