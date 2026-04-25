-- dbt/models/staging/stg_vehicle_positions.sql
-- Cleans raw_vehicle_positions.
-- Filters invalid coordinates, deduplicates per vehicle per fetch window.

{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select *
    from {{ source('transitwatch', 'raw_vehicle_positions') }}
    where fetched_at >= now() - interval '{{ var("lookback_hours", 2) }} hours'
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by vehicle_id, trip_id
            order by fetched_at desc
        ) as rn
    from source
    where vehicle_id is not null
),

cleaned as (
    select
        md5(
            coalesce(vehicle_id, '') || '|' ||
            coalesce(trip_id, '')    || '|' ||
            fetched_at::text
        )                                               as surrogate_key,

        vehicle_id,
        trip_id,
        route_id,

        -- Filter invalid GPS coordinates
        case
            when latitude  between -90  and 90
             and longitude between -180 and 180
            then latitude
            else null
        end                                             as latitude,

        case
            when latitude  between -90  and 90
             and longitude between -180 and 180
            then longitude
            else null
        end                                             as longitude,

        speed                                           as speed_ms,
        current_status,

        -- Convert unix timestamp → timestamptz
        case
            when timestamp > 0
            then to_timestamp(timestamp)
            else null
        end                                             as position_time,

        fetched_at

    from deduplicated
    where rn = 1
)

select * from cleaned
