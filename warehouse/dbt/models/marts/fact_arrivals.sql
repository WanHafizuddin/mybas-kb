{{ config(
    partition_by={'field': 'service_date', 'data_type': 'date'} if target.type == 'bigquery' else none,
    cluster_by=['route_id'] if target.type == 'bigquery' else none
) }}

-- One row per detected stop arrival with a sane delay, joined to the
-- weather at that stop's nearest weather point in the hour of arrival.
with arrivals as (
    select *, {{ to_local('actual_arrival_utc') }} as actual_arrival_local
    from {{ ref('int_arrivals_checked') }}
    where is_sane_delay
)
select
    a.arrival_id,
    a.agency,
    a.route_id,
    a.trip_id,
    a.stop_id,
    a.stop_sequence,
    a.service_date,
    a.vehicle_id,
    a.scheduled_arrival_utc,
    a.actual_arrival_utc,
    a.delay_seconds,
    a.delay_seconds / 60.0                                       as delay_minutes,
    extract(hour from a.actual_arrival_local)                    as local_hour,
    {{ iso_dow('a.actual_arrival_local') }}                      as day_of_week,
    {{ iso_dow('a.actual_arrival_local') }} in (6, 7)            as is_weekend,
    a.delay_seconds between {{ var('on_time_min_delay_s') }} and {{ var('on_time_max_delay_s') }}
                                                                 as is_on_time,
    d.weather_point_id,
    w.temperature_c,
    w.precipitation_mm,
    w.rain_mm,
    case when w.precipitation_mm is null then null
         else w.precipitation_mm >= {{ var('rain_threshold_mm') }} end as is_raining
from arrivals as a
left join {{ ref('dim_stop') }} as d
    on d.stop_id = a.stop_id
left join {{ ref('stg_weather_hourly') }} as w
    on  w.point_id = d.weather_point_id
    and w.hour_utc = {{ dbt.date_trunc('hour', 'a.actual_arrival_utc') }}
