-- On-time performance by route, local hour of day and day of week.
select
    f.route_id,
    r.route_short_name,
    f.local_hour,
    f.day_of_week,
    count(*)                                                    as n_arrivals,
    avg(case when f.is_on_time then 1.0 else 0.0 end)           as on_time_rate,
    avg(case when f.delay_seconds > {{ var('on_time_max_delay_s') }} then 1.0 else 0.0 end)
                                                                as late_rate,
    avg(case when f.delay_seconds < {{ var('on_time_min_delay_s') }} then 1.0 else 0.0 end)
                                                                as early_rate,
    avg(f.delay_seconds) / 60.0                                 as avg_delay_minutes,
    count(distinct {{ dbt.concat(["f.trip_id", "'|'", "cast(f.service_date as " ~ dbt.type_string() ~ ")"]) }})
                                                                as n_trips
from {{ ref('fact_arrivals') }} as f
left join {{ ref('dim_route') }} as r using (route_id)
group by 1, 2, 3, 4
