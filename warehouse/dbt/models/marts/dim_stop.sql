select
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    w.point_id    as weather_point_id,
    w.point_name  as weather_point_name,
    w.distance_km as weather_point_distance_km
from {{ ref('stg_stops') }} as s
left join {{ ref('int_stop_weather_point') }} as w using (stop_id)
