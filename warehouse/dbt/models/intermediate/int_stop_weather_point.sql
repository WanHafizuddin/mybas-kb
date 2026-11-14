-- Nearest weather point to each stop. Routes reach ~150 km from Kota Bharu,
-- so one city-wide weather series would misstate rain on rural stops.
with distances as (
    select
        s.stop_id,
        p.point_id,
        p.point_name,
        {{ haversine_km('s.stop_lat', 's.stop_lon', 'p.latitude', 'p.longitude') }} as distance_km
    from {{ ref('stg_stops') }} as s
    cross join {{ ref('stg_weather_points') }} as p
)
select stop_id, point_id, point_name, distance_km
from distances
qualify row_number() over (partition by stop_id order by distance_km, point_id) = 1
