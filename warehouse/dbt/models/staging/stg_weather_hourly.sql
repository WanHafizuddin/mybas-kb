select
    point_id,
    hour_utc,
    temperature_c,
    precipitation_mm,
    rain_mm
from {{ source('raw', 'weather_hourly') }}
