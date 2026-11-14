select
    point_id,
    name       as point_name,
    latitude,
    longitude
from {{ source('raw', 'weather_points') }}
