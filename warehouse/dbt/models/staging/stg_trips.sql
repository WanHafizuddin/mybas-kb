select
    trip_id,
    route_id,
    service_id,
    cast(direction_id as integer) as direction_id,
    trip_headsign,
    cast(n_stops as integer)      as n_stops
from {{ source('raw', 'trips') }}
