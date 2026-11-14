select
    route_id,
    agency,
    route_short_name,
    route_long_name,
    route_color
from {{ ref('stg_routes') }}
