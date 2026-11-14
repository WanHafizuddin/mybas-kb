select
    stop_id,
    stop_name,
    cast(stop_lat as {{ dbt.type_float() }}) as stop_lat,
    cast(stop_lon as {{ dbt.type_float() }}) as stop_lon
from {{ source('raw', 'stops') }}
