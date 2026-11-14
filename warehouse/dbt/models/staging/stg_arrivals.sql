select
    {{ dbt.concat(["trip_id", "'|'", "cast(service_date as " ~ dbt.type_string() ~ ")", "'|'", "cast(stop_sequence as " ~ dbt.type_string() ~ ")"]) }}
                                        as arrival_id,
    agency,
    route_id,
    trip_id,
    stop_id,
    cast(stop_sequence as integer)      as stop_sequence,
    service_date,
    vehicle_id,
    scheduled_arrival                   as scheduled_arrival_utc,
    actual_arrival                      as actual_arrival_utc,
    cast(delay_seconds as integer)      as delay_seconds
from {{ source('raw', 'arrivals') }}
