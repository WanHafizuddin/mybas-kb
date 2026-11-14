-- Every arrival, flagged as sane or not. Delays outside the sane range are
-- almost always a wrong trip_id in the feed or a GPS glitch, not real
-- lateness; they are routed to arrivals_rejected, never silently dropped.
select
    a.*,
    a.delay_seconds between {{ var('sane_min_delay_s') }} and {{ var('sane_max_delay_s') }} as is_sane_delay
from {{ ref('stg_arrivals') }} as a
