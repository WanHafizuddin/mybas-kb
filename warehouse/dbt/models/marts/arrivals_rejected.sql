-- Arrivals whose delay is outside the sane range (see dbt_project.yml vars).
-- Kept for inspection; excluded from fact_arrivals and every mart.
select *
from {{ ref('int_arrivals_checked') }}
where not is_sane_delay
