{#
  Cross-database helpers so the same models run on DuckDB (local) and
  BigQuery (production). All stored timestamps are UTC; these turn them into
  Kuala Lumpur wall-clock values for hour-of-day / day-of-week analysis.
#}

{% macro to_local(ts) -%}
  {{ return(adapter.dispatch('to_local', 'basmy')(ts)) }}
{%- endmacro %}

{% macro default__to_local(ts) -%}
  ({{ ts }} at time zone '{{ var("local_timezone") }}')
{%- endmacro %}

{% macro bigquery__to_local(ts) -%}
  datetime({{ ts }}, '{{ var("local_timezone") }}')
{%- endmacro %}


{# ISO day of week: 1 = Monday ... 7 = Sunday. #}
{% macro iso_dow(local_ts) -%}
  {{ return(adapter.dispatch('iso_dow', 'basmy')(local_ts)) }}
{%- endmacro %}

{% macro default__iso_dow(local_ts) -%}
  isodow({{ local_ts }})
{%- endmacro %}

{% macro bigquery__iso_dow(local_ts) -%}
  mod(extract(dayofweek from {{ local_ts }}) + 5, 7) + 1
{%- endmacro %}


{# Great-circle distance in km (no RADIANS() in BigQuery, so use pi/180). #}
{% macro haversine_km(lat1, lon1, lat2, lon2) -%}
  2 * 6371.0 * asin(sqrt(
      power(sin(({{ lat2 }} - {{ lat1 }}) * 3.141592653589793 / 360.0), 2)
    + cos({{ lat1 }} * 3.141592653589793 / 180.0) * cos({{ lat2 }} * 3.141592653589793 / 180.0)
      * power(sin(({{ lon2 }} - {{ lon1 }}) * 3.141592653589793 / 360.0), 2)
  ))
{%- endmacro %}
