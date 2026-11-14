# Looker Studio charts

Four charts for a bas.my reliability dashboard, built on the BigQuery dataset
`basmy_real` (never `basmy_synthetic` for anything shown as real). In Looker
Studio: **Add data → BigQuery → Custom query**, choose project
`mybas-tracker-504819`, paste one query per data source, and turn on
**Enable date range parameters** so the report's date control feeds
`@DS_START_DATE` / `@DS_END_DATE` (format `YYYYMMDD`).

| File | Chart type | Dimension(s) | Metric(s) |
|---|---|---|---|
| `1_on_time_by_route.sql` | Bar chart (horizontal), sorted by on-time rate | `route` | `on_time_rate` (percent), `n_arrivals` as tooltip |
| `2_delay_by_hour.sql` | Line chart | `local_hour` | `avg_delay_minutes`, `p90_delay_minutes` |
| `3_on_time_heatmap.sql` | Pivot table with heatmap | rows `local_hour`, columns `day_name` | `on_time_rate` |
| `4_rain_vs_dry.sql` | Grouped bar chart | `route`, breakdown `weather` | `avg_delay_minutes`, `on_time_rate` |

All times are Kuala Lumpur local (`local_hour`, `day_of_week` come from
`fact_arrivals`). On-time = 1 minute early to 5 minutes late (dbt vars).
Each query drops groups with fewer than 20 arrivals so a handful of trips
can't make a route look perfect or terrible.

Cost: each query scans only a few columns of a table partitioned by
`service_date`, so it stays far inside BigQuery's free 1 TB/month of queries.
