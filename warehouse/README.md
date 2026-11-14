# Phase 3: warehouse, orchestration, dashboard data

```
raw positions ─Spark (transform/)─> arrivals ─┐
Open-Meteo archive ─fetch_weather.py─> weather ┼─ load.py ─> raw tables ─dbt─> staging ─> dims / fact / mart
GTFS Static snapshots (data/gtfs_static) ──────┘        (DuckDB locally, BigQuery in production)
```

Airflow ([`orchestration/`](../orchestration/)) runs all of this daily at
20:00 UTC (04:00 in Kuala Lumpur), for the service date that just ended.

**Real and synthetic data never meet.** Each has its own warehouse:

| | DuckDB (local) | BigQuery (production) |
|---|---|---|
| raw tables | `data/<dataset>/warehouse.duckdb`, schema `raw` | dataset `basmy_<dataset>_raw` |
| dbt models | same file, schema `analytics` | dataset `basmy_<dataset>` |

`<dataset>` is `real` or `synthetic`.

## Weather: `weather/fetch_weather.py`

- Pulls hourly `temperature_2m`, `precipitation` and `rain` from the
  Open-Meteo archive, in UTC, with one request covering all points.
- The points are the 10 towns in
  [`config/weather_points.toml`](../config/weather_points.toml). Each stop
  uses its nearest point, so a stop on the Gua Musang road doesn't get Kota
  Bharu's rain.
- The date range is taken from the arrivals partitions, ±1 day.
- The archive lags real time by a few days. Missing hours stay `null`;
  `fact_arrivals.is_raining` is then `null` too, and chart 4 leaves those
  rows out.

## dbt: `dbt/`

| Model | What it is |
|---|---|
| `stg_*` | typed views over the raw tables |
| `int_stop_weather_point` | nearest weather point per stop (haversine) |
| `int_arrivals_checked` | every arrival, flagged `is_sane_delay` |
| `dim_route`, `dim_stop` | routes; stops with their weather point |
| `fact_arrivals` | sane arrivals, plus local hour, ISO day of week, weekend flag, on-time flag, and weather at that stop and hour. Partitioned by `service_date` on BigQuery. |
| `arrivals_rejected` | arrivals outside the sane range, kept for inspection |
| `mart_on_time_route_hour_dow` | on-time, late and early rates and average delay by route × local hour × day of week |

The definitions are dbt vars in [`dbt/dbt_project.yml`](dbt/dbt_project.yml):

- **On time:** from 1 minute early to 5 minutes late.
- **Sane:** from 30 minutes early to 2 hours late.
- **Raining:** at least 0.1 mm of precipitation in that hour.

**Tests (57):**

- `not_null` and `unique` on every key.
- `relationships`:
  - fact → `dim_route`
  - fact → `dim_stop`
  - arrivals → trips
  - trips → routes
  - weather → weather points
- `value_between`:
  - delays within the sane range
  - hour 0–23
  - coordinates inside Kelantan
  - temperature and precipitation within plausible bounds
  - on-time rate 0–1
- `unique_combination` on weather (point × hour) and on the mart's grain.
- `accepted_values` on day of week.

The SQL runs on both engines. Time-zone, day-of-week and distance logic is
in [`dbt/macros/time.sql`](dbt/macros/time.sql), with a DuckDB and a
BigQuery version of each macro.

## Run it by hand

From the repo root, for the synthetic dataset. Fetch weather and load the
DuckDB warehouse:

```bash
python -m warehouse.weather.fetch_weather --arrivals data/synthetic/processed/arrivals --out data/synthetic/weather/weather_hourly.parquet
```

```bash
python -m warehouse.load --dataset synthetic --engine duckdb --arrivals data/synthetic/processed/arrivals --weather data/synthetic/weather
```

Then build and test with dbt, from `warehouse/dbt` (bash):

```bash
cd warehouse/dbt && BASMY_DUCKDB_PATH=../../data/synthetic/warehouse.duckdb dbt build --profiles-dir . --target duckdb
```

**BigQuery:**

1. Run `terraform apply` in `infra/`. It creates `basmy_real`,
   `basmy_real_raw`, `basmy_synthetic` and `basmy_synthetic_raw` in
   us-central1.
2. Make sure you're signed in with `gcloud auth application-default login`.
3. Load the raw tables:

   ```bash
   python -m warehouse.load --dataset real --engine bigquery --project mybas-tracker-504819 --arrivals data/real/processed/arrivals --weather data/real/weather
   ```

4. Build the models, from `warehouse/dbt`:

   ```bash
   cd warehouse/dbt && BASMY_DATASET=real dbt build --profiles-dir . --target bigquery
   ```

## Looker Studio

[`looker/`](looker/) has the SQL for 4 charts, and a README on how to wire
each one up:

- on-time rate by route
- delay by hour
- heatmap of day × hour
- raining vs. dry

## Dependency note

dbt needs `protobuf < 7`, but `gtfs-realtime-bindings` 3.x needs protobuf 7.
So any environment with dbt uses bindings 2.2, which the `warehouse` extra in
`pyproject.toml` pins. The VM collector never installs dbt and keeps 3.x
(`collector/requirements.txt`).
