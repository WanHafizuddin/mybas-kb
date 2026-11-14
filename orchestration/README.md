# Airflow (local, Docker Compose)

There are two containers:

- **Postgres:** Airflow's metadata database.
- **Airflow 3 standalone:** the API server, scheduler, DAG processor and
  triggerer, using LocalExecutor.

The image ([`Dockerfile`](Dockerfile)) adds Java 21, PySpark 4, dbt (DuckDB
and BigQuery adapters) and the Cloud Storage connector. These go in a
separate venv, `/opt/basmy-venv`, so they can't clash with Airflow's own
packages. The repo is bind-mounted at `/opt/basmy`.

## Start and stop

From the repo root.

Start:

```bash
docker compose -f orchestration/docker-compose.yml up -d --build
```

Then open http://localhost:8080. It's bound to 127.0.0.1 only and there's
no login, since everyone on localhost is treated as admin.

Stop (the Postgres volume is kept):

```bash
docker compose -f orchestration/docker-compose.yml down
```

## DAG: `basmy_daily`

```
prepare_synthetic_raw >> detect_arrivals >> fetch_weather >> load_warehouse >> dbt_run >> dbt_test
```

It's scheduled at `0 20 * * *` UTC (04:00 Kuala Lumpur) and processes
yesterday's service date (Kuala Lumpur time) on the **real** dataset. Catchup
is off.

When you trigger it manually (**Trigger DAG** → **config**), it takes these
parameters:

| param | values |
|---|---|
| `dataset` | `real` (default) or `synthetic`. Synthetic only generates raw data if `data/synthetic/raw` doesn't exist yet. |
| `engine` | `duckdb` (default) or `bigquery` |
| `service_date` | `""` = yesterday (Kuala Lumpur), `YYYY-MM-DD`, or `all` |

You can also trigger from the command line:

```bash
docker compose -f orchestration/docker-compose.yml exec airflow airflow dags trigger basmy_daily --conf '{"dataset":"synthetic","service_date":"all"}'
```

**Single-date runs.** With a date, the Spark job reads only the raw UTC
partitions for that date plus one day either side, keeps the pings that
belong to that service day, and overwrites only that `service_date`
partition. The warehouse load then refreshes the raw tables from all
partitions, and dbt rebuilds the models.

**Real data location.** `BASMY_REAL_RAW` sets where real raw data is read
from. The default is the local Supabase export in
`data/real/raw/vehicle_positions`. To read the collector's bucket instead,
set it to `gs://mybas-tracker-504819-data/raw/vehicle_positions`. That uses
your gcloud application-default credentials, mounted read-only.
