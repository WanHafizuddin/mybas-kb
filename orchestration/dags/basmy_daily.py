"""
Daily bas.my pipeline:

    raw positions --Spark--> arrivals --+--> load warehouse --> dbt run --> dbt test
    Open-Meteo archive ---> weather ----+

Runs every day at 20:00 UTC (04:00 in Kuala Lumpur, after service ends) and
processes the previous Kuala Lumpur service date.

Trigger parameters (Trigger DAG w/ config):
    dataset       "real" | "synthetic"   -- never mixed; each has its own warehouse
    engine        "duckdb" | "bigquery"
    service_date  "" (= yesterday, Kuala Lumpur time), "YYYY-MM-DD", or "all"
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

GTFS = (
    "$BASMY_HOME/data/gtfs_static/mybas-kota-bharu/2026-08-09.zip "
    "$BASMY_HOME/data/gtfs_static/mybas-kota-bharu/2026-11-14.zip"
)

# Shared shell prelude: resolve the parameters into paths and flags.
PRELUDE = r"""
set -euo pipefail
cd "$BASMY_HOME"
export PYTHONPATH="$BASMY_HOME"
DATASET="{{ params.dataset }}"
ENGINE="{{ params.engine }}"
SD="{{ params.service_date }}"
# Default: yesterday in Kuala Lumpur. The scheduled run fires at 04:00 KL, so
# this is the service day that just ended (catchup is off, so no backfills).
if [ -z "$SD" ]; then SD=$(TZ=Asia/Kuala_Lumpur date -d yesterday +%F); fi
LABEL=$(echo "$DATASET" | tr '[:lower:]' '[:upper:]')
if [ "$DATASET" = real ]; then RAW="$BASMY_REAL_RAW"; else RAW="$BASMY_HOME/data/synthetic/raw/vehicle_positions"; fi
ARRIVALS="$BASMY_HOME/data/$DATASET/processed/arrivals"
WEATHER="$BASMY_HOME/data/$DATASET/weather"
export BASMY_DATASET="$DATASET"
export BASMY_DUCKDB_PATH="$BASMY_HOME/data/$DATASET/warehouse.duckdb"
if [ "$SD" = all ]; then SD_FLAG=""; else SD_FLAG="--service-date $SD"; fi
echo "dataset=$DATASET engine=$ENGINE service_date=$SD raw=$RAW"
"""

with DAG(
    dag_id="basmy_daily",
    description="Raw positions -> arrivals (Spark) -> weather -> warehouse -> dbt run/test",
    schedule="0 20 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    params={
        "dataset": Param("real", enum=["real", "synthetic"]),
        "engine": Param("duckdb", enum=["duckdb", "bigquery"]),
        "service_date": Param("", type="string",
                              description="'' = yesterday (Kuala Lumpur), YYYY-MM-DD, or 'all'"),
    },
    tags=["basmy"],
) as dag:

    prepare_synthetic = BashOperator(
        task_id="prepare_synthetic_raw",
        doc_md="Synthetic runs only: generate SYNTHETIC raw data if none exists yet.",
        bash_command=PRELUDE + r"""
if [ "$DATASET" = synthetic ] && [ ! -d "$RAW" ]; then
  "$BASMY_PYTHON" -m transform.synthetic --out "$BASMY_HOME/data/synthetic" --days 3 --trips-per-day 40
else
  echo "nothing to generate"
fi
""",
    )

    detect_arrivals = BashOperator(
        task_id="detect_arrivals",
        doc_md="Phase 2 Spark job: dedupe, match to trips, interpolate stop arrivals.",
        bash_command=PRELUDE + f"""
"$BASMY_PYTHON" -m transform.arrivals_job --raw "$RAW" --gtfs {GTFS} \\
  --out "$ARRIVALS" --label "$LABEL" $SD_FLAG --samples 0 \\
  --report-json "$BASMY_HOME/data/$DATASET/quality/arrivals_$SD.json"
""",
    )

    fetch_weather = BashOperator(
        task_id="fetch_weather",
        bash_command=PRELUDE + r"""
"$BASMY_PYTHON" -m warehouse.weather.fetch_weather --arrivals "$ARRIVALS" --out "$WEATHER/weather_hourly.parquet"
""",
    )

    load_warehouse = BashOperator(
        task_id="load_warehouse",
        bash_command=PRELUDE + f"""
"$BASMY_PYTHON" -m warehouse.load --dataset "$DATASET" --engine "$ENGINE" \\
  --arrivals "$ARRIVALS" --weather "$WEATHER" --gtfs {GTFS}
""",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=PRELUDE + r"""
cd warehouse/dbt
"$BASMY_DBT" run --profiles-dir . --target "$ENGINE"
""",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=PRELUDE + r"""
cd warehouse/dbt
"$BASMY_DBT" test --profiles-dir . --target "$ENGINE"
""",
    )

    prepare_synthetic >> detect_arrivals >> fetch_weather >> load_warehouse >> dbt_run >> dbt_test
