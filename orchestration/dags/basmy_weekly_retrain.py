"""
Weekly retrain of the bus delay model (Phase 4).

    dbt build (rebuild + test the warehouse) -> train baseline + XGBoost
    -> export new version -> promote ONLY if MAE improves on the same test window

Runs Mondays 21:00 UTC (Tuesday 05:00 Kuala Lumpur, after the Monday daily
pipeline). On REAL data the training step exits with code 99 while there is
less than 4 weeks of history; Airflow marks it SKIPPED rather than failed.

Trigger parameters:
    dataset  "real" | "synthetic"   -- models are stored per dataset; a synthetic
                                       model can never become the real current model
    engine   "duckdb" | "bigquery"
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

PRELUDE = r"""
set -euo pipefail
cd "$BASMY_HOME"
export PYTHONPATH="$BASMY_HOME"
DATASET="{{ params.dataset }}"
ENGINE="{{ params.engine }}"
export BASMY_DATASET="$DATASET"
export BASMY_DUCKDB_PATH="$BASMY_HOME/data/$DATASET/warehouse.duckdb"
echo "dataset=$DATASET engine=$ENGINE models=$BASMY_MODELS"
"""

with DAG(
    dag_id="basmy_weekly_retrain",
    description="Rebuild warehouse, retrain delay model, promote only if MAE improves",
    schedule="0 21 * * 1",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "dataset": Param("real", enum=["real", "synthetic"]),
        "engine": Param("duckdb", enum=["duckdb", "bigquery"]),
    },
    tags=["basmy"],
) as dag:

    dbt_build = BashOperator(
        task_id="dbt_build",
        doc_md="Rebuild every model and run every dbt test before training on it.",
        bash_command=PRELUDE + r"""
cd warehouse/dbt
"$BASMY_DBT" build --profiles-dir . --target "$ENGINE"
""",
    )

    train_and_promote = BashOperator(
        task_id="train_and_promote",
        doc_md="Exit 99 (= skipped) when REAL history is under 4 weeks.",
        skip_on_exit_code=99,
        bash_command=PRELUDE + r"""
"$BASMY_PYTHON" -m ml.train --dataset "$DATASET" --engine "$ENGINE" --models "$BASMY_MODELS"
""",
    )

    dbt_build >> train_and_promote
