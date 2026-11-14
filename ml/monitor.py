"""
Daily prediction monitoring (Phase 5).

Joins the predictions served by /predict-delay (Supabase table
delay_predictions) to the arrivals the pipeline actually detected
(fact_arrivals) for one service date, and computes the daily MAE per source.
Warns when the model's daily MAE is more than 30% worse than its test MAE
(from the serving model's metadata).

Only predictions made BEFORE the bus actually arrived are scored (a request
made after the arrival is not a prediction).

Exit codes: 0 = done (alert or not; alerts are logged as WARNING),
99 = nothing to check (no DATABASE_URL / no predictions) -> Airflow "skipped".

Usage:
    python -m ml.monitor --service-date 2026-12-14 --dataset real
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DEGRADATION_THRESHOLD = 1.30   # alert when daily MAE > 130% of the model's test MAE
EXIT_NOTHING_TO_CHECK = 99

log = logging.getLogger("ml.monitor")


@dataclass
class MonitorResult:
    service_date: str
    n_predictions: int = 0
    n_matched: int = 0
    mae_minutes_by_source: dict = field(default_factory=dict)
    n_by_source: dict = field(default_factory=dict)
    model_version: str | None = None
    model_test_mae_minutes: float | None = None
    alert: bool = False
    message: str = ""


def daily_mae(predictions: pd.DataFrame, arrivals: pd.DataFrame) -> pd.DataFrame:
    """One row per matched prediction with its absolute error in minutes."""
    p = predictions.copy()
    a = arrivals[["trip_id", "service_date", "stop_sequence", "actual_arrival_utc"]].copy()
    for df in (p, a):
        df["service_date"] = pd.to_datetime(df["service_date"]).dt.date
        df["stop_sequence"] = df["stop_sequence"].astype("int64")
    m = p.merge(a, on=["trip_id", "service_date", "stop_sequence"], how="inner")
    m["requested_at"] = pd.to_datetime(m["requested_at"], utc=True)
    m["predicted_arrival"] = pd.to_datetime(m["predicted_arrival"], utc=True)
    m["actual_arrival_utc"] = pd.to_datetime(m["actual_arrival_utc"], utc=True)
    m = m[m["requested_at"] < m["actual_arrival_utc"]]
    m["abs_error_min"] = (m["predicted_arrival"] - m["actual_arrival_utc"]).abs().dt.total_seconds() / 60.0
    return m


def evaluate(predictions: pd.DataFrame, arrivals: pd.DataFrame, service_date: date,
             model_meta: dict | None, threshold: float = DEGRADATION_THRESHOLD) -> MonitorResult:
    res = MonitorResult(service_date=service_date.isoformat(), n_predictions=len(predictions))
    m = daily_mae(predictions, arrivals) if len(predictions) else predictions
    res.n_matched = len(m)
    if res.n_matched:
        g = m.groupby("source")["abs_error_min"]
        res.mae_minutes_by_source = {k: round(float(v), 4) for k, v in g.mean().items()}
        res.n_by_source = {k: int(v) for k, v in g.size().items()}
    if model_meta:
        res.model_version = model_meta.get("version")
        res.model_test_mae_minutes = (model_meta.get("mae_minutes") or {}).get("xgboost")
    model_mae = res.mae_minutes_by_source.get("model")
    if model_mae is not None and res.model_test_mae_minutes:
        limit = threshold * res.model_test_mae_minutes
        res.alert = model_mae > limit
        res.message = (f"{'ALERT' if res.alert else 'OK'}: model MAE {model_mae:.3f} min on {service_date} vs "
                       f"test MAE {res.model_test_mae_minutes:.3f} (alert above {limit:.3f}, +30%)")
    elif model_mae is None:
        res.message = "no model-sourced predictions matched to arrivals; nothing to compare"
    else:
        res.message = "serving model metadata has no test MAE; nothing to compare"
    return res


# --- I/O ---------------------------------------------------------------------------------------

def read_predictions(database_url: str, service_date: date) -> pd.DataFrame:
    import psycopg

    with psycopg.connect(database_url) as conn:
        conn.read_only = True
        cur = conn.execute(
            """SELECT requested_at, trip_id, service_date, stop_sequence, predicted_arrival, source, model_version
               FROM delay_predictions
               WHERE service_date = %s AND trip_id IS NOT NULL AND stop_sequence IS NOT NULL""",
            (service_date,),
        )
        cols = [d.name for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def read_arrivals(duckdb_path: str, service_date: date) -> pd.DataFrame:
    import duckdb

    con = duckdb.connect(duckdb_path, read_only=True)
    try:
        con.execute("SET TimeZone = 'UTC'")
        return con.execute(
            """select trip_id, service_date, stop_sequence, actual_arrival_utc
               from analytics.fact_arrivals where service_date = ?""", [service_date]).df()
    finally:
        con.close()


def serving_model_meta(models_uri: str, dataset: str) -> dict | None:
    from ml.registry import Store, current_version, read_metadata

    store = Store(models_uri)
    version = current_version(store, dataset)
    return read_metadata(store, dataset, version) if version else None


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Daily MAE of served predictions vs detected arrivals")
    p.add_argument("--service-date", type=date.fromisoformat, required=True)
    p.add_argument("--dataset", choices=["real", "synthetic"], default="real")
    p.add_argument("--duckdb-path", help="default: data/<dataset>/warehouse.duckdb")
    p.add_argument("--models", default=os.environ.get("BASMY_MODELS", str(ROOT / "data" / "models")))
    p.add_argument("--report-json", help="write the result as JSON here")
    args = p.parse_args(argv)

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("monitor: DATABASE_URL not set, cannot read delay_predictions; skipping")
        return EXIT_NOTHING_TO_CHECK
    try:
        predictions = read_predictions(database_url, args.service_date)
    except Exception as exc:  # noqa: BLE001 -- e.g. table not created yet
        print(f"monitor: cannot read delay_predictions ({type(exc).__name__}: {exc}); skipping")
        return EXIT_NOTHING_TO_CHECK
    if predictions.empty:
        print(f"monitor: no predictions logged for {args.service_date}; skipping")
        return EXIT_NOTHING_TO_CHECK

    arrivals = read_arrivals(args.duckdb_path or str(ROOT / "data" / args.dataset / "warehouse.duckdb"),
                             args.service_date)
    result = evaluate(predictions, arrivals, args.service_date, serving_model_meta(args.models, args.dataset))
    (log.warning if result.alert else log.info)(result.message)
    print(json.dumps(result.__dict__, indent=2, default=str))
    if args.report_json:
        Path(args.report_json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report_json).write_text(json.dumps(result.__dict__, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
