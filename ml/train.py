"""
Phase 4: train the bus delay model.

    warehouse (fact_arrivals + weather) -> training table (ml/features.py)
    -> time split (oldest 80% / newest 20%) -> baseline vs XGBoost -> MLflow
    -> export <models>/<dataset>/<version>/ -> promote only if MAE improves

Baseline: predicted delay at a stop = delay measured at the previous stop.
Both models are scored on the SAME test rows: those that have a
previous-stop delay (the baseline cannot predict the others).

On REAL data, training is refused with fewer than 28 days of history
(exit code 99, which the weekly Airflow DAG treats as "skipped").

Usage:
    python -m ml.train --dataset synthetic
    python -m ml.train --dataset real --engine bigquery --models gs://BUCKET/models
"""

import argparse
import json
import os
import sys
import tempfile
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from ml import features as F
from ml.registry import Store, current_version, export_model, read_metadata, set_current, should_promote
from ml.split import time_split

ROOT = Path(__file__).resolve().parent.parent
MIN_REAL_HISTORY_DAYS = 28
# A new model must beat the current one by at least this much MAE (minutes,
# = 0.3 s) on the same test window; smaller differences are numerical noise
# (e.g. the same data trained on another platform) and must not swap models.
MIN_IMPROVEMENT_MIN = 0.005
EXIT_INSUFFICIENT_HISTORY = 99

XGB_PARAMS = {
    "n_estimators": 600,
    "learning_rate": 0.05,
    "max_depth": 6,
    "min_child_weight": 5,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "objective": "reg:absoluteerror",   # we report MAE, so optimise it
    "tree_method": "hist",
    "enable_categorical": True,
    "max_cat_to_onehot": 1,
    "early_stopping_rounds": 50,
    "random_state": 42,
}


class InsufficientHistory(Exception):
    pass


def check_history(df: pd.DataFrame, dataset: str, min_days: int = MIN_REAL_HISTORY_DAYS) -> int:
    dates = pd.to_datetime(pd.Series(df["service_date"].unique()))
    span = 0 if dates.empty else (dates.max() - dates.min()).days + 1
    if dataset == "real" and span < min_days:
        raise InsufficientHistory(
            f"REAL data covers {span} day(s) ({len(dates)} service dates with arrivals); "
            f"training needs at least {min_days} days. Not training. "
            f"Keep the collector running and retry when the history is long enough."
        )
    return span


def mae(y, p) -> float:
    return float(np.mean(np.abs(np.asarray(y, dtype=float) - np.asarray(p, dtype=float))))


# The model predicts the CHANGE in delay relative to an offset that is known
# at prediction time (the last observed delay; else the route/hour 2-week
# average; else 0), and the offset is added back. Trees cannot extrapolate,
# so predicting absolute delay caps out on long routes where delays grow
# beyond what was seen in training; the residual form carries them forward.
TARGET_MODE = "residual_vs_last_known_delay"


def offset(df: pd.DataFrame) -> pd.Series:
    return df["prev_stop_delay_min"].fillna(df["route_hour_avg_delay_2w_min"]).fillna(0.0)


def predict_delay(predict_fn, df: pd.DataFrame, target_mode: str = TARGET_MODE) -> np.ndarray:
    raw = np.asarray(predict_fn(df[F.FEATURES]), dtype=float)
    return raw + offset(df).to_numpy() if target_mode == TARGET_MODE else raw


def apply_categories(df: pd.DataFrame, categories: dict[str, list]) -> pd.DataFrame:
    """Use exactly the training categories (unknown values become NaN), so
    codes mean the same thing at training, evaluation and serving time."""
    df = df.copy()
    for col, cats in categories.items():
        df[col] = pd.Categorical(df[col].astype(str), categories=cats)
    return df


def train_xgb(train: pd.DataFrame):
    import xgboost as xgb

    # Early-stopping validation = the newest ~10% of TRAINING dates (still in
    # the past relative to the test window).
    fit, val, _ = time_split(train, train_frac=0.9)
    model = xgb.XGBRegressor(**XGB_PARAMS)
    model.fit(fit[F.FEATURES], fit[F.LABEL] - offset(fit),
              eval_set=[(val[F.FEATURES], val[F.LABEL] - offset(val))], verbose=False)
    return model


def evaluate(model, test: pd.DataFrame) -> dict:
    comparable = test[test["prev_stop_delay_min"].notna()]
    xgb_pred = predict_delay(model.predict, comparable)
    base_pred = comparable["prev_stop_delay_min"]
    per_route = (
        comparable.assign(err_xgb=np.abs(comparable[F.LABEL] - xgb_pred),
                          err_base=np.abs(comparable[F.LABEL] - base_pred))
        .groupby("route_id", observed=True)
        .agg(n=("err_xgb", "size"), mae_baseline=("err_base", "mean"), mae_xgboost=("err_xgb", "mean"))
        .reset_index()
    )
    return {
        "n_test_rows": len(test),
        "n_compared_rows": len(comparable),
        "mae_baseline": mae(comparable[F.LABEL], base_pred),
        "mae_xgboost": mae(comparable[F.LABEL], xgb_pred),
        "mae_xgboost_all_test_rows": mae(test[F.LABEL], predict_delay(model.predict, test)),
        "per_route": per_route,
    }


def score_current(store: Store, dataset: str, test: pd.DataFrame) -> tuple[str | None, float | None, bool]:
    """Re-measure the current model's MAE on THIS test window."""
    import xgboost as xgb

    version = current_version(store, dataset)
    if version is None:
        return None, None, True
    meta = read_metadata(store, dataset, version)
    if meta is None or meta.get("features") != F.FEATURES:
        return version, None, False
    booster = xgb.Booster()
    booster.load_model(store.fetch_file(f"{dataset}/{version}/model.json"))
    cur_test = apply_categories(test, meta["categories"])
    comparable = cur_test[cur_test["prev_stop_delay_min"].notna()]
    pred = predict_delay(booster.inplace_predict, comparable, meta.get("target_mode", "absolute"))
    return version, mae(comparable[F.LABEL], pred), True


def log_mlflow(tracking_uri: str, dataset: str, params: dict, metrics: dict, importance: dict,
               per_route: pd.DataFrame, model, metadata: dict) -> str:
    import mlflow
    import mlflow.xgboost

    mlflow.set_tracking_uri(tracking_uri)
    name = f"basmy-delay-{dataset}"
    if mlflow.get_experiment_by_name(name) is None:
        # Artifacts sit next to the SQLite DB, so each tracking store (e.g. the
        # Windows dev one and the Airflow container's) has paths it can write.
        if tracking_uri.startswith("sqlite:///"):
            artifacts = Path(tracking_uri[len("sqlite:///"):]).resolve().parent / "artifacts"
        else:
            artifacts = (ROOT / "ml" / "mlruns" / "artifacts").resolve()
        artifacts.mkdir(parents=True, exist_ok=True)
        mlflow.create_experiment(name, artifact_location=artifacts.as_uri())
    mlflow.set_experiment(name)
    with mlflow.start_run(run_name=metadata["version"]) as run:
        mlflow.set_tags({"dataset": dataset.upper(), "label_unit": "minutes"})
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        mlflow.log_metrics({f"importance_gain__{k}": v for k, v in importance.items()})
        mlflow.log_dict(importance, "feature_importance_gain.json")
        mlflow.log_dict(metadata, "metadata.json")
        mlflow.log_text(per_route.to_csv(index=False), "mae_per_route.csv")
        mlflow.xgboost.log_model(model, name="model")
        return run.info.run_id


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Train the bus delay model (baseline vs XGBoost)")
    p.add_argument("--dataset", choices=["real", "synthetic"], required=True)
    p.add_argument("--engine", choices=["duckdb", "bigquery"], default="duckdb")
    p.add_argument("--duckdb-path", help="default: data/<dataset>/warehouse.duckdb")
    p.add_argument("--project", default=os.environ.get("GCP_PROJECT"))
    p.add_argument("--models", default=os.environ.get("BASMY_MODELS", str(ROOT / "data" / "models")),
                   help="models root: local folder or gs://BUCKET/models")
    p.add_argument("--mlflow-uri", default=os.environ.get(
        "MLFLOW_TRACKING_URI", "sqlite:///" + (ROOT / "ml" / "mlruns" / "mlflow.db").as_posix()))
    args = p.parse_args(argv)
    label = args.dataset.upper()

    if args.engine == "duckdb":
        frames = F.read_duckdb(args.duckdb_path or str(ROOT / "data" / args.dataset / "warehouse.duckdb"))
    else:
        frames = F.read_bigquery(args.project, f"basmy_{args.dataset}")
    table = F.build_training_table(frames)

    try:
        span_days = check_history(table, args.dataset)
    except InsufficientHistory as exc:
        print(f"[{label}] {exc}")
        return EXIT_INSUFFICIENT_HISTORY

    train, test, split = time_split(table)
    categories = {c: [str(x) for x in table[c].cat.categories] for c in F.CATEGORICAL}
    train, test = apply_categories(train, categories), apply_categories(test, categories)
    model = train_xgb(train)
    result = evaluate(model, test)

    store = Store(args.models)
    cur_version, cur_mae, comparable = score_current(store, args.dataset, test)
    decision = should_promote(result["mae_xgboost"], cur_mae, min_improvement=MIN_IMPROVEMENT_MIN,
                              comparable=comparable)

    version = datetime.now(timezone.utc).strftime("v%Y%m%dT%H%M%SZ")
    booster = model.get_booster()
    importance = {k: float(v) for k, v in sorted(booster.get_score(importance_type="gain").items(),
                                                 key=lambda kv: -kv[1])}
    metadata = {
        "version": version,
        "dataset": label,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "label": F.LABEL,
        "target_mode": TARGET_MODE,
        "target_note": "prediction = model output + offset; offset = prev_stop_delay_min, else "
                       "route_hour_avg_delay_2w_min, else 0 (all known at prediction time)",
        "features": F.FEATURES,
        "categories": categories,
        "leakage_notes": F.LEAKAGE,
        "split": asdict(split),
        "history_span_days": span_days,
        "mae_minutes": {
            "baseline_prev_stop": result["mae_baseline"],
            "xgboost": result["mae_xgboost"],
            "xgboost_all_test_rows": result["mae_xgboost_all_test_rows"],
            "n_compared_rows": result["n_compared_rows"],
        },
        "xgb_params": XGB_PARAMS,
        "best_iteration": int(model.best_iteration) if getattr(model, "best_iteration", None) is not None else None,
        "promotion": {"previous_current": cur_version, "previous_current_mae_same_window": cur_mae,
                      "promoted": decision.promote, "reason": decision.reason},
    }
    with tempfile.TemporaryDirectory() as tmp:
        model_path = str(Path(tmp) / "model.json")
        booster.save_model(model_path)
        export_model(store, args.dataset, version, model_path, metadata)
    if decision.promote:
        set_current(store, args.dataset, version, decision.reason)

    run_id = log_mlflow(
        args.mlflow_uri, args.dataset,
        params={**{f"xgb_{k}": v for k, v in XGB_PARAMS.items()}, "dataset": label,
                "features": ",".join(F.FEATURES), "train_start": split.train_start, "train_end": split.train_end,
                "test_start": split.test_start, "test_end": split.test_end,
                "train_rows": split.train_rows, "test_rows": split.test_rows},
        metrics={k: result[k] for k in ("mae_baseline", "mae_xgboost", "mae_xgboost_all_test_rows")}
        | {"n_compared_rows": result["n_compared_rows"], "promoted": float(decision.promote)},
        importance=importance, per_route=result["per_route"], model=model, metadata=metadata,
    )

    # --- report -----------------------------------------------------------------------
    b, x = result["mae_baseline"], result["mae_xgboost"]
    gain = 100 * (b - x) / b if b else 0.0
    verdict = (f"XGBoost BEATS the baseline by {gain:.1f}% MAE" if x < b
               else f"XGBoost does NOT beat the baseline ({-gain:.1f}% worse MAE)")
    print(f"\n=== Delay model [{label}] {'(SYNTHETIC DATA - NOT REAL PERFORMANCE)' if label == 'SYNTHETIC' else ''} ===")
    print(f"history: {span_days} days; train {split.train_start}..{split.train_end} ({split.train_rows} rows), "
          f"test {split.test_start}..{split.test_end} ({split.test_rows} rows)")
    print(f"MAE in minutes, on the {result['n_compared_rows']} test rows that have a previous-stop delay:")
    print(f"  {'model':<34}{'MAE (min)':>10}")
    print(f"  {'baseline: delay at previous stop':<34}{b:>10.3f}")
    print(f"  {'XGBoost':<34}{x:>10.3f}")
    print(f"  (XGBoost on all {result['n_test_rows']} test rows, incl. first-detected stops: "
          f"{result['mae_xgboost_all_test_rows']:.3f})")
    print(f"VERDICT [{label}]: {verdict}")
    pr = result["per_route"].copy()
    pr["better"] = np.where(pr["mae_xgboost"] < pr["mae_baseline"], "xgboost", "baseline")
    print(f"\nPer route [{label}]:")
    print(pr.round(3).to_string(index=False))
    print(f"\ntop features by gain: {', '.join(list(importance)[:5])}")
    print(f"exported {args.models}/{args.dataset}/{version}; promotion: {decision.reason}")
    print(f"MLflow run {run_id} ({args.mlflow_uri})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
