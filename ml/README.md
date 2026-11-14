# Phase 4: delay model

The model predicts the **delay (in minutes)** at a stop for a bus whose
recent position we know.

```
warehouse (fact_arrivals, dim_stop, weather) -> features.py -> split.py (time-based)
  -> baseline vs XGBoost -> MLflow (SQLite) -> models/<dataset>/<version>/ -> promote if better
```

## Training table: `features.py`

There is one row per trip instance and stop. Every feature has to be known
at prediction time; `LEAKAGE` in the code records why each one is.

| Feature | How it's built | Leakage note |
|---|---|---|
| `route_id`, `stop_id`, `stop_sequence` | timetable | static |
| `hour`, `day_of_week`, `is_weekend` | **scheduled** arrival time, Kuala Lumpur | The actual arrival hour would give away the answer, so it isn't used. |
| `prev_stop_delay_min`, `stops_since_prev` | last stop detected earlier on the same trip instance | The bus has already passed it. Empty when there isn't one. |
| `route_hour_avg_delay_2w_min` | route × scheduled hour, service dates d−14 to d−1 | Never the same day. |
| `rain_mm`, `temperature_c` | weather in the hour the bus was **last seen** | **Flagged.** The weather in the arrival hour is in the future when we predict, so it isn't used. Rows with no previous stop use the scheduled hour, which in serving means a forecast. |
| **label** `delay_min` | actual − scheduled | — |

## Model

- **Split:** whole service dates. The oldest ~80% train and the newest ~20%
  test. Early stopping uses the newest 10% of the training dates.
- **Baseline:** delay = the delay at the previous stop.
- **XGBoost:**
  - native categoricals for route and stop, optimising MAE.
  - **Target:** the *change* in delay since the last known delay. That last
    known delay is the previous stop's delay; if there isn't one, the
    route × hour two-week average; failing that, 0. Trees can't extrapolate,
    so predicting the absolute delay capped out on long routes where delays
    keep growing. On synthetic data that version lost to the baseline by
    35%, and the residual form fixed it.
- **Scoring:** both models are scored on the **same** test rows, those that
  have a previous-stop delay. MAE is reported overall and per route.
- **Real-data guard:** on real data the script refuses to train with less
  than **28 days** of history. It prints why and exits with code 99, which
  the weekly DAG treats as *skipped*.

## Tracking, export and promotion

- **MLflow:** SQLite at `ml/mlruns/mlflow.db`, with artifacts in
  `ml/mlruns/artifacts` (both gitignored). Each run logs the parameters,
  MAE metrics, gain feature importance, per-route MAE as CSV, the metadata
  and the model. To browse the runs:

  ```bash
  mlflow ui --backend-store-uri sqlite:///ml/mlruns/mlflow.db
  ```

- **Export:** each run writes `<models>/<dataset>/<version>/model.json` and
  `metadata.json`. The metadata records the training and test windows, MAE,
  the feature list, the category lists, the target mode and the leakage
  notes. `<models>` is `data/models` locally, or `gs://BUCKET/models` with
  `--models`.
- **Promotion:** `<models>/<dataset>/current.json` points to the model that
  serving uses. A new model replaces it only if its MAE beats the current
  model's MAE, **re-measured on the new test window**, by more than 0.005 min
  (0.3 s). Smaller differences are numerical noise. Real and
  synthetic models are kept in separate folders, so a synthetic model can
  never become the current real model.

## Run it

Install the ML dependencies:

```bash
pip install -e ".[ml,warehouse]"
```

Train on the synthetic warehouse; this always runs:

```bash
python -m ml.train --dataset synthetic
```

Train on the real warehouse; this refuses until there are 4 weeks of history:

```bash
python -m ml.train --dataset real
```

The weekly retrain runs as the `basmy_weekly_retrain` Airflow DAG on Mondays
at 21:00 UTC: `dbt build`, then train, then promote if better.

## Results so far

The only results are on **SYNTHETIC** data. They are not real-world
performance.

The run used 6 weeks (28 Sep to 8 Nov 2026), trained on 28 Sep to 31 Oct
(91,013 rows) and tested on 1 to 8 Nov (22,405 compared rows).

| Model | MAE (min) |
|---|---|
| baseline: delay at previous stop | 0.291 |
| XGBoost | **0.261** (10.3% better; better on all 16 routes) |

The synthetic delays were generated from route, peak-hour, weekend and
real-rain effects plus noise, so the model is recovering effects that were
put there on purpose. That proves the pipeline works, not that the model
will beat the baseline on real buses.

On real data the script refuses to train: the warehouse covers 4 days.
Collection on the VM started on 14 November 2026, so real training becomes
possible from about 12 December.
