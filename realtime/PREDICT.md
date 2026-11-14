# `GET /predict-delay` (Phase 5)

```
GET /predict-delay?route_id=30440&stop_id=7590
```

This returns the predicted arrival of the **next bus on that route at that
stop**. The answer is built from live bus positions (the same Supabase table
the map uses), the current weather, and the delay model if one is loaded.

```json
{
  "source": "live_delay",
  "predicted_delay_seconds": 422.4,
  "predicted_arrival": "2026-11-14T13:46:02+00:00",
  "predicted_arrival_local": "2026-11-14T21:46:02+08:00",
  "scheduled_arrival": "2026-11-14T13:39:00+00:00",
  "trip_id": "256_1_WE_3", "vehicle_id": "DEP7355", "service_date": "2026-11-14",
  "stop_sequence": 136, "current_delay_seconds": 422.4,
  "model_version": null, "model_dataset": null, "features": null,
  "note": "model unavailable (BASMY_MODEL_URI not set)"
}
```

## Sources (fallback order)

The endpoint never fails because a model is missing or broken.

| `source` | When it's used | Predicted delay |
|---|---|---|
| `model` | A model is loaded and a live bus is approaching the stop | The model's output. The features follow `ml/features.py`, with the bus's current delay as the last known delay. |
| `live_delay` | No model, or the model errored, and a live bus is approaching | The bus's current delay, carried forward |
| `timetable` | No live bus is approaching | 0 (the next scheduled trip) |
| `none` | Nothing is scheduled for the rest of the day | `null` |

The endpoint returns **404** for an unknown route or stop, and **503** only if
the timetable itself failed to load. The other endpoints are never affected.

**How the live delay is worked out:**

1. Take each live bus seen in the last 15 minutes whose trip, matched by
   `trip_id`, serves this stop.
2. Place it on the trip's route line. Skip it if it's more than 150 m off the
   line or has already passed the stop.
3. Compare the time it was seen with the timetable time at that exact point
   on the route.
4. If several buses qualify, use the one that will arrive soonest.

## Configuration (environment)

| Variable | Default | Meaning |
|---|---|---|
| `BASMY_MODEL_URI` | unset, so no model | Models root: `gs://BUCKET/models` or a local folder. The endpoint loads `<root>/<dataset>/current.json` at startup and keeps it in memory. |
| `BASMY_MODEL_DATASET` | `real` | Which dataset's models to load |
| `BASMY_ALLOW_SYNTHETIC_MODEL` | unset | Must be `1` before a SYNTHETIC model will load. Local demos only. |
| `BASMY_GTFS` | the two snapshots in `data/gtfs_static` | Timetable snapshots, oldest first |

**Memory:** the API uses numpy and `xgboost-cpu` only, with no pandas, so it
fits Render's free 512 MB.

## Prediction log

Every response with a predicted arrival is appended to **`delay_predictions`**
([`schema_predictions.sql`](schema_predictions.sql)). Until that table
exists, the API logs one warning and stops trying, and responses are
unaffected.

The daily Airflow task `monitor_predictions` (`ml/monitor.py`) then:

1. joins the logged predictions to the arrivals that were actually detected,
2. works out the daily MAE for each source,
3. logs a **WARNING** if the model's MAE is more than 30% worse than its test
   MAE.

## Deploying (Render)

`requirements.txt` now installs the repo's shared packages (`-e ..`) plus
numpy and `xgboost-cpu`. It must be installed from `realtime/`, which is what
Render does. On the first deploy after this change, check that the build
log installs `basmy` and that `/health` and `/vehicles/current` still
respond.

No real model exists yet: real training needs 4 weeks of history, from
about 12 December 2026. Deploy **without** `BASMY_MODEL_URI` and the
endpoint serves `live_delay` and `timetable`.
