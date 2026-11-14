"""
Backend API for BAS.MY Kota Bharu real-time vehicle positions.

Serves what the collector has actually stored in vehicle_positions -- no
trip/route/ETA endpoints yet, because we don't have trip/route data in the
feed yet (see realtime/inspect_feed.py findings). Once that changes, or once
ETA prediction exists, this API grows to match -- not before.

Run:
    uvicorn api:app --reload --port 8000

Then browse http://localhost:8000/docs for interactive API docs.
"""

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(ENV_PATH)  # local dev only -- in production DATABASE_URL is injected directly


def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            f"DATABASE_URL not set. Locally: copy {ENV_PATH.name}.example to {ENV_PATH.name} and fill it "
            "in. In production: set it as an environment variable on the host."
        )
    return url


pool: ConnectionPool | None = None
log = logging.getLogger("api")
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_GTFS = [
    REPO_ROOT / "data/gtfs_static/mybas-kota-bharu/2026-08-09.zip",
    REPO_ROOT / "data/gtfs_static/mybas-kota-bharu/2026-11-14.zip",
]


class ServingState:
    """Everything /predict-delay needs, loaded once at startup. Any piece
    can be missing: no model -> fallbacks; no GTFS -> only this endpoint
    returns 503; the existing endpoints are never affected."""

    def __init__(self):
        self.index = None
        self.weather = None
        self.model = None
        self.model_error: str | None = None
        self.error: str | None = None
        self.log_predictions = True

    @classmethod
    def load(cls) -> "ServingState":
        state = cls()
        try:
            from basmy.gtfs_static import load_merged
            from basmy.matching import TripIndex
            from basmy.serving import CurrentWeather

            gtfs_paths = os.environ.get("BASMY_GTFS", "").split() or DEFAULT_GTFS
            state.index = TripIndex(load_merged(gtfs_paths))
            state.weather = CurrentWeather()
        except Exception as exc:  # noqa: BLE001
            state.error = f"{type(exc).__name__}: {exc}"
            log.error("prediction serving disabled: %s", state.error)
            return state
        models_uri = os.environ.get("BASMY_MODEL_URI")
        if not models_uri:
            state.model_error = "BASMY_MODEL_URI not set"
        else:
            try:
                from basmy.serving import DelayModel

                state.model = DelayModel.load(
                    models_uri,
                    dataset=os.environ.get("BASMY_MODEL_DATASET", "real"),
                    allow_synthetic=os.environ.get("BASMY_ALLOW_SYNTHETIC_MODEL") == "1",
                )
                log.info("loaded delay model %s (%s)", state.model.version, state.model.dataset)
            except Exception as exc:  # noqa: BLE001 -- missing model = fallbacks, never a crash
                state.model_error = f"{type(exc).__name__}: {exc}"
                log.warning("no delay model, using fallbacks: %s", state.model_error)
        return state


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = ConnectionPool(get_database_url(), min_size=1, max_size=5, kwargs={"row_factory": dict_row})
    app.state.serving = ServingState.load()
    yield
    pool.close()


app = FastAPI(title="BAS.MY Kota Bharu Realtime API", lifespan=lifespan)

# Read-only public data (bus positions) -- fine to allow broadly. Covers local
# dev (any localhost port) plus the two deployed frontends.
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"http://(localhost|127\.0\.0\.1):\d+"
    r"|https://kb-mybastracker-simulatedlive\.onrender\.com"
    r"|https://kb-my-bas-tracker-simulated-live\.vercel\.app",
    allow_methods=["GET"],
    allow_headers=["*"],
)


class VehiclePosition(BaseModel):
    id: int
    entity_id: str
    vehicle_id: str | None
    vehicle_label: str | None
    license_plate: str | None
    latitude: float
    longitude: float
    bearing: float | None
    speed: float | None
    trip_id: str | None
    route_id: str | None
    direction_id: int | None
    current_stop_sequence: int | None
    stop_id: str | None
    current_status: str | None
    congestion_level: str | None
    occupancy_status: str | None
    occupancy_percentage: int | None
    vehicle_timestamp: datetime | None
    feed_header_timestamp: datetime | None
    collected_at: datetime


@app.get("/health")
def health():
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/vehicles/current", response_model=list[VehiclePosition])
def current_vehicles(
    max_age_minutes: int = Query(
        default=15,
        ge=1,
        le=1440,
        description="Only return vehicles whose latest position was collected within "
        "this many minutes. Bounds the result to recently-active buses.",
    ),
):
    """Latest known position for every vehicle collected within the recency window.

    Without a window this returns the last-known position of every vehicle ever
    seen, so buses that ran earlier and parked pile up as stale grey markers on
    the map (the frontend greys anything older than 5 min). The window bounds the
    result to vehicles active in the last `max_age_minutes`. Recently-idle buses
    (5..max_age_minutes old) still come through and render grey; older ghosts are
    dropped entirely.

    Freshness is measured by `collected_at` (when our collector polled), not
    `vehicle_timestamp` -- the latter can lag or be null while a bus idles, so it
    is not a reliable "did we still see this bus" signal.
    """
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (vehicle_id) *
            FROM vehicle_positions
            WHERE vehicle_id IS NOT NULL
              AND collected_at > now() - %(max_age_minutes)s * interval '1 minute'
            ORDER BY vehicle_id, collected_at DESC
            """,
            {"max_age_minutes": max_age_minutes},
        )
        return cur.fetchall()


@app.get("/vehicles/{vehicle_id}/history", response_model=list[VehiclePosition])
def vehicle_history(
    vehicle_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
):
    """Most recent positions for one vehicle, newest first."""
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM vehicle_positions
            WHERE vehicle_id = %s
            ORDER BY collected_at DESC
            LIMIT %s
            """,
            (vehicle_id, limit),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"no positions found for vehicle_id={vehicle_id}")
    return rows


# --- delay prediction (Phase 5) ---------------------------------------------------------------

LIVE_WINDOW_MINUTES = 15

INSERT_PREDICTION_SQL = """
    INSERT INTO delay_predictions (
        route_id, stop_id, trip_id, vehicle_id, service_date, stop_sequence,
        scheduled_arrival, predicted_arrival, predicted_delay_seconds, current_delay_seconds,
        source, model_version, model_dataset, features
    ) VALUES (
        %(route_id)s, %(stop_id)s, %(trip_id)s, %(vehicle_id)s, %(service_date)s, %(stop_sequence)s,
        %(scheduled_arrival)s, %(predicted_arrival)s, %(predicted_delay_seconds)s, %(current_delay_seconds)s,
        %(source)s, %(model_version)s, %(model_dataset)s, %(features)s
    )
"""


def get_serving() -> "ServingState | None":
    return getattr(app.state, "serving", None)


def get_live_vehicles():
    """Latest position per vehicle seen in the last 15 minutes (same window
    as /vehicles/current)."""
    from basmy.serving import LiveVehicle

    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (vehicle_id) vehicle_id, trip_id, latitude, longitude,
                   coalesce(vehicle_timestamp, collected_at) AS seen_at
            FROM vehicle_positions
            WHERE vehicle_id IS NOT NULL
              AND collected_at > now() - %(m)s * interval '1 minute'
            ORDER BY vehicle_id, collected_at DESC
            """,
            {"m": LIVE_WINDOW_MINUTES},
        )
        return [LiveVehicle(r["vehicle_id"], r["trip_id"], r["latitude"], r["longitude"], r["seen_at"].timestamp())
                for r in cur.fetchall()]


def log_prediction(body: dict, state: "ServingState") -> None:
    """Append the prediction to delay_predictions. Logging must never break
    the endpoint: if the table does not exist yet, warn once and stop trying."""
    if not state.log_predictions or body.get("predicted_arrival") is None:
        return
    import psycopg

    row = {k: body.get(k) for k in (
        "route_id", "stop_id", "trip_id", "vehicle_id", "service_date", "stop_sequence", "scheduled_arrival",
        "predicted_arrival", "predicted_delay_seconds", "current_delay_seconds", "source",
        "model_version", "model_dataset")}
    row["features"] = json.dumps(body.get("features")) if body.get("features") is not None else None
    try:
        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute(INSERT_PREDICTION_SQL, row)
    except psycopg.errors.UndefinedTable:
        state.log_predictions = False
        log.warning("delay_predictions table does not exist; prediction logging disabled "
                    "(apply realtime/schema_predictions.sql)")
    except Exception as exc:  # noqa: BLE001
        log.warning("could not log prediction: %s", exc)


@app.get("/predict-delay")
def predict_delay(
    background_tasks: BackgroundTasks,
    route_id: str = Query(..., description="GTFS route_id, e.g. 30425"),
    stop_id: str = Query(..., description="GTFS stop_id, e.g. 7496"),
    state=Depends(get_serving),
    vehicles=Depends(get_live_vehicles),
):
    """Predicted arrival of the next bus on `route_id` at `stop_id`.

    `source` says how it was made, in fallback order: `model` (delay model on
    live features), `live_delay` (no model: the bus's current delay carried
    forward), `timetable` (no live bus approaching: the next scheduled trip).
    A missing model never makes this endpoint fail.
    """
    from basmy.serving import predict

    if state is None or state.index is None:
        raise HTTPException(status_code=503, detail="prediction serving unavailable")
    try:
        body = predict(state.index, route_id, stop_id, vehicles, datetime.now(timezone.utc),
                       model=state.model, weather=state.weather)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc.args[0]))
    if body["source"] != "model" and state.model is None:
        reason = f"model unavailable ({state.model_error})"
        body["note"] = f"{body['note']}; {reason}" if body.get("note") else reason
    background_tasks.add_task(log_prediction, body, state)
    return body
