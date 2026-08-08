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

from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from dotenv import dotenv_values
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

ENV_PATH = Path(__file__).parent / ".env"


def get_database_url() -> str:
    values = dotenv_values(ENV_PATH)
    url = values.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            f"DATABASE_URL not found in {ENV_PATH}. Copy .env.example to .env and fill it in."
        )
    return url


pool: ConnectionPool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = ConnectionPool(get_database_url(), min_size=1, max_size=5, kwargs={"row_factory": dict_row})
    yield
    pool.close()


app = FastAPI(title="BAS.MY Kota Bharu Realtime API", lifespan=lifespan)

# Read-only public data (bus positions) -- any localhost dev port can read it.
# Tighten this to the real frontend origin once it's deployed somewhere.
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"http://(localhost|127\.0\.0\.1):\d+",
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
def current_vehicles():
    """Latest known position for every vehicle seen at least once."""
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (vehicle_id) *
            FROM vehicle_positions
            WHERE vehicle_id IS NOT NULL
            ORDER BY vehicle_id, collected_at DESC
            """
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
