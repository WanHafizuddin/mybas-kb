"""
Periodically poll the BAS.MY Kota Bharu GTFS-Realtime vehicle-position feed
and append every observation to PostgreSQL. Never overwrites -- each poll of
each vehicle becomes its own row, so history accumulates for later ETA and
delay modeling.

Usage:
    python collector.py                 # poll forever, every 30s
    python collector.py --interval 15   # poll forever, every 15s
    python collector.py --once          # single poll, then exit
    python collector.py --max-iterations 5   # poll 5 times, then exit

Requires realtime/.env with DATABASE_URL (see .env.example) and the
vehicle_positions table (see schema.sql).
"""

import argparse
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2

FEED_URL = "https://api.data.gov.my/gtfs-realtime/vehicle-position/mybas-kota-bharu"
ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(ENV_PATH)  # local dev only -- in production DATABASE_URL is injected directly

# Loose bounding box around Kelantan/Kota Bharu. Positions outside this are
# logged as a warning (data quality signal worth keeping) but still stored --
# we don't have grounds to assume they're invalid, only unusual.
PLAUSIBLE_LAT_RANGE = (4.5, 7.0)
PLAUSIBLE_LON_RANGE = (100.5, 103.0)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("collector")


def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            f"DATABASE_URL not set. Locally: copy {ENV_PATH.name}.example to {ENV_PATH.name} and fill it "
            "in. In production: set it as an environment variable on the host."
        )
    return url


def fetch_feed() -> gtfs_realtime_pb2.FeedMessage:
    response = requests.get(FEED_URL, timeout=10)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def to_utc(epoch_seconds) -> datetime | None:
    if not epoch_seconds:
        return None
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


def extract_row(entity, feed_header_timestamp) -> dict:
    vehicle = entity.vehicle
    row = {"entity_id": entity.id, "feed_header_timestamp": feed_header_timestamp}

    if vehicle.HasField("vehicle"):
        vd = vehicle.vehicle
        row["vehicle_id"] = vd.id if vd.HasField("id") else None
        row["vehicle_label"] = vd.label if vd.HasField("label") else None
        row["license_plate"] = vd.license_plate if vd.HasField("license_plate") else None
    else:
        row["vehicle_id"] = row["vehicle_label"] = row["license_plate"] = None

    pos = vehicle.position
    row["latitude"] = pos.latitude
    row["longitude"] = pos.longitude
    row["bearing"] = pos.bearing if pos.HasField("bearing") else None
    row["speed"] = pos.speed if pos.HasField("speed") else None

    if vehicle.HasField("trip"):
        trip = vehicle.trip
        row["trip_id"] = trip.trip_id if trip.HasField("trip_id") else None
        row["route_id"] = trip.route_id if trip.HasField("route_id") else None
        row["direction_id"] = trip.direction_id if trip.HasField("direction_id") else None
    else:
        row["trip_id"] = row["route_id"] = row["direction_id"] = None

    row["current_stop_sequence"] = (
        vehicle.current_stop_sequence if vehicle.HasField("current_stop_sequence") else None
    )
    row["stop_id"] = vehicle.stop_id if vehicle.HasField("stop_id") else None
    row["current_status"] = (
        gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(vehicle.current_status)
        if vehicle.HasField("current_status") else None
    )
    row["congestion_level"] = (
        gtfs_realtime_pb2.VehiclePosition.CongestionLevel.Name(vehicle.congestion_level)
        if vehicle.HasField("congestion_level") else None
    )
    row["occupancy_status"] = (
        gtfs_realtime_pb2.VehiclePosition.OccupancyStatus.Name(vehicle.occupancy_status)
        if vehicle.HasField("occupancy_status") else None
    )
    row["occupancy_percentage"] = (
        vehicle.occupancy_percentage if vehicle.HasField("occupancy_percentage") else None
    )
    row["vehicle_timestamp"] = to_utc(vehicle.timestamp if vehicle.HasField("timestamp") else None)

    return row


def validate(row: dict) -> list[str]:
    """Return a list of warnings. Never raises -- we store what the feed
    gives us and flag anomalies rather than silently discarding data."""
    warnings = []
    lat, lon = row["latitude"], row["longitude"]
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        warnings.append(f"lat/lon out of Earth bounds: {lat}, {lon}")
    elif not (PLAUSIBLE_LAT_RANGE[0] <= lat <= PLAUSIBLE_LAT_RANGE[1]) or not (
        PLAUSIBLE_LON_RANGE[0] <= lon <= PLAUSIBLE_LON_RANGE[1]
    ):
        warnings.append(f"lat/lon outside plausible Kelantan bounding box: {lat}, {lon}")
    return warnings


INSERT_SQL = """
    INSERT INTO vehicle_positions (
        entity_id, vehicle_id, vehicle_label, license_plate,
        latitude, longitude, bearing, speed,
        trip_id, route_id, direction_id, current_stop_sequence, stop_id,
        current_status, congestion_level, occupancy_status, occupancy_percentage,
        vehicle_timestamp, feed_header_timestamp
    ) VALUES (
        %(entity_id)s, %(vehicle_id)s, %(vehicle_label)s, %(license_plate)s,
        %(latitude)s, %(longitude)s, %(bearing)s, %(speed)s,
        %(trip_id)s, %(route_id)s, %(direction_id)s, %(current_stop_sequence)s, %(stop_id)s,
        %(current_status)s, %(congestion_level)s, %(occupancy_status)s, %(occupancy_percentage)s,
        %(vehicle_timestamp)s, %(feed_header_timestamp)s
    )
"""


def poll_once(conn) -> int:
    feed = fetch_feed()
    header_ts = to_utc(feed.header.timestamp)
    vehicles = [e for e in feed.entity if e.HasField("vehicle")]

    rows = []
    for entity in vehicles:
        row = extract_row(entity, header_ts)
        for warning in validate(row):
            log.warning("%s: %s", row["entity_id"], warning)
        rows.append(row)

    with conn.cursor() as cur:
        for row in rows:
            cur.execute(INSERT_SQL, row)
    conn.commit()

    log.info("polled feed: %d vehicles, %d rows inserted", len(vehicles), len(rows))
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=float, default=30.0, help="seconds between polls")
    parser.add_argument("--once", action="store_true", help="poll a single time and exit")
    parser.add_argument("--max-iterations", type=int, default=None, help="stop after N polls")
    args = parser.parse_args()

    database_url = get_database_url()
    conn = psycopg.connect(database_url)
    log.info("connected to database")

    iterations = 0
    try:
        while True:
            try:
                poll_once(conn)
            except requests.RequestException as exc:
                log.error("feed fetch failed: %s", exc)
            except psycopg.Error as exc:
                log.error("database error: %s -- reconnecting", exc)
                try:
                    conn.close()  # best-effort; connection may already be dead
                except Exception:
                    pass
                try:
                    conn = psycopg.connect(database_url)
                    log.info("reconnected to database")
                except psycopg.Error as reconnect_exc:
                    log.error("reconnect failed: %s -- will retry next poll", reconnect_exc)

            iterations += 1
            if args.once or (args.max_iterations and iterations >= args.max_iterations):
                break
            time.sleep(args.interval)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
