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
from pathlib import Path

import psycopg
import requests
from dotenv import load_dotenv

# Fetch/parse/row-extraction logic lives in the shared basmy package so the
# always-on raw collector (collector/) and later pipeline phases use the exact
# same code. Install once from the repo root with `pip install -e .`.
from basmy.gtfs_rt import (  # noqa: F401  (re-exported for existing callers)
    KOTA_BHARU_VEHICLE_POSITIONS_URL as FEED_URL,
    PLAUSIBLE_LAT_RANGE,
    PLAUSIBLE_LON_RANGE,
    extract_row,
    fetch_feed,
    to_utc,
    validate,
)

ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(ENV_PATH)  # local dev only -- in production DATABASE_URL is injected directly

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
