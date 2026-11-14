"""
Export REAL Kota Bharu history from the Supabase vehicle_positions table into
the Phase 1 raw layout, so the arrival job can run on real pings before the
always-on collector has built up its own history.

Read-only: opens a read-only transaction and only runs SELECT. The Supabase
table keeps the 19 flattened columns, not the full protobuf, so each record
is rebuilt with the fields the table has. `fetched_at` is the table's
`collected_at`.

Usage (needs realtime/.env with DATABASE_URL):
    python -m transform.export_supabase --out data/real --from 2026-08-09 --to 2026-08-13
"""

import argparse
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from transform.synthetic import AGENCY, FEED_URL, write_raw

KL = ZoneInfo("Asia/Kuala_Lumpur")

QUERY = """
    SELECT entity_id, vehicle_id, vehicle_label, license_plate, latitude, longitude, bearing, speed,
           trip_id, route_id, direction_id, current_stop_sequence, stop_id,
           vehicle_timestamp, feed_header_timestamp, collected_at
    FROM vehicle_positions
    WHERE collected_at >= %(start)s AND collected_at < %(end)s
    ORDER BY collected_at
"""


def _epoch(ts: datetime | None) -> str | None:
    return str(int(ts.timestamp())) if ts else None


def row_to_record(row: dict) -> dict:
    vehicle: dict = {"position": {"latitude": row["latitude"], "longitude": row["longitude"]}}
    if row["bearing"] is not None:
        vehicle["position"]["bearing"] = row["bearing"]
    if row["speed"] is not None:
        vehicle["position"]["speed"] = row["speed"]
    trip = {k: row[k] for k in ("trip_id", "route_id", "direction_id") if row[k] is not None}
    if trip:
        vehicle["trip"] = trip
    descriptor = {k_out: row[k_in] for k_in, k_out in
                  (("vehicle_id", "id"), ("vehicle_label", "label"), ("license_plate", "license_plate"))
                  if row[k_in] is not None}
    if descriptor:
        vehicle["vehicle"] = descriptor
    if row["vehicle_timestamp"] is not None:
        vehicle["timestamp"] = _epoch(row["vehicle_timestamp"])
    if row["current_stop_sequence"] is not None:
        vehicle["current_stop_sequence"] = row["current_stop_sequence"]
    if row["stop_id"] is not None:
        vehicle["stop_id"] = row["stop_id"]

    header = {"gtfs_realtime_version": "2.0"}
    if row["feed_header_timestamp"] is not None:
        header["timestamp"] = _epoch(row["feed_header_timestamp"])
    collected = row["collected_at"].astimezone(timezone.utc)
    return {
        "agency": AGENCY,
        "feed_url": FEED_URL,
        "fetched_at": collected.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "header": header,
        "entity": {"id": row["entity_id"], "vehicle": vehicle},
        "_source": "supabase.vehicle_positions",
    }


def main(argv=None):
    p = argparse.ArgumentParser(description="Export real KB history from Supabase (read-only) to the raw layout")
    p.add_argument("--out", default="data/real")
    p.add_argument("--from", dest="start", required=True, help="first local (KL) date, YYYY-MM-DD")
    p.add_argument("--to", dest="end", required=True, help="last local (KL) date, inclusive")
    args = p.parse_args(argv)

    import psycopg
    from dotenv import load_dotenv
    from psycopg.rows import dict_row

    load_dotenv(Path(__file__).resolve().parent.parent / "realtime" / ".env")
    start = datetime.combine(date.fromisoformat(args.start), time(0), tzinfo=KL)
    end = datetime.combine(date.fromisoformat(args.end) + timedelta(days=1), time(0), tzinfo=KL)

    with psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row) as conn:
        conn.read_only = True
        with conn.cursor() as cur:
            cur.execute(QUERY, {"start": start, "end": end})
            records = [row_to_record(r) for r in cur]

    files = write_raw(records, Path(args.out), part_name="part-supabase-export")
    print(f"REAL: exported {len(records)} rows ({args.start}..{args.end} KL) into {len(files)} files under {args.out}/raw")


if __name__ == "__main__":
    main()
