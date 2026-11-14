"""
Synthetic dataset generator: realistic Kota Bharu bus trips with KNOWN
arrival times and delays, written in exactly the raw format the Phase 1
collector produces, plus a ground-truth file.

Everything this produces is SYNTHETIC. It exists so the pipeline (arrival
detection now; warehouse and model later) can be tested end to end before
enough real history exists. Never report numbers computed from it as real
performance.

What it simulates, per trip instance:
- a start delay and a delay that drifts stop to stop (worse at peak hours and
  in a rain-like "bad day" factor), dwell time at each stop;
- the bus driving along the real route geometry from the GTFS timetable;
- the feed's quirks: ~30 s pings at a random phase, GPS noise, dropped
  pings, the same vehicle timestamp repeated across polls (duplicates),
  occasional far-off-route GPS glitches, and idle pings with no trip_id
  before the trip starts.

Usage:
    python -m transform.synthetic --out data/synthetic --days 3 --trips-per-day 40 --seed 7
"""

import argparse
import gzip
import json
import math
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from basmy.gtfs_static import GtfsStatic, load_merged
from basmy.matching import TripIndex, TripInfo
from collector.spool import partition_path

AGENCY = "mybas-kota-bharu"
FEED_URL = "https://api.data.gov.my/gtfs-realtime/vehicle-position/mybas-kota-bharu"
DEFAULT_GTFS = [
    "data/gtfs_static/mybas-kota-bharu/2026-08-09.zip",
    "data/gtfs_static/mybas-kota-bharu/2026-11-14.zip",
]


@dataclass
class SyntheticConfig:
    ping_interval_s: float = 30.0
    gps_noise_m: float = 6.0
    drop_rate: float = 0.05
    duplicate_rate: float = 0.10       # vehicle timestamp unchanged on the next poll
    glitch_rate: float = 0.02          # ping 300-800 m off route
    idle_minutes_before: int = 10      # idle pings with no trip_id before departure
    dwell_s: tuple[int, int] = (10, 45)


def _peak_factor(local_seconds: int) -> float:
    hour = (local_seconds // 3600) % 24
    return 1.8 if hour in (7, 8, 17, 18) else 1.0


def simulate_trip_timeline(info: TripInfo, rng: random.Random, bad_day: float) -> list[tuple[float, float, float]]:
    """True (arrival_offset_s, departure_offset_s, delay_s) per stop, in
    seconds since service-day midnight."""
    peak = _peak_factor(info.first_arrival)
    delay = rng.gauss(60 * bad_day, 90)
    delay = max(-90.0, delay)
    timeline = []
    prev_departure = None
    for k, sched in enumerate(info.scheduled_arrivals):
        if k > 0:
            delay += rng.gauss(4 * peak * bad_day, 20)
        arrival = sched + delay
        if prev_departure is not None and arrival < prev_departure + 20:
            arrival = prev_departure + 20  # buses cannot teleport
            delay = arrival - sched
        dwell = rng.uniform(10, 45) if 0 < k < len(info.scheduled_arrivals) - 1 else 0.0
        departure = arrival + dwell
        timeline.append((arrival, departure, delay))
        prev_departure = departure
    return timeline


def position_at(info: TripInfo, timeline, offset_s: float) -> float:
    """Distance along the route at a time (seconds since service-day midnight)."""
    if offset_s <= timeline[0][1]:
        return info.stop_distances[0]
    for k in range(len(timeline) - 1):
        _, dep_k, _ = timeline[k]
        arr_next, dep_next, _ = timeline[k + 1]
        if offset_s <= arr_next:
            frac = (offset_s - dep_k) / max(1e-6, arr_next - dep_k)
            d0, d1 = info.stop_distances[k], info.stop_distances[k + 1]
            return d0 + frac * (d1 - d0)
        if offset_s <= dep_next:
            return info.stop_distances[k + 1]
    return info.stop_distances[-1]


def _record(fetched: datetime, vehicle_ts: int, vehicle_id: str, lat: float, lon: float,
            trip_id: str | None, bearing: float) -> dict:
    vehicle = {
        "position": {"latitude": round(lat, 6), "longitude": round(lon, 6), "bearing": round(bearing, 1)},
        "timestamp": str(vehicle_ts),
        "vehicle": {"id": vehicle_id, "label": vehicle_id, "license_plate": vehicle_id},
    }
    if trip_id:
        vehicle["trip"] = {"trip_id": trip_id}
    return {
        "agency": AGENCY,
        "feed_url": FEED_URL,
        "fetched_at": fetched.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "header": {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET",
                   "timestamp": str(int(fetched.timestamp()))},
        "entity": {"id": vehicle_id, "vehicle": vehicle},
        "_synthetic": True,
    }


def generate(
    gtfs: GtfsStatic,
    start: date,
    days: int,
    trips_per_day: int,
    seed: int = 7,
    config: SyntheticConfig | None = None,
    trip_ids: list[str] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Return (raw_records, ground_truth_rows)."""
    config = config or SyntheticConfig()
    rng = random.Random(seed)
    index = TripIndex(gtfs)
    records: list[dict] = []
    truth: list[dict] = []
    fleet = [f"SYN{n:04d}" for n in range(1, 400)]

    for day_n in range(days):
        service_date = start + timedelta(days=day_n)
        active = sorted(
            tid for tid, t in gtfs.trips.items()
            if gtfs.service_active(t.service_id, service_date) and (trip_ids is None or tid in trip_ids)
        )
        if not active:
            continue
        chosen = active if trip_ids is not None else rng.sample(active, min(trips_per_day, len(active)))
        bad_day = rng.choice([1.0, 1.0, 1.0, 2.0])  # a "rainy" day now and then
        day_start = index.service_day_start(service_date)

        for n, trip_id in enumerate(chosen):
            info = index.get(trip_id)
            if info is None:
                continue
            vehicle = fleet[(day_n * 97 + n) % len(fleet)]
            timeline = simulate_trip_timeline(info, rng, bad_day)

            for k, (arr, _dep, delay) in enumerate(timeline):
                truth.append({
                    "agency": AGENCY,
                    "trip_id": trip_id,
                    "route_id": info.route_id,
                    "vehicle_id": vehicle,
                    "service_date": service_date.isoformat(),
                    "stop_id": info.stop_ids[k],
                    "stop_sequence": info.stop_sequences[k],
                    "scheduled_arrival": index.scheduled_instant(service_date, info.scheduled_arrivals[k]).isoformat(),
                    "true_arrival": (day_start + timedelta(seconds=arr)).astimezone(timezone.utc).isoformat(),
                    "true_delay_seconds": round(arr - info.scheduled_arrivals[k], 3),
                })

            # Pings: idle at the first stop without trip_id, then in service.
            t = timeline[0][0] - config.idle_minutes_before * 60 + rng.uniform(0, config.ping_interval_s)
            end = timeline[-1][0] + 60
            last_vehicle_ts = None
            while t <= end:
                fetched = (day_start + timedelta(seconds=t)).astimezone(timezone.utc)
                in_service = t >= timeline[0][0] - 120
                if rng.random() < config.drop_rate:
                    t += config.ping_interval_s
                    continue
                if last_vehicle_ts is not None and rng.random() < config.duplicate_rate:
                    vehicle_ts = last_vehicle_ts  # feed re-sent the previous position
                    lat, lon = last_pos
                else:
                    vehicle_ts = int(fetched.timestamp()) - rng.randint(0, 8)
                    d = position_at(info, timeline, (vehicle_ts - day_start.timestamp()))
                    lat, lon = info.line.point_at(d)
                    if rng.random() < config.glitch_rate:
                        dist, angle = rng.uniform(300, 800), rng.uniform(0, 2 * math.pi)
                        lat, lon = info.line.offset_point(lat, lon, dist * math.cos(angle), dist * math.sin(angle))
                    else:
                        lat, lon = info.line.offset_point(
                            lat, lon, rng.gauss(0, config.gps_noise_m), rng.gauss(0, config.gps_noise_m)
                        )
                records.append(_record(fetched, vehicle_ts, vehicle, lat, lon,
                                       trip_id if in_service else None, rng.uniform(0, 360)))
                last_vehicle_ts, last_pos = vehicle_ts, (lat, lon)
                t += config.ping_interval_s
    return records, truth


def write_raw(records: list[dict], out_dir: Path, part_name: str = "part-synthetic") -> list[Path]:
    """Write records in the collector's layout:
    raw/vehicle_positions/agency=/date=/hour=/<part_name>.jsonl.gz"""
    by_partition: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        fetched = datetime.fromisoformat(r["fetched_at"].replace("Z", "+00:00"))
        by_partition[partition_path(r["agency"], fetched)].append(r)
    written = []
    for part, rows in sorted(by_partition.items()):
        path = out_dir / "raw" / "vehicle_positions" / part / f"{part_name}.jsonl.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        rows.sort(key=lambda r: r["fetched_at"])
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, separators=(",", ":")) + "\n")
        written.append(path)
    return written


def write_truth(truth: list[dict], out_dir: Path) -> Path:
    path = out_dir / "ground_truth" / "arrivals.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for row in truth:
            fh.write(json.dumps(row) + "\n")
    return path


def main(argv=None):
    p = argparse.ArgumentParser(description="Generate SYNTHETIC raw vehicle positions with known arrivals")
    p.add_argument("--gtfs", nargs="+", default=DEFAULT_GTFS, help="GTFS zips/folders, oldest first")
    p.add_argument("--out", default="data/synthetic")
    p.add_argument("--start", default="2026-11-09", help="first service date (YYYY-MM-DD)")
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--trips-per-day", type=int, default=40)
    p.add_argument("--seed", type=int, default=7)
    args = p.parse_args(argv)

    gtfs = load_merged(args.gtfs)
    records, truth = generate(gtfs, date.fromisoformat(args.start), args.days, args.trips_per_day, args.seed)
    out = Path(args.out)
    files = write_raw(records, out)
    truth_path = write_truth(truth, out)
    print(f"SYNTHETIC: {len(records)} raw records in {len(files)} files under {out / 'raw'}; "
          f"{len(truth)} ground-truth arrivals in {truth_path}")


if __name__ == "__main__":
    main()
