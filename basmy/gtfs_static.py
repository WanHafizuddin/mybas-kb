"""
Load and merge GTFS Static feeds (zip files or extracted folders).

Pure Python (csv module only) so it runs anywhere: inside Spark tasks, in
tests, and later in the FastAPI backend on Render's small free tier.

Why merging exists: the upstream myBAS Kota Bharu GTFS is not stable. The
August 2026 export had 8 routes (weekday + weekend); the November export has
16 routes but only weekend trips and half the shapes. Neither alone covers the
fleet seen in the real-time feed, so callers pass several snapshots, oldest
first, and newer snapshots override older ones trip by trip.

Times: GTFS stop times are "seconds since local midnight of the service day"
and may exceed 24:00:00 for trips that run past midnight. They are kept as
integer seconds here; turning them into instants is done by the caller with
the service date and the agency timezone (Asia/Kuala_Lumpur, no DST).
"""

import csv
import io
import zipfile
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


def parse_gtfs_time(value: str) -> int:
    """'HH:MM:SS' -> seconds since service-day midnight. Hours may be >= 24."""
    h, m, s = value.strip().split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def format_gtfs_time(seconds: int) -> str:
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def parse_gtfs_date(value: str) -> date:
    value = value.strip()
    return date(int(value[:4]), int(value[4:6]), int(value[6:8]))


@dataclass(frozen=True)
class StopTime:
    stop_id: str
    stop_sequence: int
    arrival: int     # seconds since service-day midnight
    departure: int


@dataclass
class Trip:
    trip_id: str
    route_id: str
    service_id: str
    shape_id: str | None
    direction_id: int | None
    headsign: str | None
    stop_times: list[StopTime] = field(default_factory=list)


@dataclass(frozen=True)
class Calendar:
    service_id: str
    days: tuple[bool, ...]  # monday..sunday
    start: date
    end: date


@dataclass
class GtfsStatic:
    agency_timezone: str = "Asia/Kuala_Lumpur"
    routes: dict[str, dict] = field(default_factory=dict)
    stops: dict[str, tuple[float, float]] = field(default_factory=dict)       # stop_id -> (lat, lon)
    shapes: dict[str, list[tuple[float, float]]] = field(default_factory=dict)  # shape_id -> [(lat, lon)]
    trips: dict[str, Trip] = field(default_factory=dict)
    calendars: dict[str, Calendar] = field(default_factory=dict)
    # (service_id, date) -> True (added) / False (removed), from calendar_dates.txt
    calendar_exceptions: dict[tuple[str, date], bool] = field(default_factory=dict)
    sources: list[str] = field(default_factory=list)

    def service_active(self, service_id: str, day: date) -> bool:
        exception = self.calendar_exceptions.get((service_id, day))
        if exception is not None:
            return exception
        cal = self.calendars.get(service_id)
        if cal is None:
            return False
        return cal.start <= day <= cal.end and cal.days[day.weekday()]

    def merge(self, newer: "GtfsStatic") -> "GtfsStatic":
        """Return a new feed: self overridden by `newer`, entity by entity.
        A trip in `newer` replaces the whole trip (including its stop times)."""
        merged = GtfsStatic(agency_timezone=newer.agency_timezone or self.agency_timezone)
        for name in ("routes", "stops", "shapes", "trips", "calendars", "calendar_exceptions"):
            combined = dict(getattr(self, name))
            combined.update(getattr(newer, name))
            setattr(merged, name, combined)
        merged.sources = self.sources + newer.sources
        return merged


# --- loading -----------------------------------------------------------------

class _Source:
    """Read GTFS text files from a zip or a folder, tolerating BOMs, CRLF and
    missing optional files."""

    def __init__(self, path: Path):
        self.path = path
        self.zip = zipfile.ZipFile(path) if path.is_file() else None

    def rows(self, name: str, required: bool = True) -> list[dict]:
        if self.zip is not None:
            members = {Path(n).name: n for n in self.zip.namelist()}
            if name not in members:
                if required:
                    raise FileNotFoundError(f"{name} missing from {self.path}")
                return []
            text = self.zip.read(members[name]).decode("utf-8-sig")
        else:
            file = self.path / name
            if not file.exists():
                if required:
                    raise FileNotFoundError(f"{name} missing from {self.path}")
                return []
            text = file.read_text(encoding="utf-8-sig")
        reader = csv.DictReader(io.StringIO(text, newline=""))
        return [{(k or "").strip(): (v or "").strip() for k, v in row.items()} for row in reader]


def _int_or_none(value: str) -> int | None:
    return int(value) if value not in ("", None) else None


def load_gtfs(path: Path | str) -> GtfsStatic:
    path = Path(path)
    src = _Source(path)
    feed = GtfsStatic(sources=[str(path)])

    agencies = src.rows("agency.txt", required=False)
    if agencies and agencies[0].get("agency_timezone"):
        feed.agency_timezone = agencies[0]["agency_timezone"]

    feed.routes = {r["route_id"]: r for r in src.rows("routes.txt")}
    feed.stops = {s["stop_id"]: (float(s["stop_lat"]), float(s["stop_lon"])) for s in src.rows("stops.txt")}

    shape_points: dict[str, list[tuple[int, float, float]]] = {}
    for p in src.rows("shapes.txt", required=False):
        shape_points.setdefault(p["shape_id"], []).append(
            (int(p["shape_pt_sequence"]), float(p["shape_pt_lat"]), float(p["shape_pt_lon"]))
        )
    feed.shapes = {sid: [(lat, lon) for _, lat, lon in sorted(pts)] for sid, pts in shape_points.items()}

    for t in src.rows("trips.txt"):
        feed.trips[t["trip_id"]] = Trip(
            trip_id=t["trip_id"],
            route_id=t["route_id"],
            service_id=t["service_id"],
            shape_id=t.get("shape_id") or None,
            direction_id=_int_or_none(t.get("direction_id", "")),
            headsign=t.get("trip_headsign") or None,
        )

    for st in src.rows("stop_times.txt"):
        trip = feed.trips.get(st["trip_id"])
        if trip is None:
            continue
        arrival = st.get("arrival_time") or st.get("departure_time")
        departure = st.get("departure_time") or arrival
        if not arrival:
            continue  # untimed stop; GTFS allows it but we cannot score it
        trip.stop_times.append(
            StopTime(st["stop_id"], int(st["stop_sequence"]), parse_gtfs_time(arrival), parse_gtfs_time(departure))
        )
    for trip in feed.trips.values():
        trip.stop_times.sort(key=lambda s: s.stop_sequence)
    feed.trips = {tid: t for tid, t in feed.trips.items() if t.stop_times}

    for c in src.rows("calendar.txt", required=False):
        feed.calendars[c["service_id"]] = Calendar(
            service_id=c["service_id"],
            days=tuple(c[d] == "1" for d in WEEKDAYS),
            start=parse_gtfs_date(c["start_date"]),
            end=parse_gtfs_date(c["end_date"]),
        )
    for cd in src.rows("calendar_dates.txt", required=False):
        feed.calendar_exceptions[(cd["service_id"], parse_gtfs_date(cd["date"]))] = cd["exception_type"] == "1"

    return feed


def load_merged(paths: list[Path | str]) -> GtfsStatic:
    """Load snapshots oldest -> newest and merge them (newest wins)."""
    if not paths:
        raise ValueError("at least one GTFS path is required")
    feed = load_gtfs(paths[0])
    for p in paths[1:]:
        feed = feed.merge(load_gtfs(p))
    return feed
