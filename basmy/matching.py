"""
Shared matching logic: GTFS-Realtime pings -> trip -> position along the
route -> arrival time at each stop.

Used by the Spark arrival-detection job (transform/) and, in Phase 5, by the
FastAPI /predict-delay endpoint, so a ping is interpreted the same way in
training and in serving.

This is the Python counterpart of buildTripIndex() in src/utils/gtfs.js: the
Kota Bharu feed reports trip_id during service but never route_id, so every
ping is joined to the static timetable through trip_id.

Geometry is a local equirectangular projection around each route's first
point -- metre-accurate over a ~100 km bus route at 6 degrees N and pure
Python, so it runs inside Spark workers and on a 512 MB web dyno alike.
"""

import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from basmy.gtfs_static import GtfsStatic, Trip

EARTH_RADIUS_M = 6_371_000.0

# Defaults used by the arrival job; all overridable per call.
MAX_OFFSET_M = 100.0        # pings further than this from the route are ignored
MAX_GAP_S = 600             # no arrival is interpolated across a gap longer than this
ARRIVE_TOLERANCE_M = 20.0   # "arrived" = within this distance before the stop along the route
SHAPE_TRUST_M = 150.0       # if any stop lies further than this from the shape, the shape is wrong
MAX_SPEED_MPS = 30.0        # 108 km/h; bounds how far ahead the next ping is searched
SERVICE_WINDOW_BEFORE_S = 3600   # a ping may belong to a trip from 1 h before its first stop...
SERVICE_WINDOW_AFTER_S = 5400    # ...until 1.5 h after its last scheduled stop


# --- geometry ------------------------------------------------------------------

class Polyline:
    """A route line in local metres with cumulative distance at each vertex."""

    def __init__(self, points: list[tuple[float, float]]):
        if len(points) < 2:
            raise ValueError("a polyline needs at least 2 points")
        self.lat0 = points[0][0]
        self.lon0 = points[0][1]
        self._kx = math.cos(math.radians(self.lat0)) * EARTH_RADIUS_M * math.pi / 180
        self._ky = EARTH_RADIUS_M * math.pi / 180
        self.xy = [self.to_xy(lat, lon) for lat, lon in points]
        self.cum = [0.0]
        for (x1, y1), (x2, y2) in zip(self.xy, self.xy[1:]):
            self.cum.append(self.cum[-1] + math.hypot(x2 - x1, y2 - y1))
        self.length = self.cum[-1]

    def to_xy(self, lat: float, lon: float) -> tuple[float, float]:
        return (lon - self.lon0) * self._kx, (lat - self.lat0) * self._ky

    def project(self, lat: float, lon: float, lo: float = -math.inf, hi: float = math.inf) -> tuple[float, float]:
        """Closest point on the line, restricted to segments overlapping
        [lo, hi] metres along it. Returns (distance_along_m, offset_m)."""
        px, py = self.to_xy(lat, lon)
        best_d, best_off = 0.0, math.inf
        for i in range(len(self.xy) - 1):
            c1, c2 = self.cum[i], self.cum[i + 1]
            if c2 < lo or c1 > hi:
                continue
            (x1, y1), (x2, y2) = self.xy[i], self.xy[i + 1]
            dx, dy = x2 - x1, y2 - y1
            seg2 = dx * dx + dy * dy
            t = 0.0 if seg2 == 0 else max(0.0, min(1.0, ((px - x1) * dx + (py - y1) * dy) / seg2))
            off = math.hypot(px - (x1 + t * dx), py - (y1 + t * dy))
            if off < best_off:
                best_off, best_d = off, c1 + t * (c2 - c1)
        return best_d, best_off

    def point_at(self, distance: float) -> tuple[float, float]:
        """(lat, lon) at a distance along the line (clamped to its ends)."""
        distance = max(0.0, min(self.length, distance))
        for i in range(len(self.cum) - 1):
            if self.cum[i + 1] >= distance:
                span = self.cum[i + 1] - self.cum[i]
                t = 0.0 if span == 0 else (distance - self.cum[i]) / span
                x = self.xy[i][0] + t * (self.xy[i + 1][0] - self.xy[i][0])
                y = self.xy[i][1] + t * (self.xy[i + 1][1] - self.xy[i][1])
                return self.lat0 + y / self._ky, self.lon0 + x / self._kx
        return self.point_at(self.length)

    def offset_point(self, lat: float, lon: float, east_m: float, north_m: float) -> tuple[float, float]:
        return lat + north_m / self._ky, lon + east_m / self._kx


# --- trip index ------------------------------------------------------------------

@dataclass
class TripInfo:
    trip_id: str
    route_id: str
    service_id: str
    stop_ids: list[str]
    stop_sequences: list[int]
    scheduled_arrivals: list[int]   # seconds since service-day midnight (may exceed 86400)
    line: Polyline
    stop_distances: list[float]     # metres along `line`, non-decreasing
    geometry_source: str            # "shape" or "stops" (shape missing or not trustworthy)

    @property
    def first_arrival(self) -> int:
        return self.scheduled_arrivals[0]

    @property
    def last_arrival(self) -> int:
        return self.scheduled_arrivals[-1]


def _stop_distances(line: Polyline, stop_points: list[tuple[float, float]]) -> tuple[list[float], float]:
    """Project stops onto the line in order, never moving backwards, so a
    route that loops back on itself still gets increasing stop distances."""
    distances, worst, prev = [], 0.0, 0.0
    for lat, lon in stop_points:
        d, off = line.project(lat, lon, lo=prev)
        d = max(d, prev)
        distances.append(d)
        worst = max(worst, off)
        prev = d
    return distances, worst


class TripIndex:
    """trip_id -> TripInfo, built lazily from a (merged) GtfsStatic."""

    def __init__(self, gtfs: GtfsStatic):
        self.gtfs = gtfs
        self.tz = ZoneInfo(gtfs.agency_timezone)
        self._cache: dict[str, TripInfo | None] = {}
        self._geometry_cache: dict[tuple, tuple[Polyline, list[float], str]] = {}

    def __contains__(self, trip_id: str) -> bool:
        return self.get(trip_id) is not None

    def get(self, trip_id: str | None) -> TripInfo | None:
        if not trip_id:
            return None
        if trip_id not in self._cache:
            trip = self.gtfs.trips.get(trip_id)
            self._cache[trip_id] = self._build(trip) if trip is not None else None
        return self._cache[trip_id]

    def _build(self, trip: Trip) -> TripInfo | None:
        stop_ids = [st.stop_id for st in trip.stop_times]
        if any(s not in self.gtfs.stops for s in stop_ids) or len(stop_ids) < 2:
            return None
        key = (trip.shape_id, tuple(stop_ids))
        if key not in self._geometry_cache:
            stop_points = [self.gtfs.stops[s] for s in stop_ids]
            geometry = None
            shape = self.gtfs.shapes.get(trip.shape_id) if trip.shape_id else None
            if shape and len(shape) >= 2:
                line = Polyline(shape)
                dists, worst = _stop_distances(line, stop_points)
                if worst <= SHAPE_TRUST_M:
                    geometry = (line, dists, "shape")
            if geometry is None:
                # Shape missing, or its stops are far off it: the stop
                # sequence itself is the best route line we have.
                line = Polyline(stop_points)
                dists, _ = _stop_distances(line, stop_points)
                geometry = (line, dists, "stops")
            self._geometry_cache[key] = geometry
        line, dists, source = self._geometry_cache[key]
        return TripInfo(
            trip_id=trip.trip_id,
            route_id=trip.route_id,
            service_id=trip.service_id,
            stop_ids=stop_ids,
            stop_sequences=[st.stop_sequence for st in trip.stop_times],
            scheduled_arrivals=[st.arrival for st in trip.stop_times],
            line=line,
            stop_distances=dists,
            geometry_source=source,
        )

    # --- time ---------------------------------------------------------------

    def service_day_start(self, service_date: date) -> datetime:
        """The instant GTFS times count from: local midnight of the service
        date (exact for Asia/Kuala_Lumpur, which has no DST)."""
        return datetime.combine(service_date, time(0), tzinfo=self.tz)

    def scheduled_instant(self, service_date: date, seconds: int) -> datetime:
        return (self.service_day_start(service_date) + timedelta(seconds=seconds)).astimezone(timezone.utc)

    def resolve_service_date(self, info: TripInfo, epoch_s: float) -> date | None:
        """Which service date does a ping of this trip at this instant belong
        to? Candidates are the local date and the day before (for trips
        running past midnight, whose times exceed 24:00:00). The service must
        run that day and the ping must fall inside the trip's time window."""
        local = datetime.fromtimestamp(epoch_s, tz=self.tz)
        best: tuple[float, date] | None = None
        for back in (0, 1):
            day = local.date() - timedelta(days=back)
            if not self.gtfs.service_active(info.service_id, day):
                continue
            s = (local - self.service_day_start(day)).total_seconds()
            if info.first_arrival - SERVICE_WINDOW_BEFORE_S <= s <= info.last_arrival + SERVICE_WINDOW_AFTER_S:
                # Distance from the scheduled span; 0 when inside it.
                distance = max(0.0, info.first_arrival - s, s - info.last_arrival)
                if best is None or distance < best[0]:
                    best = (distance, day)
        return best[1] if best else None


# --- arrival detection ---------------------------------------------------------------

@dataclass
class DetectionStats:
    pings: int = 0
    on_route: int = 0
    off_route: int = 0
    # Stops the bus passed while we were watching: between the first and the
    # last on-route ping. Separates "no data" from "detection failed".
    stops_in_observed_span: int = 0
    # Of those, stops skipped because the pings either side were more than
    # max_gap_s apart (collector or feed outage), so no arrival is claimed.
    skipped_gap: int = 0


def project_pings(
    info: TripInfo,
    pings: list[tuple[float, float, float]],
    max_offset_m: float = MAX_OFFSET_M,
) -> tuple[list[tuple[float, float]], DetectionStats]:
    """Project time-sorted (epoch_s, lat, lon) pings onto the trip's line.

    Each ping is searched near the previous on-route position (a little
    behind, and as far ahead as the bus could have driven), so loop routes do
    not jump between passes. Pings further than max_offset_m from the line
    are dropped. Distances are made non-decreasing (GPS jitter can otherwise
    look like the bus reversing). Returns [(epoch_s, distance_m)].
    """
    stats = DetectionStats(pings=len(pings))
    out: list[tuple[float, float]] = []
    prev_d = prev_t = None
    for t, lat, lon in pings:
        if prev_d is None:
            d, off = info.line.project(lat, lon)
        else:
            reach = MAX_SPEED_MPS * max(0.0, t - prev_t) + 500.0
            d, off = info.line.project(lat, lon, lo=prev_d - 300.0, hi=prev_d + reach)
        if off > max_offset_m:
            stats.off_route += 1
            continue
        if prev_d is not None:
            d = max(d, prev_d)
        out.append((t, d))
        prev_d, prev_t = d, t
        stats.on_route += 1
    return out, stats


def interpolate_arrivals(
    info: TripInfo,
    track: list[tuple[float, float]],
    max_gap_s: float = MAX_GAP_S,
    tolerance_m: float = ARRIVE_TOLERANCE_M,
    stats: "DetectionStats | None" = None,
) -> list[tuple[int, float]]:
    """For each stop, find the last on-route ping before the bus reached it and
    the first one at or past it, and interpolate linearly in distance.

    No extrapolation: a stop is only scored when there is a ping on each side
    of it no more than max_gap_s apart. Returns [(stop_index, epoch_s)].
    """
    arrivals = []
    j = 0
    for k, stop_d in enumerate(info.stop_distances):
        threshold = stop_d - tolerance_m
        while j < len(track) and track[j][1] < threshold:
            j += 1
        if j == 0 or j >= len(track):
            continue  # no ping before the stop, or the bus never got there
        (t0, d0), (t1, d1) = track[j - 1], track[j]
        if t1 - t0 > max_gap_s:
            if stats is not None:
                stats.skipped_gap += 1
            continue
        if d1 <= d0:
            continue
        arrivals.append((k, t0 + (threshold - d0) / (d1 - d0) * (t1 - t0)))
    return arrivals


def detect_arrivals(
    info: TripInfo,
    pings: list[tuple[float, float, float]],
    max_offset_m: float = MAX_OFFSET_M,
    max_gap_s: float = MAX_GAP_S,
    tolerance_m: float = ARRIVE_TOLERANCE_M,
) -> tuple[list[tuple[int, float]], DetectionStats]:
    pings = sorted(pings)
    track, stats = project_pings(info, pings, max_offset_m)
    if track:
        first_d, last_d = track[0][1], track[-1][1]
        stats.stops_in_observed_span = sum(
            1 for d in info.stop_distances if first_d < d - tolerance_m <= last_d
        )
    return interpolate_arrivals(info, track, max_gap_s, tolerance_m, stats), stats
