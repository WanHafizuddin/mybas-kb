from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from basmy.gtfs_static import (
    Calendar, GtfsStatic, StopTime, Trip, format_gtfs_time, load_gtfs, load_merged, parse_gtfs_time,
)
from basmy.matching import Polyline, TripIndex, detect_arrivals, interpolate_arrivals, project_pings

ROOT = Path(__file__).resolve().parent.parent
SNAP_AUG = ROOT / "data/gtfs_static/mybas-kota-bharu/2026-08-09.zip"
SNAP_NOV = ROOT / "data/gtfs_static/mybas-kota-bharu/2026-11-14.zip"


# --- a tiny hand-made feed: a straight 2 km east-bound road ------------------------

LAT = 6.12
LON0 = 102.20
M_PER_DEG_LON = 110_574.0  # approx at 6 N; exact value comes from Polyline


def lon_at(m: float) -> float:
    return LON0 + m / (Polyline([(LAT, LON0), (LAT, LON0 + 0.1)])._kx)


def tiny_feed(times=("23:50:00", "24:00:00", "24:10:00")) -> GtfsStatic:
    g = GtfsStatic()
    g.stops = {"A": (LAT, lon_at(0)), "B": (LAT, lon_at(1000)), "C": (LAT, lon_at(2000))}
    g.shapes = {"S": [(LAT, lon_at(0)), (LAT, lon_at(2000))]}
    g.trips = {"T1": Trip("T1", "R1", "DAILY", "S", 0, "east", [
        StopTime(s, i + 1, parse_gtfs_time(t), parse_gtfs_time(t)) for i, (s, t) in enumerate(zip("ABC", times))
    ])}
    g.calendars = {"DAILY": Calendar("DAILY", (True,) * 7, date(2026, 1, 1), date(2026, 12, 31))}
    return g


def kl_epoch(y, mo, d, h, mi, s=0) -> float:
    # Asia/Kuala_Lumpur is UTC+8 with no DST.
    return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc).timestamp() - 8 * 3600


# --- gtfs_static ------------------------------------------------------------------------

def test_parse_gtfs_time_past_midnight():
    assert parse_gtfs_time("06:30:10") == 6 * 3600 + 30 * 60 + 10
    assert parse_gtfs_time("25:10:00") == 25 * 3600 + 600
    assert format_gtfs_time(90600) == "25:10:00"


def test_load_real_snapshots_and_merge_newest_wins():
    aug, nov = load_gtfs(SNAP_AUG), load_gtfs(SNAP_NOV)
    assert len({t.route_id for t in aug.trips.values()}) == 8
    assert {t.service_id for t in nov.trips.values()} == {"WEEKEND"}  # the broken November export
    merged = load_merged([SNAP_AUG, SNAP_NOV])
    assert len({t.route_id for t in merged.trips.values()}) == 16
    assert {t.service_id for t in merged.trips.values()} == {"WEEKDAY", "WEEKEND"}
    shared = next(tid for tid in aug.trips if tid in nov.trips)
    assert merged.trips[shared] is not aug.trips[shared]
    assert merged.trips[shared].stop_times == nov.trips[shared].stop_times
    assert merged.agency_timezone == "Asia/Kuala_Lumpur"


def test_service_active_respects_calendar():
    g = load_gtfs(SNAP_AUG)
    assert g.service_active("WEEKDAY", date(2026, 11, 13))      # Friday
    assert not g.service_active("WEEKDAY", date(2026, 11, 14))  # Saturday
    assert g.service_active("WEEKEND", date(2026, 11, 14))
    assert not g.service_active("WEEKDAY", date(2027, 1, 4))     # after end_date


# --- geometry -------------------------------------------------------------------------

def test_polyline_projection_distance_and_offset():
    line = Polyline([(LAT, lon_at(0)), (LAT, lon_at(2000))])
    assert line.length == pytest.approx(2000, abs=1)
    lat_north = LAT + 50 / line._ky
    d, off = line.project(lat_north, lon_at(700))
    assert d == pytest.approx(700, abs=1)
    assert off == pytest.approx(50, abs=1)


def test_projection_window_disambiguates_out_and_back_route():
    # Out 1 km east then back on the same road: the point at 300 m east is
    # both 300 m and 1700 m along the line. The window picks the right pass.
    line = Polyline([(LAT, lon_at(0)), (LAT, lon_at(1000)), (LAT + 1e-5, lon_at(0))])
    d_out, _ = line.project(LAT, lon_at(300), lo=0, hi=800)
    d_back, _ = line.project(LAT, lon_at(300), lo=1200, hi=2000)
    assert d_out == pytest.approx(300, abs=2)
    assert d_back == pytest.approx(1700, abs=2)


def test_real_trip_index_uses_shape_or_falls_back_to_stops():
    index = TripIndex(load_merged([SNAP_AUG, SNAP_NOV]))
    sources = {index.get(t).geometry_source for t in index.gtfs.trips}
    assert sources == {"shape", "stops"}
    info = index.get("241_0_WD_1")
    assert info.route_id == "30425"
    assert info.stop_distances == sorted(info.stop_distances)
    assert info.line.length > 10_000
    assert index.get("does-not-exist") is None


# --- service date ------------------------------------------------------------------------

def test_service_date_for_trip_running_past_midnight():
    index = TripIndex(tiny_feed())
    info = index.get("T1")
    # 00:05 local on 15 Nov belongs to the 14 Nov service day (GTFS 24:05:00).
    assert index.resolve_service_date(info, kl_epoch(2026, 11, 15, 0, 5)) == date(2026, 11, 14)
    # 23:55 local on 14 Nov is the same instance.
    assert index.resolve_service_date(info, kl_epoch(2026, 11, 14, 23, 55)) == date(2026, 11, 14)
    # Noon is far outside the trip's window.
    assert index.resolve_service_date(info, kl_epoch(2026, 11, 14, 12, 0)) is None


def test_scheduled_instant_handles_times_past_24h():
    index = TripIndex(tiny_feed())
    instant = index.scheduled_instant(date(2026, 11, 14), parse_gtfs_time("24:10:00"))
    assert instant == datetime(2026, 11, 14, 16, 10, tzinfo=timezone.utc)  # 00:10 KL on 15 Nov


def test_service_date_respects_calendar():
    g = load_gtfs(SNAP_AUG)
    index = TripIndex(g)
    info = index.get("241_0_WD_1")  # weekday trip from 06:30
    assert index.resolve_service_date(info, kl_epoch(2026, 11, 13, 6, 40)) == date(2026, 11, 13)  # Fri
    assert index.resolve_service_date(info, kl_epoch(2026, 11, 14, 6, 40)) is None                # Sat


# --- interpolation ---------------------------------------------------------------------

def _pings(*pairs, t0=1_000_000.0):
    """(seconds after t0, metres along the road) -> (epoch, lat, lon)."""
    return [(t0 + s, LAT, lon_at(m)) for s, m in pairs]


def test_interpolates_between_pings_either_side_of_stop():
    info = TripIndex(tiny_feed(("08:00:00", "08:05:00", "08:10:00"))).get("T1")
    # B is at 1000 m; tolerance 20 m -> "arrived" at 980 m.
    arrivals, stats = detect_arrivals(info, _pings((0, -5), (30, 400), (60, 900), (90, 1100), (120, 2000)),
                                      tolerance_m=20)
    by_stop = dict(arrivals)
    assert 0 not in by_stop                      # no ping before stop A: never extrapolated
    assert by_stop[1] - 1_000_000 == pytest.approx(60 + (980 - 900) / 200 * 30)
    assert 2 in by_stop                           # last ping reaches the end of the line
    assert stats.on_route == 5


def test_off_route_pings_are_ignored():
    info = TripIndex(tiny_feed(("08:00:00", "08:05:00", "08:10:00"))).get("T1")
    line = info.line
    far_north = LAT + 400 / line._ky
    pings = [(1_000_000, LAT, lon_at(100)), (1_000_030, far_north, lon_at(1000)), (1_000_060, LAT, lon_at(1500))]
    track, stats = project_pings(info, pings, max_offset_m=100)
    assert stats.off_route == 1 and stats.on_route == 2
    assert [round(d) for _, d in track] == [100, 1500]


def test_large_gap_is_not_interpolated():
    info = TripIndex(tiny_feed(("08:00:00", "08:05:00", "08:10:00"))).get("T1")
    track = [(0.0, 500.0), (900.0, 1500.0)]  # 15 minutes between pings around stop B
    from basmy.matching import DetectionStats
    stats = DetectionStats()
    assert interpolate_arrivals(info, track, max_gap_s=600, stats=stats) == []
    assert stats.skipped_gap == 1
    assert [k for k, _ in interpolate_arrivals(info, track, max_gap_s=1200)] == [1]


def test_gps_jitter_backwards_does_not_undo_progress():
    info = TripIndex(tiny_feed(("08:00:00", "08:05:00", "08:10:00"))).get("T1")
    track, _ = project_pings(info, _pings((0, 100), (30, 990), (60, 960), (90, 1200)))
    assert [round(d) for _, d in track] == [100, 990, 990, 1200]
