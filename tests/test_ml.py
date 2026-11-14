from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from ml.features import FEATURES, LABEL, LEAKAGE, WarehouseFrames, build_training_table
from ml.registry import Store, current_version, export_model, set_current, should_promote
from ml.split import time_split
from ml.train import InsufficientHistory, check_history

UTC = timezone.utc


def arrival(trip, day, seq, sched_hour_local, delay_s, route="R1", stop=None, vehicle="V1", minute=0):
    """One fact_arrivals row. Kuala Lumpur is UTC+8."""
    sched = datetime(day.year, day.month, day.day, sched_hour_local, minute, tzinfo=UTC) - timedelta(hours=8)
    return {
        "arrival_id": f"{trip}|{day}|{seq}", "route_id": route, "trip_id": trip,
        "stop_id": stop or f"S{seq}", "stop_sequence": seq, "service_date": day, "vehicle_id": vehicle,
        "scheduled_arrival_utc": sched, "actual_arrival_utc": sched + timedelta(seconds=delay_s),
        "delay_seconds": delay_s,
    }


def frames(rows, weather_rows=()):
    stops = pd.DataFrame({"stop_id": sorted({r["stop_id"] for r in rows}), "weather_point_id": "kb"})
    weather = pd.DataFrame(list(weather_rows) or [
        {"point_id": "kb", "hour_utc": pd.Timestamp("2000-01-01", tz="UTC"), "temperature_c": 0.0,
         "precipitation_mm": 0.0}])
    return WarehouseFrames(pd.DataFrame(rows), stops, weather)


# --- feature building -------------------------------------------------------------------

def test_every_feature_has_a_leakage_note():
    assert set(FEATURES) == set(LEAKAGE)


def test_previous_stop_delay_stays_within_one_trip_instance():
    d = date(2026, 11, 2)
    rows = [arrival("T1", d, 1, 7, 60), arrival("T1", d, 2, 7, 120, minute=5), arrival("T1", d, 4, 7, 240, minute=15),
            arrival("T2", d, 1, 7, 600)]
    t = build_training_table(frames(rows)).set_index("arrival_id")
    assert np.isnan(t.loc[f"T1|{d}|1", "prev_stop_delay_min"])
    assert t.loc[f"T1|{d}|2", "prev_stop_delay_min"] == 1.0
    assert t.loc[f"T1|{d}|4", "prev_stop_delay_min"] == 2.0
    assert t.loc[f"T1|{d}|4", "stops_since_prev"] == 2       # stop 3 was not detected
    assert np.isnan(t.loc[f"T2|{d}|1", "prev_stop_delay_min"])  # never borrows from another trip


def test_calendar_features_come_from_scheduled_not_actual_time():
    d = date(2026, 11, 2)  # Monday
    # Scheduled 23:50 local, actually arrives 40 min late (00:30 next day).
    t = build_training_table(frames([arrival("T1", d, 1, 23, 2400, minute=50)]))
    assert t.loc[0, "hour"] == 23
    assert t.loc[0, "day_of_week"] == 1 and t.loc[0, "is_weekend"] == 0
    assert t.loc[0, LABEL] == 40.0


def test_route_hour_average_uses_only_previous_14_days():
    base = date(2026, 11, 20)
    rows = [
        arrival("A", base - timedelta(days=15), 1, 8, 6000),  # outside the window
        arrival("B", base - timedelta(days=3), 1, 8, 120),    # inside
        arrival("C", base - timedelta(days=1), 1, 8, 240),    # inside
        arrival("D", base, 1, 8, 9999),                        # same day: must not count
        arrival("E", base, 1, 9, 30),                          # other hour: own group
    ]
    t = build_training_table(frames(rows)).set_index("arrival_id")
    assert t.loc[f"D|{base}|1", "route_hour_avg_delay_2w_min"] == pytest.approx((2 + 4) / 2)
    assert np.isnan(t.loc[f"A|{base - timedelta(days=15)}|1", "route_hour_avg_delay_2w_min"])  # no history
    assert np.isnan(t.loc[f"E|{base}|1", "route_hour_avg_delay_2w_min"])


def test_weather_is_taken_when_the_bus_was_last_seen_not_at_arrival():
    d = date(2026, 11, 2)
    first = arrival("T1", d, 1, 7, 0, minute=55)                 # seen 23:55 UTC (07:55 local)
    second = arrival("T1", d, 2, 8, 0, minute=30)                # arrives in the next hour
    w = [{"point_id": "kb", "hour_utc": pd.Timestamp(first["actual_arrival_utc"]).floor("h"),
          "temperature_c": 25.0, "precipitation_mm": 0.0},
         {"point_id": "kb", "hour_utc": pd.Timestamp(second["actual_arrival_utc"]).floor("h"),
          "temperature_c": 22.0, "precipitation_mm": 9.0}]   # the future: must not be used
    t = build_training_table(frames([first, second], w)).set_index("arrival_id")
    assert t.loc[f"T1|{d}|2", "rain_mm"] == 0.0
    assert t.loc[f"T1|{d}|2", "temperature_c"] == 25.0


# --- split ------------------------------------------------------------------------------

def test_time_split_oldest_80_percent_whole_days():
    days = [date(2026, 10, 1) + timedelta(days=i) for i in range(10)]
    df = pd.DataFrame({"service_date": np.repeat(days, 10), "x": range(100)})
    train, test, info = time_split(df)
    assert set(train.service_date).isdisjoint(set(test.service_date))
    assert max(train.service_date) < min(test.service_date)
    assert len(train) == 80 and len(test) == 20
    assert (info.train_start, info.test_end) == (days[0], days[-1])


def test_time_split_always_leaves_a_test_day():
    df = pd.DataFrame({"service_date": [date(2026, 10, 1)] * 99 + [date(2026, 10, 2)]})
    train, test, _ = time_split(df)
    assert len(test) == 1 and len(train) == 99


# --- history guard -------------------------------------------------------------------------

def test_real_data_needs_four_weeks():
    short = pd.DataFrame({"service_date": [date(2026, 8, 9), date(2026, 8, 12)]})
    with pytest.raises(InsufficientHistory, match="4 day"):
        check_history(short, "real")
    assert check_history(short, "synthetic") == 4  # synthetic is not refused
    long = pd.DataFrame({"service_date": [date(2026, 10, 1), date(2026, 10, 28)]})
    assert check_history(long, "real") == 28


# --- promotion -----------------------------------------------------------------------------

def test_promotion_rule():
    assert should_promote(2.0, None).promote                         # first model
    assert should_promote(1.9, 2.0).promote                          # better on the same window
    assert not should_promote(2.0, 2.0).promote                      # equal is not better
    assert not should_promote(2.1, 2.0).promote                      # worse
    assert not should_promote(1.0, 2.0, comparable=False).promote    # different features: keep current
    assert not should_promote(1.95, 2.0, min_improvement=0.1).promote


def test_registry_keeps_real_and_synthetic_apart(tmp_path):
    store = Store(str(tmp_path))
    model = tmp_path / "m.json"
    model.write_text("{}")
    export_model(store, "synthetic", "v1", str(model), {"dataset": "SYNTHETIC"})
    set_current(store, "synthetic", "v1", "first")
    assert current_version(store, "synthetic") == "v1"
    assert current_version(store, "real") is None
    assert (tmp_path / "synthetic" / "v1" / "metadata.json").exists()
