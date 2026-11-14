import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from basmy.matching import TripIndex
from basmy.serving import (
    CurrentWeather, DelayModel, LiveVehicle, ModelUnavailable, live_candidates, predict,
)
from tests.test_matching import LAT, lon_at, tiny_feed

UTC = timezone.utc
KL = timezone(timedelta(hours=8))
DAY = datetime(2026, 11, 16, tzinfo=KL)  # a Monday; tiny_feed runs daily


def kl(h, m, s=0) -> datetime:
    return DAY.replace(hour=h, minute=m, second=s)


@pytest.fixture
def index():
    # Stops A (0 m) 08:00, B (1000 m) 08:05, C (2000 m) 08:10, route R1, trip T1.
    g = tiny_feed(("08:00:00", "08:05:00", "08:10:00"))
    g.routes = {"R1": {"route_id": "R1", "route_short_name": "R1"}}
    return TripIndex(g)


def bus(metres, seen: datetime, trip="T1", vid="V1", north_m=0.0):
    lat = LAT + north_m / 110_574.0
    return LiveVehicle(vid, trip, lat, lon_at(metres), seen.timestamp())


class FakeModel:
    version, dataset = "v-test", "REAL"
    route_hour_avg = {}

    def __init__(self, minutes=None, fail=False):
        self.minutes, self.fail, self.seen = minutes, fail, None

    def predict_delay_min(self, features):
        if self.fail:
            raise RuntimeError("broken model")
        self.seen = features
        return self.minutes


class NoWeather:
    def at(self, lat, lon):
        return (None, None)


# --- live position -> current delay ---------------------------------------------------------

def test_current_delay_from_position_between_stops(index):
    # 500 m along = halfway A->B, scheduled 08:02:30; seen at 08:03:00 -> 30 s late.
    (c,) = live_candidates(index, "R1", "C", [bus(500, kl(8, 3))])
    assert c.current_delay_s == pytest.approx(30, abs=2)
    assert c.last_passed_index == 0 and c.stops_ahead == 2
    assert c.scheduled_target == kl(8, 10).astimezone(UTC)


def test_bus_that_passed_the_stop_or_is_off_route_is_ignored(index):
    assert live_candidates(index, "R1", "B", [bus(1500, kl(8, 7))]) == []
    assert live_candidates(index, "R1", "C", [bus(500, kl(8, 3), north_m=400)]) == []
    assert live_candidates(index, "R1", "C", [bus(500, kl(8, 3), trip="unknown")]) == []
    assert live_candidates(index, "R1", "C", [bus(500, kl(13, 0))]) == []  # outside the trip window


# --- every fallback path ---------------------------------------------------------------------

def test_source_model(index):
    model = FakeModel(minutes=2.0)
    body = predict(index, "R1", "C", [bus(500, kl(8, 3))], kl(8, 3).astimezone(UTC), model, NoWeather())
    assert body["source"] == "model"
    assert body["model_version"] == "v-test"
    assert body["predicted_delay_seconds"] == 120.0
    assert body["predicted_arrival"] == (kl(8, 12)).astimezone(UTC).isoformat()
    assert model.seen["prev_stop_delay_min"] == pytest.approx(0.5, abs=0.05)
    assert model.seen["stops_since_prev"] == 2
    assert model.seen["hour"] == 8 and model.seen["day_of_week"] == 1


def test_source_live_delay_when_no_model(index):
    body = predict(index, "R1", "C", [bus(500, kl(8, 3))], kl(8, 3).astimezone(UTC), None, NoWeather())
    assert body["source"] == "live_delay"
    assert body["predicted_delay_seconds"] == pytest.approx(30, abs=2)
    assert body["model_version"] is None


def test_source_live_delay_when_model_breaks(index):
    body = predict(index, "R1", "C", [bus(500, kl(8, 3))], kl(8, 3).astimezone(UTC), FakeModel(fail=True))
    assert body["source"] == "live_delay"


def test_source_timetable_when_no_live_bus(index):
    body = predict(index, "R1", "B", [], kl(7, 0).astimezone(UTC), FakeModel(minutes=5.0))
    assert body["source"] == "timetable"
    assert body["predicted_delay_seconds"] == 0.0
    assert body["scheduled_arrival"] == kl(8, 5).astimezone(UTC).isoformat()


def test_no_more_trips_today(index):
    body = predict(index, "R1", "B", [], kl(23, 0).astimezone(UTC))
    assert body["source"] == "none" and body["predicted_arrival"] is None


def test_unknown_route_or_stop(index):
    with pytest.raises(KeyError):
        predict(index, "NOPE", "B", [], kl(7, 0).astimezone(UTC))
    with pytest.raises(KeyError):
        predict(index, "R1", "NOPE", [], kl(7, 0).astimezone(UTC))


def test_prediction_never_earlier_than_now(index):
    body = predict(index, "R1", "C", [bus(1900, kl(8, 3))], kl(8, 3).astimezone(UTC), FakeModel(minutes=-10))
    assert body["predicted_arrival"] == kl(8, 3).astimezone(UTC).isoformat()


# --- the real model path: numpy encoding == training-time pandas encoding -------------------

@pytest.fixture
def exported_model(tmp_path):
    xgb = pytest.importorskip("xgboost")
    from ml import features as F
    from ml.registry import Store, export_model, set_current

    rng = np.random.default_rng(0)
    n = 400
    df = pd.DataFrame({
        "route_id": pd.Categorical(rng.choice(["30425", "30426"], n)),
        "stop_id": pd.Categorical(rng.choice(["7496", "7497", "7500"], n)),
        "stop_sequence": rng.integers(1, 30, n), "hour": rng.integers(6, 22, n),
        "day_of_week": rng.integers(1, 8, n), "is_weekend": rng.integers(0, 2, n),
        "rain_mm": rng.random(n), "temperature_c": 25 + rng.random(n),
        "prev_stop_delay_min": rng.normal(2, 1, n), "stops_since_prev": rng.integers(1, 4, n),
        "route_hour_avg_delay_2w_min": rng.normal(1, 1, n),
    })[F.FEATURES]
    y = rng.normal(0.3, 0.2, n)
    model = xgb.XGBRegressor(n_estimators=20, max_depth=3, enable_categorical=True, tree_method="hist")
    model.fit(df, y)
    path = tmp_path / "model.json"
    model.get_booster().save_model(path)
    meta = {"version": "v1", "dataset": "REAL", "features": F.FEATURES,
            "target_mode": "residual_vs_last_known_delay",
            "categories": {c: [str(x) for x in df[c].cat.categories] for c in F.CATEGORICAL}}
    store = Store(str(tmp_path / "models"))
    export_model(store, "real", "v1", str(path), meta, extra_json={"route_hour_avg.json": {"30425": {"8": 1.5}}})
    set_current(store, "real", "v1", "test")
    return tmp_path / "models", model, df


def test_delay_model_numpy_path_matches_pandas_training_path(exported_model):
    root, model, df = exported_model
    loaded = DelayModel.load(str(root), "real")
    assert loaded.version == "v1" and loaded.route_hour_avg["30425"]["8"] == 1.5
    for i in range(10):
        row = df.iloc[[i]]
        expected = float(model.predict(row)[0]) + float(row["prev_stop_delay_min"].iloc[0])
        features = {k: (str(v) if k in ("route_id", "stop_id") else v) for k, v in row.iloc[0].items()}
        assert loaded.predict_delay_min(features) == pytest.approx(expected, abs=1e-4)


def test_unknown_category_does_not_crash(exported_model):
    root, _, df = exported_model
    loaded = DelayModel.load(str(root), "real")
    features = {k: (str(v) if k in ("route_id", "stop_id") else v) for k, v in df.iloc[0].items()}
    features["stop_id"] = "never-seen"
    assert np.isfinite(loaded.predict_delay_min(features))


def test_synthetic_model_refused_unless_allowed(tmp_path):
    from ml.registry import Store, export_model, set_current

    store = Store(str(tmp_path))
    (tmp_path / "m.json").write_text("{}")
    export_model(store, "real", "v1", str(tmp_path / "m.json"), {"version": "v1", "dataset": "SYNTHETIC"})
    set_current(store, "real", "v1", "x")
    with pytest.raises(ModelUnavailable, match="SYNTHETIC"):
        DelayModel.load(str(tmp_path), "real")
    with pytest.raises(ModelUnavailable):
        DelayModel.load(str(tmp_path), "synthetic")
    with pytest.raises(ModelUnavailable, match="no current model"):
        DelayModel.load(str(tmp_path / "empty"), "real")


# --- weather ---------------------------------------------------------------------------------

def test_weather_failure_gives_none_and_is_cached():
    class Boom:
        calls = 0

        def get(self, *a, **k):
            Boom.calls += 1
            raise OSError("offline")

    w = CurrentWeather(session=Boom())
    assert w.at(6.12, 102.24) == (None, None)
    assert w.at(6.12, 102.24) == (None, None)
    assert Boom.calls == 1  # not retried on every request


# --- the HTTP endpoint ------------------------------------------------------------------------

@pytest.fixture
def client(index, monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "realtime"))
    try:
        import api
    finally:
        sys.path.pop(0)
    state = api.ServingState()
    state.index, state.weather, state.model_error = index, NoWeather(), "BASMY_MODEL_URI not set"
    logged = []
    monkeypatch.setattr(api, "log_prediction", lambda body, st: logged.append(body))
    api.app.dependency_overrides[api.get_serving] = lambda: state
    api.app.dependency_overrides[api.get_live_vehicles] = lambda: []
    yield TestClient(api.app), api, state, logged
    api.app.dependency_overrides.clear()


def test_endpoint_timetable_fallback_and_logging(client):
    c, api, state, logged = client
    r = c.get("/predict-delay", params={"route_id": "R1", "stop_id": "B"})
    assert r.status_code == 200
    body = r.json()
    assert body["source"] in ("timetable", "none")
    assert "model unavailable (BASMY_MODEL_URI not set)" in body["note"]
    assert len(logged) == 1


def test_endpoint_live_delay(client, index):
    c, api, state, _ = client
    now = datetime.now(UTC)
    # A bus 500 m along, seen now; the tiny feed's trip window may not cover
    # "now", so give the trip today's times around the current moment.
    local_now = now.astimezone(KL)
    sec = local_now.hour * 3600 + local_now.minute * 60
    for st_list in (index.gtfs.trips["T1"].stop_times,):
        for i, st in enumerate(list(st_list)):
            st_list[i] = type(st)(st.stop_id, st.stop_sequence, sec - 60 + 300 * i, sec - 60 + 300 * i)
    index._cache.clear()
    api.app.dependency_overrides[api.get_live_vehicles] = lambda: [bus(500, now)]
    r = c.get("/predict-delay", params={"route_id": "R1", "stop_id": "C"})
    assert r.status_code == 200 and r.json()["source"] == "live_delay"


def test_endpoint_404_and_503(client):
    c, api, state, _ = client
    assert c.get("/predict-delay", params={"route_id": "NOPE", "stop_id": "B"}).status_code == 404
    assert c.get("/predict-delay", params={"route_id": "R1"}).status_code == 422
    api.app.dependency_overrides[api.get_serving] = lambda: None
    assert c.get("/predict-delay", params={"route_id": "R1", "stop_id": "B"}).status_code == 503


def test_existing_endpoints_still_registered(client):
    _, api, _, _ = client
    paths = {r.path for r in api.app.routes}
    assert {"/health", "/vehicles/current", "/vehicles/{vehicle_id}/history"} <= paths
