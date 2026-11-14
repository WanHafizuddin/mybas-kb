"""
Live delay prediction for GET /predict-delay (Phase 5).

Uses the same matching code as arrival detection (basmy.matching) and the
same feature definitions as training (ml.features), so a prediction is built
the way the model was trained. No pandas here: the API runs on Render's free
tier (512 MB), so features are encoded straight into a numpy row.

Fallback order, never raising because a model is missing:
    "model"       a model is loaded and a live bus on the route is approaching
    "live_delay"  no model, but a live bus: predicted delay = its current delay
    "timetable"   no live bus: the next scheduled trip, delay 0
"""

import logging
import math
import threading
import time
import tomllib
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from basmy.matching import ARRIVE_TOLERANCE_M, EARTH_RADIUS_M, TripIndex, TripInfo

log = logging.getLogger("basmy.serving")

ROOT = Path(__file__).resolve().parent.parent
WEATHER_POINTS = ROOT / "config" / "weather_points.toml"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
LIVE_MAX_OFFSET_M = 150.0      # a live ping further than this from its trip's line is not trusted
WEATHER_TTL_S = 15 * 60


# --- live vehicles -------------------------------------------------------------------------

@dataclass(frozen=True)
class LiveVehicle:
    vehicle_id: str
    trip_id: str | None
    latitude: float
    longitude: float
    seen_epoch: float          # vehicle timestamp (feed), else collected_at


@dataclass
class Candidate:
    vehicle: LiveVehicle
    info: TripInfo
    service_date: date
    target_index: int          # index of the requested stop in the trip
    last_passed_index: int     # -1: has not reached the first stop yet
    current_delay_s: float     # delay right now, at the bus's position
    scheduled_target: datetime  # scheduled arrival at the requested stop (UTC)

    @property
    def stops_ahead(self) -> int:
        return self.target_index - max(self.last_passed_index, 0)


def scheduled_seconds_at(info: TripInfo, distance_m: float) -> float:
    """Timetable time (seconds since service-day midnight) at a point along
    the route, interpolated between the stops either side of it."""
    d, s = info.stop_distances, info.scheduled_arrivals
    if distance_m <= d[0]:
        return float(s[0])
    for k in range(1, len(d)):
        if distance_m <= d[k]:
            span = d[k] - d[k - 1]
            frac = 0.0 if span <= 0 else (distance_m - d[k - 1]) / span
            return s[k - 1] + frac * (s[k] - s[k - 1])
    return float(s[-1])


def live_candidates(index: TripIndex, route_id: str, stop_id: str,
                    vehicles: list[LiveVehicle]) -> list[Candidate]:
    """Live buses on `route_id` whose trip serves `stop_id` and that have not
    passed it yet."""
    out = []
    for v in vehicles:
        info = index.get(v.trip_id)
        if info is None or info.route_id != route_id or stop_id not in info.stop_ids:
            continue
        service_date = index.resolve_service_date(info, v.seen_epoch)
        if service_date is None:
            continue
        dist, offset = info.line.project(v.latitude, v.longitude)
        if offset > LIVE_MAX_OFFSET_M:
            continue
        target = info.stop_ids.index(stop_id)
        if dist >= info.stop_distances[target] - ARRIVE_TOLERANCE_M:
            continue  # already at or past the stop
        last_passed = max((k for k, sd in enumerate(info.stop_distances) if sd <= dist), default=-1)
        day_start = index.service_day_start(service_date).timestamp()
        delay = (v.seen_epoch - day_start) - scheduled_seconds_at(info, dist)
        if last_passed < 0:
            delay = max(0.0, delay)  # waiting before the first stop: not early, just not departed
        out.append(Candidate(
            vehicle=v, info=info, service_date=service_date, target_index=target,
            last_passed_index=last_passed, current_delay_s=delay,
            scheduled_target=index.scheduled_instant(service_date, info.scheduled_arrivals[target]),
        ))
    return out


def next_scheduled(index: TripIndex, route_id: str, stop_id: str, now: datetime) -> tuple[TripInfo, date, datetime] | None:
    """Next timetable arrival of `route_id` at `stop_id` after `now`. Checks
    today's and yesterday's service dates (times past 24:00:00)."""
    local_today = now.astimezone(index.tz).date()
    best = None
    for trip_id, trip in index.gtfs.trips.items():
        if trip.route_id != route_id:
            continue
        info = index.get(trip_id)
        if info is None or stop_id not in info.stop_ids:
            continue
        k = info.stop_ids.index(stop_id)
        for day in (local_today - timedelta(days=1), local_today):
            if not index.gtfs.service_active(info.service_id, day):
                continue
            when = index.scheduled_instant(day, info.scheduled_arrivals[k])
            if when >= now and (best is None or when < best[2]):
                best = (info, day, when)
    return best


# --- weather -------------------------------------------------------------------------------

def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


class CurrentWeather:
    """Current precipitation and temperature per weather point (Open-Meteo
    forecast API, one request for all points), cached for 15 minutes. Any
    failure just yields None values: weather is a feature, never a blocker."""

    def __init__(self, points_path: Path = WEATHER_POINTS, session=None, ttl_s: float = WEATHER_TTL_S):
        with open(points_path, "rb") as fh:
            self.points = tomllib.load(fh)["points"]
        self.session = session or requests
        self.ttl_s = ttl_s
        self._cache: dict[str, tuple[float | None, float | None]] = {}
        self._fetched = 0.0
        self._lock = threading.Lock()

    def nearest_point(self, lat: float, lon: float) -> str:
        return min(self.points, key=lambda p: _haversine_m(lat, lon, p["latitude"], p["longitude"]))["point_id"]

    def _refresh(self) -> None:
        params = {
            "latitude": ",".join(str(p["latitude"]) for p in self.points),
            "longitude": ",".join(str(p["longitude"]) for p in self.points),
            "current": "temperature_2m,precipitation",
            "timezone": "GMT",
        }
        resp = self.session.get(FORECAST_URL, params=params, timeout=5)
        resp.raise_for_status()
        blocks = resp.json()
        blocks = blocks if isinstance(blocks, list) else [blocks]
        self._cache = {
            p["point_id"]: (b["current"].get("precipitation"), b["current"].get("temperature_2m"))
            for p, b in zip(self.points, blocks)
        }

    def at(self, lat: float, lon: float) -> tuple[float | None, float | None]:
        """(precipitation_mm, temperature_c) at the nearest point, or (None, None)."""
        with self._lock:
            if time.monotonic() - self._fetched > self.ttl_s:
                try:
                    self._refresh()
                except Exception as exc:  # noqa: BLE001
                    log.warning("current weather unavailable: %s", exc)
                    self._cache = {}
                self._fetched = time.monotonic()
            return self._cache.get(self.nearest_point(lat, lon), (None, None))


# --- model ---------------------------------------------------------------------------------

class ModelUnavailable(Exception):
    pass


@dataclass
class DelayModel:
    booster: object
    metadata: dict
    route_hour_avg: dict = field(default_factory=dict)

    @property
    def version(self) -> str:
        return self.metadata["version"]

    @property
    def dataset(self) -> str:
        return self.metadata["dataset"]

    @classmethod
    def load(cls, models_uri: str, dataset: str = "real", allow_synthetic: bool = False) -> "DelayModel":
        """Load the CURRENT model under <models_uri>/<dataset>/. Refuses a
        SYNTHETIC model unless explicitly allowed (local demos only)."""
        import json

        import xgboost as xgb

        from ml.registry import Store, current_version, read_metadata

        if dataset == "synthetic" and not allow_synthetic:
            raise ModelUnavailable("synthetic models are refused unless BASMY_ALLOW_SYNTHETIC_MODEL=1")
        store = Store(models_uri)
        version = current_version(store, dataset)
        if version is None:
            raise ModelUnavailable(f"no current model under {models_uri}/{dataset}")
        meta = read_metadata(store, dataset, version)
        if meta is None:
            raise ModelUnavailable(f"metadata missing for {dataset}/{version}")
        if meta.get("dataset") == "SYNTHETIC" and not allow_synthetic:
            raise ModelUnavailable(f"{dataset}/{version} is a SYNTHETIC model; refused")
        booster = xgb.Booster()
        booster.load_model(store.fetch_file(f"{dataset}/{version}/model.json"))
        lookup_text = store.read_text(f"{dataset}/{version}/route_hour_avg.json")
        return cls(booster, meta, json.loads(lookup_text) if lookup_text else {})

    def encode(self, features: dict) -> "object":
        """One row in the model's feature order. Categorical values become
        their training category code (unknown -> NaN), exactly like the
        pandas categorical used at training time."""
        import numpy as np

        cats = self.metadata["categories"]
        row = []
        for name in self.metadata["features"]:
            value = features.get(name)
            if name in cats:
                value = str(value)
                row.append(float(cats[name].index(value)) if value in cats[name] else np.nan)
            else:
                row.append(np.nan if value is None else float(value))
        return np.asarray([row], dtype=np.float32)

    def predict_delay_min(self, features: dict) -> float:
        import xgboost as xgb

        names = self.metadata["features"]
        types = ["c" if n in self.metadata["categories"] else "q" for n in names]
        dm = xgb.DMatrix(self.encode(features), feature_names=names, feature_types=types,
                         enable_categorical=True)
        raw = float(self.booster.predict(dm)[0])
        if self.metadata.get("target_mode") == "residual_vs_last_known_delay":
            offset = features.get("prev_stop_delay_min")
            if offset is None:
                offset = features.get("route_hour_avg_delay_2w_min")
            raw += float(offset or 0.0)
        return raw


# --- the prediction --------------------------------------------------------------------------

def model_features(c: Candidate, index: TripIndex, weather: CurrentWeather | None, model: DelayModel) -> dict:
    local = c.scheduled_target.astimezone(index.tz)
    hour, dow = local.hour, local.isoweekday()
    lat, lon = index.gtfs.stops[c.info.stop_ids[c.target_index]]
    rain, temp = weather.at(lat, lon) if weather is not None else (None, None)
    return {
        "route_id": c.info.route_id,
        "stop_id": c.info.stop_ids[c.target_index],
        "stop_sequence": c.info.stop_sequences[c.target_index],
        "hour": hour,
        "day_of_week": dow,
        "is_weekend": int(dow >= 6),
        "rain_mm": rain,
        "temperature_c": temp,
        "prev_stop_delay_min": c.current_delay_s / 60.0,
        "stops_since_prev": c.stops_ahead,
        "route_hour_avg_delay_2w_min": model.route_hour_avg.get(c.info.route_id, {}).get(str(hour)),
    }


def predict(index: TripIndex, route_id: str, stop_id: str, vehicles: list[LiveVehicle], now: datetime,
            model: DelayModel | None = None, weather: CurrentWeather | None = None) -> dict:
    """Build the /predict-delay response body. Never raises for a missing
    model or missing weather; raises KeyError only for an unknown route/stop."""
    if route_id not in index.gtfs.routes:
        raise KeyError(f"unknown route_id {route_id!r}")
    if stop_id not in index.gtfs.stops:
        raise KeyError(f"unknown stop_id {stop_id!r}")

    body = {
        "route_id": route_id, "stop_id": stop_id, "generated_at": now.isoformat(),
        "source": None, "predicted_delay_seconds": None, "predicted_arrival": None,
        "predicted_arrival_local": None, "scheduled_arrival": None, "trip_id": None,
        "vehicle_id": None, "service_date": None, "stop_sequence": None,
        "current_delay_seconds": None, "model_version": None, "model_dataset": None,
        "features": None, "note": None,
    }

    best = None
    for c in live_candidates(index, route_id, stop_id, vehicles):
        source, features = "live_delay", None
        delay_s = c.current_delay_s
        if model is not None:
            try:
                features = model_features(c, index, weather, model)
                delay_s = 60.0 * model.predict_delay_min(features)
                source = "model"
            except Exception as exc:  # noqa: BLE001 -- a broken model must degrade, not fail
                log.warning("model prediction failed, using live delay: %s", exc)
                source, features, delay_s = "live_delay", None, c.current_delay_s
        arrival = max(now, c.scheduled_target + timedelta(seconds=delay_s))
        if best is None or arrival < best[0]:
            best = (arrival, c, source, delay_s, features)

    if best is not None:
        arrival, c, source, delay_s, features = best
        body.update(
            source=source, predicted_delay_seconds=round(delay_s, 1),
            predicted_arrival=arrival.isoformat(), predicted_arrival_local=arrival.astimezone(index.tz).isoformat(),
            scheduled_arrival=c.scheduled_target.isoformat(), trip_id=c.info.trip_id,
            vehicle_id=c.vehicle.vehicle_id, service_date=c.service_date.isoformat(),
            stop_sequence=c.info.stop_sequences[c.target_index],
            current_delay_seconds=round(c.current_delay_s, 1), features=features,
        )
        if source == "model":
            body.update(model_version=model.version, model_dataset=model.dataset)
        return body

    nxt = next_scheduled(index, route_id, stop_id, now)
    if nxt is None:
        body.update(source="none", note="no live bus approaching and no more scheduled trips today")
        return body
    info, day, when = nxt
    body.update(
        source="timetable", predicted_delay_seconds=0.0, predicted_arrival=when.isoformat(),
        predicted_arrival_local=when.astimezone(index.tz).isoformat(), scheduled_arrival=when.isoformat(),
        trip_id=info.trip_id, service_date=day.isoformat(),
        stop_sequence=info.stop_sequences[info.stop_ids.index(stop_id)],
        note="no live bus approaching this stop; showing the timetable",
    )
    return body
