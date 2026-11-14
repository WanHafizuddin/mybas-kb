"""
Training table for the delay model: one row per trip instance and stop.

Every feature must be known at the moment a prediction would be made (in
Phase 5: "a bus on this trip was last seen at stop k-1; when will it reach
stop k?"). LEAKAGE lists each feature with how that is guaranteed.

Input: the Phase 3 warehouse (fact_arrivals, dim_stop, stg_weather_hourly).
Pure pandas, so the same code builds serving features in Phase 5.
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd

LOCAL_TZ = "Asia/Kuala_Lumpur"
ROUTE_HOUR_WINDOW_DAYS = 14

CATEGORICAL = ["route_id", "stop_id"]
NUMERIC = [
    "stop_sequence", "hour", "day_of_week", "is_weekend",
    "rain_mm", "temperature_c",
    "prev_stop_delay_min", "stops_since_prev",
    "route_hour_avg_delay_2w_min",
]
FEATURES = CATEGORICAL + NUMERIC
LABEL = "delay_min"

# Feature -> why it is known at prediction time (or what was done so it is).
LEAKAGE = {
    "route_id": "static timetable",
    "stop_id": "static timetable",
    "stop_sequence": "static timetable",
    "hour": "from the SCHEDULED arrival (local). The actual arrival hour would leak the label.",
    "day_of_week": "from the SCHEDULED arrival (local, ISO 1=Mon).",
    "is_weekend": "from the SCHEDULED arrival.",
    "prev_stop_delay_min": "delay at the last stop detected earlier on the same trip: the bus has "
                           "already passed it when we predict. Null when there is none.",
    "stops_since_prev": "how many stops back that observation was (1 = the previous stop).",
    "route_hour_avg_delay_2w_min": "mean delay for the route and scheduled hour over service dates "
                                   "d-14..d-1 only; never the same day, never the future.",
    "rain_mm": "FLAGGED: weather in the hour the bus was LAST SEEN (the previous stop's arrival), "
               "i.e. 'now' at prediction time; the arrival hour's weather would be the future. "
               "Rows with no previous stop use the scheduled hour, which in serving means a forecast.",
    "temperature_c": "same rule as rain_mm.",
}


@dataclass
class WarehouseFrames:
    arrivals: pd.DataFrame   # fact_arrivals
    stops: pd.DataFrame      # dim_stop (stop_id, weather_point_id)
    weather: pd.DataFrame    # stg_weather_hourly (point_id, hour_utc, temperature_c, precipitation_mm)


def _utc(s: pd.Series) -> pd.Series:
    s = pd.to_datetime(s)
    return s.dt.tz_localize("UTC") if s.dt.tz is None else s.dt.tz_convert("UTC")


def route_hour_rolling_mean(df: pd.DataFrame, window_days: int = ROUTE_HOUR_WINDOW_DAYS) -> pd.Series:
    """Mean delay (minutes) per (route_id, hour) over the `window_days`
    service dates strictly BEFORE each row's service date."""
    daily = (df.groupby(["route_id", "hour", "service_date"], observed=True)[LABEL]
               .agg(["sum", "count"]).reset_index())
    out = []
    for (route, hour), g in daily.groupby(["route_id", "hour"], observed=True):
        g = g.set_index(pd.to_datetime(g["service_date"]))[["sum", "count"]].asfreq("D", fill_value=0)
        # shift(1): today's own arrivals are never part of today's feature.
        prior = g.rolling(window_days, min_periods=1).sum().shift(1)
        mean = (prior["sum"] / prior["count"].replace(0, np.nan)).rename("avg")
        out.append(pd.DataFrame({"route_id": route, "hour": hour,
                                 "service_date": mean.index.date, "route_hour_avg_delay_2w_min": mean.values}))
    lookup = pd.concat(out, ignore_index=True) if out else pd.DataFrame(
        columns=["route_id", "hour", "service_date", "route_hour_avg_delay_2w_min"])
    merged = df[["route_id", "hour", "service_date"]].merge(
        lookup, on=["route_id", "hour", "service_date"], how="left")
    return merged["route_hour_avg_delay_2w_min"].to_numpy()


def build_training_table(frames: WarehouseFrames) -> pd.DataFrame:
    a = frames.arrivals.copy()
    a["service_date"] = pd.to_datetime(a["service_date"]).dt.date
    a["scheduled_arrival_utc"] = _utc(a["scheduled_arrival_utc"])
    a["actual_arrival_utc"] = _utc(a["actual_arrival_utc"])
    a[LABEL] = a["delay_seconds"].astype(float) / 60.0

    # Calendar features from the SCHEDULED time (known in advance).
    local = a["scheduled_arrival_utc"].dt.tz_convert(LOCAL_TZ)
    a["hour"] = local.dt.hour.astype(int)
    a["day_of_week"] = (local.dt.day_of_week + 1).astype(int)  # ISO 1=Mon..7=Sun
    a["is_weekend"] = (a["day_of_week"] >= 6).astype(int)

    # Previous detected stop on the same trip instance (same bus, trip, day).
    a = a.sort_values(["trip_id", "service_date", "vehicle_id", "stop_sequence"]).reset_index(drop=True)
    grp = a.groupby(["trip_id", "service_date", "vehicle_id"], sort=False)
    a["prev_stop_delay_min"] = grp[LABEL].shift(1)
    a["stops_since_prev"] = a["stop_sequence"] - grp["stop_sequence"].shift(1)
    prev_seen = grp["actual_arrival_utc"].shift(1)

    # Weather at the observation time: when the bus was last seen.
    obs_time = prev_seen.fillna(a["scheduled_arrival_utc"])
    a["_obs_hour"] = obs_time.dt.floor("h")
    stops = frames.stops[["stop_id", "weather_point_id"]]
    weather = frames.weather.rename(columns={"point_id": "weather_point_id", "hour_utc": "_obs_hour"}).copy()
    weather["_obs_hour"] = _utc(weather["_obs_hour"])
    a = a.merge(stops, on="stop_id", how="left").merge(
        weather[["weather_point_id", "_obs_hour", "precipitation_mm", "temperature_c"]],
        on=["weather_point_id", "_obs_hour"], how="left")
    a = a.rename(columns={"precipitation_mm": "rain_mm"})

    a["route_hour_avg_delay_2w_min"] = route_hour_rolling_mean(a)

    for c in CATEGORICAL:
        a[c] = a[c].astype("category")
    keep = ["arrival_id", "trip_id", "service_date", "vehicle_id", "scheduled_arrival_utc"] + FEATURES + [LABEL]
    return a[keep].sort_values(["service_date", "scheduled_arrival_utc", "trip_id"]).reset_index(drop=True)


# --- reading the warehouse ---------------------------------------------------------------

def read_duckdb(path: str) -> WarehouseFrames:
    import duckdb

    con = duckdb.connect(path, read_only=True)
    try:
        con.execute("SET TimeZone = 'UTC'")
        q = lambda sql: con.execute(sql).df()  # noqa: E731
        return WarehouseFrames(
            arrivals=q("""select arrival_id, route_id, trip_id, stop_id, stop_sequence, service_date, vehicle_id,
                                 scheduled_arrival_utc, actual_arrival_utc, delay_seconds
                          from analytics.fact_arrivals"""),
            stops=q("select stop_id, weather_point_id from analytics.dim_stop"),
            weather=q("select point_id, hour_utc, temperature_c, precipitation_mm from analytics.stg_weather_hourly"),
        )
    finally:
        con.close()


def read_bigquery(project: str, dataset: str) -> WarehouseFrames:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    q = lambda sql: client.query(sql).to_dataframe()  # noqa: E731
    d = f"`{project}.{dataset}`"
    return WarehouseFrames(
        arrivals=q(f"""select arrival_id, route_id, trip_id, stop_id, stop_sequence, service_date, vehicle_id,
                              scheduled_arrival_utc, actual_arrival_utc, delay_seconds from {d}.fact_arrivals"""),
        stops=q(f"select stop_id, weather_point_id from {d}.dim_stop"),
        weather=q(f"select point_id, hour_utc, temperature_c, precipitation_mm from {d}.stg_weather_hourly"),
    )
