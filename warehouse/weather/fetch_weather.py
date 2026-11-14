"""
Fetch hourly rain and temperature from the Open-Meteo archive API for every
weather point in config/weather_points.toml, over the dates present in the
arrivals data (or an explicit range).

- All timestamps are requested and stored in UTC (timezone=GMT).
- One HTTP request covers all points (Open-Meteo accepts comma-separated
  coordinates). No API key; free for non-commercial use.
- The archive lags real time by a few days. Hours it has no value for yet
  come back as null and are kept as null, never invented.

Output: a Parquet file with one row per (point_id, hour_utc):
    point_id, hour_utc, temperature_c, precipitation_mm, rain_mm, fetched_at

Usage:
    python -m warehouse.weather.fetch_weather --arrivals data/synthetic/processed/arrivals \
        --out data/synthetic/weather/weather_hourly.parquet
    python -m warehouse.weather.fetch_weather --start 2026-08-09 --end 2026-08-12 --out ...
"""

import argparse
import tomllib
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = ["temperature_2m", "precipitation", "rain"]
DEFAULT_POINTS = Path(__file__).resolve().parents[2] / "config" / "weather_points.toml"
USER_AGENT = "basmy-weather/1.0"


@dataclass(frozen=True)
class WeatherPoint:
    point_id: str
    name: str
    latitude: float
    longitude: float


def load_points(path: Path | str = DEFAULT_POINTS) -> list[WeatherPoint]:
    with open(path, "rb") as fh:
        cfg = tomllib.load(fh)
    points = [WeatherPoint(p["point_id"], p["name"], float(p["latitude"]), float(p["longitude"]))
              for p in cfg.get("points", [])]
    ids = [p.point_id for p in points]
    if not points or len(set(ids)) != len(ids):
        raise ValueError(f"{path}: need at least one point and unique point_id values")
    return points


def points_frame(points: list[WeatherPoint]) -> pd.DataFrame:
    return pd.DataFrame([p.__dict__ for p in points])


def utc_date_range_for_service_dates(service_dates: list[date]) -> tuple[date, date]:
    """A Kuala Lumpur service day runs from 16:00 UTC the day before (local
    midnight) into the next day for trips past midnight, so widen by a day
    on each side to cover every hour an arrival can fall in."""
    return min(service_dates) - timedelta(days=1), max(service_dates) + timedelta(days=1)


def service_dates_in_arrivals(arrivals_path: Path | str) -> list[date]:
    root = Path(arrivals_path)
    dates = sorted(
        date.fromisoformat(p.name.split("=", 1)[1])
        for p in root.glob("service_date=*") if p.is_dir()
    )
    if not dates:
        raise ValueError(f"no service_date=YYYY-MM-DD partitions under {root}")
    return dates


def fetch(points: list[WeatherPoint], start: date, end: date, session=None, timeout: float = 60) -> pd.DataFrame:
    params = {
        "latitude": ",".join(f"{p.latitude:.4f}" for p in points),
        "longitude": ",".join(f"{p.longitude:.4f}" for p in points),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(HOURLY_VARS),
        "timezone": "GMT",
    }
    getter = (session or requests).get
    resp = getter(ARCHIVE_URL, params=params, timeout=timeout, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    payload = resp.json()
    # One point -> a single object; several points -> a list, in request order.
    blocks = payload if isinstance(payload, list) else [payload]
    if len(blocks) != len(points):
        raise ValueError(f"expected {len(points)} locations from Open-Meteo, got {len(blocks)}")

    fetched_at = datetime.now(timezone.utc)
    frames = []
    for point, block in zip(points, blocks):
        if block.get("error"):
            raise ValueError(f"Open-Meteo error for {point.point_id}: {block.get('reason')}")
        hourly = block["hourly"]
        frames.append(pd.DataFrame({
            "point_id": point.point_id,
            "hour_utc": pd.to_datetime(hourly["time"], utc=True),
            "temperature_c": pd.array(hourly["temperature_2m"], dtype="Float64"),
            "precipitation_mm": pd.array(hourly["precipitation"], dtype="Float64"),
            "rain_mm": pd.array(hourly["rain"], dtype="Float64"),
            "fetched_at": fetched_at,
        }))
    return pd.concat(frames, ignore_index=True)


def main(argv=None):
    p = argparse.ArgumentParser(description="Fetch hourly weather (UTC) from Open-Meteo for the KB weather points")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--arrivals", help="arrivals Parquet folder; date range = its service dates +/- 1 day")
    src.add_argument("--start", help="first UTC date, YYYY-MM-DD (use with --end)")
    p.add_argument("--end", help="last UTC date, inclusive")
    p.add_argument("--points", default=str(DEFAULT_POINTS))
    p.add_argument("--out", required=True, help="output .parquet path")
    args = p.parse_args(argv)

    if args.arrivals:
        start, end = utc_date_range_for_service_dates(service_dates_in_arrivals(args.arrivals))
    else:
        if not args.end:
            p.error("--end is required with --start")
        start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)

    points = load_points(args.points)
    df = fetch(points, start, end)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    points_frame(points).to_parquet(out.with_name("weather_points.parquet"), index=False)
    missing = int(df["precipitation_mm"].isna().sum())
    print(f"weather: {len(points)} points x {start}..{end} UTC -> {len(df)} hourly rows "
          f"({missing} hours not yet in the archive) -> {out}")


if __name__ == "__main__":
    main()
