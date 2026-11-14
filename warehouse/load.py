"""
Build the warehouse's raw tables and load them into DuckDB (local) or
BigQuery (production). dbt then builds everything else from these tables.

Raw tables (same columns on both engines):
    arrivals        Phase 2 output (one row per detected stop arrival)
    routes, stops, trips   from the merged GTFS Static snapshots
    weather_hourly, weather_points   from warehouse/weather/fetch_weather.py

All timestamps are normalised to timezone-aware UTC before loading, so
DuckDB gets TIMESTAMPTZ and BigQuery gets TIMESTAMP (always UTC).

Synthetic and real data never share a warehouse: each has its own DuckDB
file / BigQuery dataset (see --dataset).

Usage:
    python -m warehouse.load --dataset synthetic --engine duckdb \
        --arrivals data/synthetic/processed/arrivals --weather data/synthetic/weather
    python -m warehouse.load --dataset real --engine bigquery --project MY_PROJECT \
        --arrivals gs://BUCKET/processed/arrivals --weather data/real/weather
"""

import argparse
import os
from pathlib import Path

import pandas as pd

from basmy.gtfs_static import load_merged

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_GTFS = [
    ROOT / "data/gtfs_static/mybas-kota-bharu/2026-08-09.zip",
    ROOT / "data/gtfs_static/mybas-kota-bharu/2026-11-14.zip",
]
AGENCY = "mybas-kota-bharu"
RAW_SCHEMA = "raw"


def gtfs_frames(gtfs_paths) -> dict[str, pd.DataFrame]:
    g = load_merged(gtfs_paths)
    routes = pd.DataFrame([
        {
            "agency": AGENCY,
            "route_id": rid,
            "route_short_name": r.get("route_short_name") or None,
            "route_long_name": r.get("route_long_name") or None,
            "route_color": r.get("route_color") or None,
        }
        for rid, r in g.routes.items()
    ])
    stops = pd.DataFrame([
        {"stop_id": sid, "stop_name": None, "stop_lat": lat, "stop_lon": lon}
        for sid, (lat, lon) in g.stops.items()
    ])
    # Stop names are not kept on GtfsStatic (only coordinates); read them from
    # the newest snapshot that has each stop.
    names = {}
    for path in gtfs_paths:
        from basmy.gtfs_static import _Source  # noqa: PLC0415 -- small internal reuse
        for row in _Source(Path(path)).rows("stops.txt"):
            names[row["stop_id"]] = row.get("stop_name") or None
    stops["stop_name"] = stops["stop_id"].map(names)
    trips = pd.DataFrame([
        {
            "trip_id": t.trip_id, "route_id": t.route_id, "service_id": t.service_id,
            "direction_id": t.direction_id, "trip_headsign": t.headsign,
            "first_stop_departure_s": t.stop_times[0].departure, "n_stops": len(t.stop_times),
        }
        for t in g.trips.values()
    ])
    return {"routes": routes, "stops": stops, "trips": trips}


def _utc(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series)
    return s.dt.tz_localize("UTC") if s.dt.tz is None else s.dt.tz_convert("UTC")


def read_arrivals(path: str) -> pd.DataFrame:
    """Read the partitioned Phase 2 output. service_date comes back from the
    partition folder name."""
    df = pd.read_parquet(path)
    df["service_date"] = pd.to_datetime(df["service_date"].astype(str)).dt.date
    for col in ("scheduled_arrival", "actual_arrival"):
        df[col] = _utc(df[col])
    df["stop_sequence"] = df["stop_sequence"].astype("int64")
    df["delay_seconds"] = df["delay_seconds"].astype("int64")
    return df[["agency", "route_id", "trip_id", "stop_id", "stop_sequence", "scheduled_arrival",
               "actual_arrival", "delay_seconds", "service_date", "vehicle_id"]]


def read_weather(weather_dir: str) -> dict[str, pd.DataFrame]:
    hourly = pd.read_parquet(Path(weather_dir) / "weather_hourly.parquet")
    hourly["hour_utc"] = _utc(hourly["hour_utc"])
    hourly["fetched_at"] = _utc(hourly["fetched_at"])
    for col in ("temperature_c", "precipitation_mm", "rain_mm"):
        hourly[col] = hourly[col].astype("float64")
    points = pd.read_parquet(Path(weather_dir) / "weather_points.parquet")
    return {"weather_hourly": hourly, "weather_points": points}


def build_tables(arrivals: str, weather: str, gtfs) -> dict[str, pd.DataFrame]:
    tables = {"arrivals": read_arrivals(arrivals)}
    tables.update(gtfs_frames(gtfs))
    tables.update(read_weather(weather))
    return tables


# --- engines --------------------------------------------------------------------------

def duckdb_path(dataset: str) -> Path:
    return Path(os.environ.get("BASMY_DUCKDB_PATH", ROOT / "data" / dataset / "warehouse.duckdb"))


def load_duckdb(tables: dict[str, pd.DataFrame], db_path: Path) -> None:
    import duckdb

    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute("SET TimeZone = 'UTC'")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        for name, df in tables.items():
            con.register("_df", df)
            con.execute(f"CREATE OR REPLACE TABLE {RAW_SCHEMA}.{name} AS SELECT * FROM _df")
            con.unregister("_df")
    finally:
        con.close()


def load_bigquery(tables: dict[str, pd.DataFrame], project: str, dataset: str, location: str) -> None:
    """Load jobs are free in BigQuery; each table is replaced (WRITE_TRUNCATE).
    Auth: Application Default Credentials (gcloud auth application-default login)."""
    from google.cloud import bigquery

    client = bigquery.Client(project=project, location=location)
    raw_dataset = f"{project}.{dataset}_{RAW_SCHEMA}"
    for name, df in tables.items():
        job = client.load_table_from_dataframe(
            df, f"{raw_dataset}.{name}",
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
        )
        job.result()


def main(argv=None):
    p = argparse.ArgumentParser(description="Load raw tables into the warehouse")
    p.add_argument("--dataset", choices=["real", "synthetic"], required=True,
                   help="real and synthetic data always go to separate warehouses")
    p.add_argument("--engine", choices=["duckdb", "bigquery"], default="duckdb")
    p.add_argument("--arrivals", required=True, help="Phase 2 arrivals folder (local or gs://)")
    p.add_argument("--weather", required=True, help="folder with weather_hourly.parquet + weather_points.parquet")
    p.add_argument("--gtfs", nargs="+", default=[str(x) for x in DEFAULT_GTFS], help="oldest first")
    p.add_argument("--project", default=os.environ.get("GCP_PROJECT"), help="GCP project (bigquery)")
    p.add_argument("--location", default=os.environ.get("BQ_LOCATION", "us-central1"))
    args = p.parse_args(argv)

    tables = build_tables(args.arrivals, args.weather, args.gtfs)
    if args.engine == "duckdb":
        path = duckdb_path(args.dataset)
        load_duckdb(tables, path)
        where = f"{path} (schema {RAW_SCHEMA})"
    else:
        if not args.project:
            p.error("--project (or GCP_PROJECT) is required for bigquery")
        load_bigquery(tables, args.project, f"basmy_{args.dataset}", args.location)
        where = f"{args.project}.basmy_{args.dataset}_{RAW_SCHEMA}"
    counts = ", ".join(f"{k}={len(v)}" for k, v in tables.items())
    print(f"loaded [{args.dataset.upper()}] into {where}: {counts}")


if __name__ == "__main__":
    main()
