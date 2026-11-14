from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")

from warehouse.load import build_tables, gtfs_frames, load_duckdb  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOTS = [
    ROOT / "data/gtfs_static/mybas-kota-bharu/2026-08-09.zip",
    ROOT / "data/gtfs_static/mybas-kota-bharu/2026-11-14.zip",
]


def _write_inputs(tmp_path: Path) -> tuple[Path, Path]:
    arrivals = tmp_path / "arrivals" / "service_date=2026-11-13"
    arrivals.mkdir(parents=True)
    pd.DataFrame({
        "agency": ["mybas-kota-bharu"], "route_id": ["30425"], "trip_id": ["241_0_WD_1"],
        "stop_id": ["7496"], "stop_sequence": [2],
        # naive = UTC, as the pandas fallback writer produces
        "scheduled_arrival": [datetime(2026, 11, 12, 22, 31)],
        "actual_arrival": [datetime(2026, 11, 12, 22, 33, 5)],
        "delay_seconds": [125], "vehicle_id": ["DFG3055"],
    }).to_parquet(arrivals / "part-0.parquet", index=False)

    weather = tmp_path / "weather"
    weather.mkdir()
    pd.DataFrame({
        "point_id": ["kota_bharu"], "hour_utc": [pd.Timestamp("2026-11-12T22:00Z")],
        "temperature_c": [26.1], "precipitation_mm": [0.4], "rain_mm": [0.4],
        "fetched_at": [pd.Timestamp("2026-11-14T00:00Z")],
    }).to_parquet(weather / "weather_hourly.parquet", index=False)
    pd.DataFrame({"point_id": ["kota_bharu"], "name": ["Kota Bharu"], "latitude": [6.1254],
                  "longitude": [102.2386]}).to_parquet(weather / "weather_points.parquet", index=False)
    return tmp_path / "arrivals", weather


def test_gtfs_frames_from_merged_snapshots():
    frames = gtfs_frames(SNAPSHOTS)
    assert len(frames["routes"]) == 16
    assert frames["stops"]["stop_name"].notna().all()
    assert frames["trips"]["trip_id"].is_unique


def test_build_and_load_duckdb_normalises_timestamps_to_utc(tmp_path):
    arrivals, weather = _write_inputs(tmp_path)
    tables = build_tables(str(arrivals), str(weather), SNAPSHOTS)
    a = tables["arrivals"]
    assert str(a["actual_arrival"].dt.tz) == "UTC"
    assert a.loc[0, "service_date"] == date(2026, 11, 13)

    db = tmp_path / "wh.duckdb"
    load_duckdb(tables, db)
    con = duckdb.connect(str(db), read_only=True)
    try:
        con.execute("SET TimeZone = 'UTC'")
        names = {r[0] for r in con.execute(
            "select table_name from information_schema.tables where table_schema = 'raw'").fetchall()}
        assert names == {"arrivals", "routes", "stops", "trips", "weather_hourly", "weather_points"}
        (ts,) = con.execute("select actual_arrival from raw.arrivals").fetchone()
        assert ts == datetime(2026, 11, 12, 22, 33, 5, tzinfo=timezone.utc)
    finally:
        con.close()
