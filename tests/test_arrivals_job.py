"""
End-to-end Spark test on SYNTHETIC trips with known arrival times.

Runs the real job (read raw gz NDJSON -> dedupe -> classify -> detect ->
join schedule) on a local Spark session and compares detected arrivals with
the generator's ground truth. Skipped automatically if PySpark or Java is
not available.
"""

import gzip
import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java is required for Spark", allow_module_level=True)

from basmy.gtfs_static import load_merged  # noqa: E402
from tests.test_matching import LAT, kl_epoch, lon_at, tiny_feed  # noqa: E402
from transform.arrivals_job import build_spark, run_job  # noqa: E402
from transform.synthetic import generate, write_raw  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOTS = [
    ROOT / "data/gtfs_static/mybas-kota-bharu/2026-08-09.zip",
    ROOT / "data/gtfs_static/mybas-kota-bharu/2026-11-14.zip",
]


@pytest.fixture(scope="module")
def spark():
    s = build_spark("basmy-arrivals-test", master="local[2]")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def synthetic(tmp_path_factory):
    gtfs = load_merged(SNAPSHOTS)
    records, truth = generate(gtfs, date(2026, 11, 13), days=2, trips_per_day=6, seed=11)
    out = tmp_path_factory.mktemp("synthetic")
    write_raw(records, out)
    return gtfs, records, truth, out / "raw" / "vehicle_positions"


@pytest.fixture(scope="module")
def result(spark, synthetic):
    gtfs, _, _, raw = synthetic
    return run_job(spark, str(raw), gtfs, "SYNTHETIC")


def test_detected_arrivals_match_ground_truth(result, synthetic):
    _, _, truth, _ = synthetic
    expected = {
        (t["trip_id"], t["service_date"], t["stop_sequence"]): datetime.fromisoformat(t["true_arrival"]).timestamp()
        for t in truth
    }
    rows = result.arrivals.collect()
    assert len(rows) >= 0.9 * len(truth)
    errors = []
    for r in rows:
        key = (r["trip_id"], r["service_date"].isoformat(), r["stop_sequence"])
        errors.append(abs(r["actual_arrival"].timestamp() - expected[key]))
    errors.sort()
    assert sum(errors) / len(errors) < 20          # mean absolute error, seconds
    assert errors[int(0.95 * len(errors))] < 60    # p95


def test_output_columns_and_delay_consistency(result):
    cols = result.arrivals.columns
    for c in ["agency", "route_id", "trip_id", "stop_id", "stop_sequence", "scheduled_arrival",
              "actual_arrival", "delay_seconds", "service_date"]:
        assert c in cols
    for r in result.arrivals.limit(50).collect():
        diff = r["actual_arrival"].timestamp() - r["scheduled_arrival"].timestamp()
        assert abs(diff - r["delay_seconds"]) <= 1
        assert r["agency"] == "mybas-kota-bharu"


def test_quality_report_counts(result, synthetic):
    _, records, truth, _ = synthetic
    rep = result.report
    assert rep.raw_records == len(records)
    assert rep.pings_after_dedupe < rep.raw_records         # generator emits duplicates
    assert rep.by_status.get("no_trip_id", 0) > 0            # idle pings before departure
    assert rep.off_route_pings > 0                           # generator emits GPS glitches
    assert rep.trip_instances == len({(t["trip_id"], t["service_date"]) for t in truth})
    assert rep.pct_stops_detected_observed > 90
    assert "SYNTHETIC" in rep.render()


def _write_records(base: Path, records: list[dict]) -> Path:
    path = base / "raw" / "vehicle_positions" / "agency=x" / "date=2026-11-14" / "hour=16" / "part.jsonl.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")
    return path.parent.parent.parent.parent


def test_trip_past_midnight_gets_previous_service_date_and_next_day_schedule(spark, tmp_path):
    """Scheduled 23:50 / 24:00 / 24:10: pings after local midnight must belong
    to the previous service date, and 24:10:00 must become 00:10 next day."""
    gtfs = tiny_feed()
    pings = [(23, 49, 0), (23, 58, 0), (23, 59, 0), (0, 1, 0), (0, 8, 0), (0, 11, 0)]
    metres = [0, 900, 960, 1050, 1900, 2000]
    records = []
    for (h, m, s), d in zip(pings, metres):
        day = 14 if h == 23 else 15
        ts = int(kl_epoch(2026, 11, day, h, m, s))
        records.append({
            "agency": "mybas-kota-bharu", "fetched_at": datetime.utcfromtimestamp(ts).isoformat() + "Z",
            "entity": {"id": "V1", "vehicle": {
                "trip": {"trip_id": "T1"}, "timestamp": str(ts),
                "position": {"latitude": LAT, "longitude": lon_at(d)}, "vehicle": {"id": "V1"}}},
        })
    raw = _write_records(tmp_path, records)
    res = run_job(spark, str(raw), gtfs, "SYNTHETIC")
    rows = {r["stop_sequence"]: r for r in res.arrivals.collect()}
    assert set(rows) == {2, 3}
    assert {r["service_date"] for r in rows.values()} == {date(2026, 11, 14)}
    # 24:10:00 on the 14 Nov service day = 00:10 KL on the 15th = 16:10 UTC on the 14th.
    # (collect() returns naive datetimes in the Python process's local zone, so compare instants.)
    expected = datetime(2026, 11, 14, 16, 10, tzinfo=timezone.utc).timestamp()
    assert rows[3]["scheduled_arrival"].timestamp() == expected
    assert res.report.by_status == {"candidate": 6}


def test_duplicate_vehicle_timestamps_are_removed(spark, tmp_path):
    rec = {
        "agency": "mybas-kota-bharu", "fetched_at": "2026-11-14T00:00:00Z",
        "entity": {"id": "V1", "vehicle": {"timestamp": "1794614400",
                                           "position": {"latitude": 6.1, "longitude": 102.2}, "vehicle": {"id": "V1"}}},
    }
    later_poll = json.loads(json.dumps(rec))
    later_poll["fetched_at"] = "2026-11-14T00:00:30Z"
    raw = _write_records(tmp_path, [rec, later_poll])
    res = run_job(spark, str(raw), tiny_feed(), "SYNTHETIC")
    assert res.report.raw_records == 2
    assert res.report.pings_after_dedupe == 1
    assert res.report.by_status == {"no_trip_id": 1}
