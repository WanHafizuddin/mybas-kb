"""
Phase 2: arrival detection (PySpark).

Raw vehicle positions (Phase 1 layout) + GTFS Static -> one row per detected
stop arrival, with the scheduled time and the delay.

Pipeline:
  1. Read raw gzipped NDJSON (explicit schema, so a day with no trip blocks
     still has a trip_id column).
  2. Deduplicate on (agency, vehicle_id, vehicle timestamp): the feed re-sends
     a bus's last position on every poll until the bus reports again.
  3. Classify each ping (shared basmy.matching): no trip_id / trip unknown to
     the timetable / outside the trip's time window / candidate. Candidates
     get route_id and service_date.
  4. Per trip instance (agency, vehicle, trip_id, service_date): project
     pings onto the route, drop pings > --max-offset-m off it, interpolate the
     arrival at each stop between the pings on either side of it.
  5. Join stop_times for the scheduled time (GTFS times past 24:00:00 roll
     into the next calendar day), compute delay_seconds.
  6. Write Parquet partitioned by service_date; print a quality report.

All timestamps are UTC. service_date is the Kuala Lumpur service day.

Usage:
    python -m transform.arrivals_job \
        --raw data/synthetic/raw/vehicle_positions \
        --gtfs data/gtfs_static/mybas-kota-bharu/2026-08-09.zip data/gtfs_static/mybas-kota-bharu/2026-11-14.zip \
        --out data/synthetic/processed/arrivals --label SYNTHETIC
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from basmy import matching
from basmy.gtfs_static import GtfsStatic, load_merged

# --- schemas ---------------------------------------------------------------------

RAW_SCHEMA = T.StructType([
    T.StructField("agency", T.StringType()),
    T.StructField("fetched_at", T.StringType()),
    T.StructField("entity", T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("vehicle", T.StructType([
            T.StructField("trip", T.StructType([
                T.StructField("trip_id", T.StringType()),
                T.StructField("route_id", T.StringType()),
                T.StructField("start_date", T.StringType()),
            ])),
            T.StructField("position", T.StructType([
                T.StructField("latitude", T.DoubleType()),
                T.StructField("longitude", T.DoubleType()),
            ])),
            T.StructField("timestamp", T.StringType()),  # uint64 -> string in proto JSON
            T.StructField("vehicle", T.StructType([
                T.StructField("id", T.StringType()),
            ])),
        ])),
    ])),
])

PING_COLUMNS = ["agency", "vehicle_id", "trip_id", "vehicle_ts", "latitude", "longitude"]

CLASSIFIED_SCHEMA = T.StructType([
    T.StructField("agency", T.StringType()),
    T.StructField("vehicle_id", T.StringType()),
    T.StructField("trip_id", T.StringType()),
    T.StructField("vehicle_ts", T.LongType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("status", T.StringType()),
    T.StructField("route_id", T.StringType()),
    T.StructField("service_date", T.StringType()),
])

DETECTION_SCHEMA = T.StructType([
    T.StructField("kind", T.StringType()),           # "arrival" or "stats"
    T.StructField("agency", T.StringType()),
    T.StructField("vehicle_id", T.StringType()),
    T.StructField("trip_id", T.StringType()),
    T.StructField("route_id", T.StringType()),
    T.StructField("service_date", T.StringType()),
    T.StructField("stop_sequence", T.IntegerType()),
    T.StructField("stop_id", T.StringType()),
    T.StructField("actual_epoch", T.DoubleType()),
    T.StructField("pings", T.IntegerType()),
    T.StructField("on_route", T.IntegerType()),
    T.StructField("off_route", T.IntegerType()),
    T.StructField("scheduled_stops", T.IntegerType()),
    T.StructField("stops_in_span", T.IntegerType()),
    T.StructField("skipped_gap", T.IntegerType()),
])

OUTPUT_COLUMNS = [
    "agency", "route_id", "trip_id", "stop_id", "stop_sequence",
    "scheduled_arrival", "actual_arrival", "delay_seconds", "service_date", "vehicle_id",
]


@dataclass
class JobParams:
    max_offset_m: float = matching.MAX_OFFSET_M
    max_gap_s: float = matching.MAX_GAP_S
    tolerance_m: float = matching.ARRIVE_TOLERANCE_M


# --- per-worker state ------------------------------------------------------------

_INDEX_CACHE: dict[int, matching.TripIndex] = {}


def _index(gtfs: GtfsStatic) -> matching.TripIndex:
    """One TripIndex per Python worker process (geometry is built lazily and
    reused across batches)."""
    key = id(gtfs)
    if key not in _INDEX_CACHE:
        _INDEX_CACHE.clear()
        _INDEX_CACHE[key] = matching.TripIndex(gtfs)
    return _INDEX_CACHE[key]


# --- stages --------------------------------------------------------------------------

def _flatten_record(r: dict) -> tuple | None:
    """Same projection as the Spark path in read_pings(), in plain Python."""
    entity = r.get("entity") or {}
    v = entity.get("vehicle") or {}
    pos = v.get("position") or {}
    lat, lon = pos.get("latitude"), pos.get("longitude")
    vehicle_id = (v.get("vehicle") or {}).get("id") or entity.get("id")
    if lat is None or lon is None or vehicle_id is None:
        return None
    trip_id = (v.get("trip") or {}).get("trip_id") or None
    ts = v.get("timestamp")
    if ts is not None:
        ts = int(ts)
    else:
        from datetime import datetime as _dt
        ts = int(_dt.fromisoformat(r["fetched_at"].replace("Z", "+00:00")).timestamp())
    return (r.get("agency"), vehicle_id, trip_id, ts, float(lat), float(lon))


def read_pings_python(spark: SparkSession, raw_path: str) -> DataFrame:
    """Windows development fallback: Hadoop cannot open local files on
    Windows without winutils.exe/hadoop.dll, so read the gzipped NDJSON with
    Python and hand Spark the rows. Everything downstream is unchanged."""
    import gzip

    rows = []
    for path in sorted(Path(raw_path).rglob("*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    row = _flatten_record(json.loads(line))
                    if row is not None:
                        rows.append(row)
    pdf = pd.DataFrame(rows, columns=PING_COLUMNS).astype({"vehicle_ts": "int64"})
    schema = "agency string, vehicle_id string, trip_id string, vehicle_ts long, latitude double, longitude double"
    return spark.createDataFrame(pdf, schema=schema)


def read_pings(spark: SparkSession, raw_path: str, reader: str = "auto") -> DataFrame:
    if reader == "auto":
        reader = "spark" if (raw_path.startswith("gs://") or spark_can_access_local_files()) else "python"
    if reader == "python":
        return read_pings_python(spark, raw_path)
    raw = (
        spark.read.schema(RAW_SCHEMA)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.jsonl.gz")
        .json(raw_path)
    )
    v = F.col("entity.vehicle")
    fetched_epoch = F.unix_timestamp(F.to_timestamp("fetched_at"))
    return (
        raw.select(
            F.col("agency"),
            F.coalesce(v["vehicle"]["id"], F.col("entity.id")).alias("vehicle_id"),
            F.nullif(v["trip"]["trip_id"], F.lit("")).alias("trip_id"),
            F.coalesce(v["timestamp"].cast("long"), fetched_epoch).alias("vehicle_ts"),
            v["position"]["latitude"].alias("latitude"),
            v["position"]["longitude"].alias("longitude"),
        )
        .where(F.col("latitude").isNotNull() & F.col("longitude").isNotNull() & F.col("vehicle_id").isNotNull())
    )


def dedupe(pings: DataFrame) -> DataFrame:
    return pings.dropDuplicates(["agency", "vehicle_id", "vehicle_ts"])


def classify(pings: DataFrame, gtfs: GtfsStatic) -> DataFrame:
    def run(batches):
        index = _index(gtfs)
        for pdf in batches:
            status, route_ids, service_dates = [], [], []
            for trip_id, ts in zip(pdf["trip_id"], pdf["vehicle_ts"]):
                if trip_id is None or (isinstance(trip_id, float) and pd.isna(trip_id)):
                    status.append("no_trip_id"); route_ids.append(None); service_dates.append(None)
                    continue
                info = index.get(trip_id)
                if info is None:
                    status.append("unknown_trip"); route_ids.append(None); service_dates.append(None)
                    continue
                day = index.resolve_service_date(info, float(ts))
                if day is None:
                    status.append("no_service_date"); route_ids.append(info.route_id); service_dates.append(None)
                    continue
                status.append("candidate"); route_ids.append(info.route_id); service_dates.append(day.isoformat())
            out = pdf[PING_COLUMNS].copy()
            out["status"] = status
            out["route_id"] = route_ids
            out["service_date"] = service_dates
            yield out

    return pings.select(*PING_COLUMNS).mapInPandas(run, schema=CLASSIFIED_SCHEMA)


def detect(candidates: DataFrame, gtfs: GtfsStatic, params: JobParams) -> DataFrame:
    def run(pdf: pd.DataFrame) -> pd.DataFrame:
        index = _index(gtfs)
        first = pdf.iloc[0]
        info = index.get(first["trip_id"])
        pings = list(zip(pdf["vehicle_ts"].astype(float), pdf["latitude"], pdf["longitude"]))
        arrivals, stats = matching.detect_arrivals(
            info, pings, params.max_offset_m, params.max_gap_s, params.tolerance_m
        )
        base = {
            "agency": first["agency"], "vehicle_id": first["vehicle_id"], "trip_id": first["trip_id"],
            "route_id": info.route_id, "service_date": first["service_date"],
        }
        rows = [{
            **base, "kind": "stats", "stop_sequence": None, "stop_id": None, "actual_epoch": None,
            "pings": stats.pings, "on_route": stats.on_route, "off_route": stats.off_route,
            "scheduled_stops": len(info.stop_ids), "stops_in_span": stats.stops_in_observed_span,
            "skipped_gap": stats.skipped_gap,
        }]
        for k, epoch in arrivals:
            rows.append({
                **base, "kind": "arrival", "stop_sequence": info.stop_sequences[k], "stop_id": info.stop_ids[k],
                "actual_epoch": epoch, "pings": stats.pings, "on_route": stats.on_route,
                "off_route": stats.off_route, "scheduled_stops": len(info.stop_ids),
                "stops_in_span": stats.stops_in_observed_span, "skipped_gap": stats.skipped_gap,
            })
        return pd.DataFrame(rows, columns=DETECTION_SCHEMA.fieldNames())

    return (
        candidates.where(F.col("status") == "candidate")
        .groupBy("agency", "vehicle_id", "trip_id", "service_date")
        .applyInPandas(run, schema=DETECTION_SCHEMA)
    )


def stop_times_frame(spark: SparkSession, gtfs: GtfsStatic) -> DataFrame:
    rows = [
        (trip.trip_id, st.stop_sequence, st.stop_id, st.arrival)
        for trip in gtfs.trips.values() for st in trip.stop_times
    ]
    schema = "trip_id string, stop_sequence int, sched_stop_id string, arrival_seconds int"
    return spark.createDataFrame(rows, schema=schema)


def build_arrivals(detections: DataFrame, stop_times: DataFrame, timezone: str) -> DataFrame:
    arrivals = detections.where(F.col("kind") == "arrival")

    # If two vehicles both claim the same trip on the same day (stale or
    # reused trip_id), keep the one with more on-route pings for each stop.
    pick = Window.partitionBy("agency", "trip_id", "service_date", "stop_sequence").orderBy(
        F.col("on_route").desc(), F.col("vehicle_id")
    )
    arrivals = arrivals.withColumn("_rank", F.row_number().over(pick)).where("_rank = 1").drop("_rank")

    joined = arrivals.join(stop_times, ["trip_id", "stop_sequence"], "inner")
    # Local midnight of the service date as a UTC instant, then add the GTFS
    # seconds. Arithmetic in seconds, so 25:10:00 lands on the next day.
    midnight_utc = F.to_utc_timestamp(F.to_timestamp("service_date"), timezone)
    scheduled_epoch = F.unix_seconds(midnight_utc) + F.col("arrival_seconds")
    return joined.select(
        "agency",
        "route_id",
        "trip_id",
        F.col("sched_stop_id").alias("stop_id"),
        "stop_sequence",
        F.timestamp_seconds(scheduled_epoch).alias("scheduled_arrival"),
        F.timestamp_micros(F.round(F.col("actual_epoch") * 1_000_000).cast("long")).alias("actual_arrival"),
        F.round(F.col("actual_epoch") - scheduled_epoch).cast("int").alias("delay_seconds"),
        F.to_date("service_date").alias("service_date"),
        "vehicle_id",
    )


# --- quality report ------------------------------------------------------------------

@dataclass
class QualityReport:
    label: str
    raw_records: int = 0
    pings_after_dedupe: int = 0
    by_status: dict = field(default_factory=dict)
    on_route_pings: int = 0
    off_route_pings: int = 0
    trip_instances: int = 0
    scheduled_stops_in_observed_trips: int = 0
    stops_in_observed_span: int = 0
    stops_skipped_gap: int = 0
    detected_arrivals: int = 0
    scheduled_stops_all_trips: int = 0
    service_dates: list = field(default_factory=list)
    delay_seconds: dict = field(default_factory=dict)

    @property
    def pct_pings_matched(self) -> float:
        return 100.0 * self.on_route_pings / self.pings_after_dedupe if self.pings_after_dedupe else 0.0

    @property
    def pct_stops_detected_observed(self) -> float:
        d = self.scheduled_stops_in_observed_trips
        return 100.0 * self.detected_arrivals / d if d else 0.0

    @property
    def pct_stops_detected_in_span(self) -> float:
        d = self.stops_in_observed_span
        return 100.0 * self.detected_arrivals / d if d else 0.0

    @property
    def pct_stops_detected_all(self) -> float:
        d = self.scheduled_stops_all_trips
        return 100.0 * self.detected_arrivals / d if d else 0.0

    def as_dict(self) -> dict:
        d = dict(self.__dict__)
        d.update(
            pct_pings_matched=round(self.pct_pings_matched, 2),
            pct_stops_detected_observed_trips=round(self.pct_stops_detected_observed, 2),
            pct_stops_detected_in_observed_span=round(self.pct_stops_detected_in_span, 2),
            pct_stops_detected_all_scheduled=round(self.pct_stops_detected_all, 2),
        )
        return d

    def render(self) -> str:
        pct = lambda n, d: f"{100.0 * n / d:5.1f}%" if d else "  n/a"
        p = self.pings_after_dedupe
        lines = [
            f"=== Arrival detection quality report [{self.label}] ===",
            f"service dates            : {', '.join(self.service_dates) or '-'}",
            f"raw records              : {self.raw_records}",
            f"pings after dedupe       : {p}  ({self.raw_records - p} duplicates removed)",
            "ping status:",
        ]
        for status in ("no_trip_id", "unknown_trip", "no_service_date", "candidate"):
            n = self.by_status.get(status, 0)
            lines.append(f"  {status:<22} {n:>8}  {pct(n, p)}")
        lines += [
            f"  candidate -> on route  {self.on_route_pings:>8}  {pct(self.on_route_pings, p)}",
            f"  candidate -> off route {self.off_route_pings:>8}  {pct(self.off_route_pings, p)}",
            f"PINGS MATCHED TO A TRIP  : {self.pct_pings_matched:5.1f}%  (on-route pings / all pings)",
            f"trip instances observed  : {self.trip_instances}",
            f"STOPS DETECTED           : {self.detected_arrivals} / {self.scheduled_stops_in_observed_trips} "
            f"scheduled stops of observed trips = {self.pct_stops_detected_observed:5.1f}%",
            f"                           {self.detected_arrivals} / {self.stops_in_observed_span} "
            f"stops the bus passed while observed = {self.pct_stops_detected_in_span:5.1f}%",
            f"                           (not scored: {self.stops_skipped_gap} of those fell in a gap longer than "
            f"--max-gap-s between consecutive pings)",
            f"                           {self.detected_arrivals} / {self.scheduled_stops_all_trips} "
            f"stops of ALL trips scheduled on those dates = {self.pct_stops_detected_all:5.1f}%",
        ]
        if self.delay_seconds:
            d = self.delay_seconds
            lines.append(
                f"delay_seconds            : median {d['median']:.0f}, p10 {d['p10']:.0f}, p90 {d['p90']:.0f}, "
                f"min {d['min']:.0f}, max {d['max']:.0f}"
            )
        return "\n".join(lines)


def quality_report(label, raw_count, deduped_count, classified, detections, arrivals, gtfs) -> QualityReport:
    rep = QualityReport(label=label, raw_records=raw_count, pings_after_dedupe=deduped_count)
    rep.by_status = {r["status"]: r["count"] for r in classified.groupBy("status").count().collect()}

    s = detections.where(F.col("kind") == "stats").agg(
        F.count("*").alias("instances"),
        F.coalesce(F.sum("on_route"), F.lit(0)).alias("on"),
        F.coalesce(F.sum("off_route"), F.lit(0)).alias("off"),
        F.coalesce(F.sum("scheduled_stops"), F.lit(0)).alias("stops"),
        F.coalesce(F.sum("stops_in_span"), F.lit(0)).alias("span"),
        F.coalesce(F.sum("skipped_gap"), F.lit(0)).alias("gap"),
    ).first()
    rep.trip_instances, rep.on_route_pings, rep.off_route_pings = s["instances"], s["on"], s["off"]
    rep.scheduled_stops_in_observed_trips = s["stops"]
    rep.stops_in_observed_span = s["span"]
    rep.stops_skipped_gap = s["gap"]

    rep.detected_arrivals = arrivals.count()
    dates = sorted(r["service_date"] for r in detections.select("service_date").distinct().collect() if r["service_date"])
    rep.service_dates = dates
    rep.scheduled_stops_all_trips = sum(
        len(t.stop_times)
        for d in dates
        for t in gtfs.trips.values()
        if gtfs.service_active(t.service_id, date.fromisoformat(d))
    )
    if rep.detected_arrivals:
        q = arrivals.agg(
            F.percentile_approx("delay_seconds", [0.1, 0.5, 0.9]).alias("q"),
            F.min("delay_seconds").alias("mn"), F.max("delay_seconds").alias("mx"),
        ).first()
        rep.delay_seconds = {"p10": q["q"][0], "median": q["q"][1], "p90": q["q"][2], "min": q["mn"], "max": q["mx"]}
    return rep


# --- writing -------------------------------------------------------------------------

def spark_can_access_local_files() -> bool:
    """Hadoop's local filesystem needs winutils.exe + hadoop.dll on Windows."""
    return os.name != "nt" or bool(os.environ.get("HADOOP_HOME"))


def write_arrivals(arrivals: DataFrame, out: str, writer: str) -> None:
    if writer == "auto":
        writer = "spark" if (out.startswith("gs://") or spark_can_access_local_files()) else "pandas"
    if writer == "spark":
        (arrivals.repartition("service_date")
         .write.mode("overwrite").partitionBy("service_date").parquet(out))
        return
    # Development fallback for Windows without winutils: collect (arrival
    # tables are small) and write the same partitioned layout with pyarrow.
    import pyarrow as pa
    import pyarrow.parquet as pq

    pdf = arrivals.toPandas()
    for d in pdf["service_date"].unique():
        target = Path(out) / f"service_date={d}"
        if target.exists():
            for f in target.glob("*.parquet"):
                f.unlink()
    table = pa.Table.from_pandas(pdf, preserve_index=False)
    pq.write_to_dataset(table, root_path=out, partition_cols=["service_date"],
                        existing_data_behavior="delete_matching")


# --- entry point ---------------------------------------------------------------------

def build_spark(app: str = "basmy-arrivals", master: str | None = None) -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName(app)
        .master(master or os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.ui.enabled", "false")
    )
    return builder.getOrCreate()


@dataclass
class JobResult:
    arrivals: DataFrame
    report: QualityReport


def run_job(spark: SparkSession, raw_path: str, gtfs: GtfsStatic, label: str,
            params: JobParams | None = None, reader: str = "auto") -> JobResult:
    params = params or JobParams()
    pings = read_pings(spark, raw_path, reader).cache()
    raw_count = pings.count()
    deduped = dedupe(pings).cache()
    deduped_count = deduped.count()
    classified = classify(deduped, gtfs).cache()
    detections = detect(classified, gtfs, params).cache()
    arrivals = build_arrivals(detections, stop_times_frame(spark, gtfs), gtfs.agency_timezone).cache()
    report = quality_report(label, raw_count, deduped_count, classified, detections, arrivals, gtfs)
    return JobResult(arrivals=arrivals, report=report)


def main(argv=None):
    p = argparse.ArgumentParser(description="Detect actual bus arrivals from raw GTFS-Realtime positions")
    p.add_argument("--raw", required=True, help="raw/vehicle_positions folder (local path or gs://)")
    default_gtfs = os.environ.get("BASMY_GTFS", "").split()
    p.add_argument("--gtfs", nargs="+", default=default_gtfs or None, required=not default_gtfs,
                   help="GTFS Static zips/folders, OLDEST FIRST (default: $BASMY_GTFS)")
    p.add_argument("--out", required=True, help="output folder, e.g. gs://BUCKET/processed/arrivals")
    p.add_argument("--label", required=True, choices=["REAL", "SYNTHETIC"],
                   help="printed on the report so synthetic numbers are never mistaken for real ones")
    p.add_argument("--report-json", help="also write the quality report as JSON to this local path")
    p.add_argument("--samples", type=int, default=20, help="sample rows to print")
    p.add_argument("--reader", choices=["auto", "spark", "python"], default="auto",
                   help="auto = Spark, except local paths on Windows without winutils (python)")
    p.add_argument("--writer", choices=["auto", "spark", "pandas"], default="auto",
                   help="auto = Spark, except local paths on Windows without winutils (pandas)")
    p.add_argument("--max-offset-m", type=float, default=matching.MAX_OFFSET_M)
    p.add_argument("--max-gap-s", type=float, default=matching.MAX_GAP_S)
    p.add_argument("--tolerance-m", type=float, default=matching.ARRIVE_TOLERANCE_M)
    args = p.parse_args(argv)

    gtfs = load_merged(args.gtfs)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = run_job(spark, args.raw, gtfs, args.label,
                         JobParams(args.max_offset_m, args.max_gap_s, args.tolerance_m), args.reader)
        write_arrivals(result.arrivals, args.out, args.writer)
        print(result.report.render())
        if args.samples:
            print(f"\n--- {args.samples} sample rows [{args.label}] ---")
            (result.arrivals.orderBy(F.rand(seed=1)).limit(args.samples)
             .orderBy("service_date", "trip_id", "stop_sequence")
             .show(args.samples, truncate=False))
        if args.report_json:
            Path(args.report_json).parent.mkdir(parents=True, exist_ok=True)
            Path(args.report_json).write_text(json.dumps(result.report.as_dict(), indent=2, default=str))
        print(f"wrote arrivals to {args.out}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
