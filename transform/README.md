# Phase 2: arrival detection

This is a PySpark job. It turns raw vehicle positions (in the layout the
Phase 1 collector writes) plus the GTFS Static timetable into one row per
detected bus arrival at a stop.

```
raw/vehicle_positions/agency=/date=/hour=/*.jsonl.gz ─┐
data/gtfs_static/mybas-kota-bharu/*.zip (oldest first) ┴─> arrivals_job ─> processed/arrivals/service_date=YYYY-MM-DD/*.parquet
                                                                        └─> quality report
```

## Output

| column | type | meaning |
|---|---|---|
| agency | string | `mybas-kota-bharu` |
| route_id | string | GTFS route, resolved from `trip_id` (the Kota Bharu feed never sends `route_id`) |
| trip_id | string | GTFS trip |
| stop_id, stop_sequence | string, int | the stop, from `stop_times` |
| scheduled_arrival | timestamp (UTC) | service date + GTFS arrival time. Times past 24:00:00 roll into the next day. |
| actual_arrival | timestamp (UTC) | interpolated from the pings either side of the stop |
| delay_seconds | int | actual − scheduled. Positive means late. |
| service_date | date | the Kuala Lumpur service day. This is the partition key. |
| vehicle_id | string | the bus that served the trip (an extra column, useful for Phase 4) |

## How it works

All the matching logic lives in [`basmy/matching.py`](../basmy/matching.py),
which Phase 5 serving will reuse. The job runs in these steps:

1. **Read and deduplicate.** It reads the raw files with an explicit schema,
   then deduplicates on `(agency, vehicle_id, vehicle timestamp)`. The feed
   re-sends a bus's last position until the bus reports again.
2. **Classify each ping.** A ping gets one of these statuses:
   - `no_trip_id`: the bus is idle or not in service.
   - `unknown_trip`: the trip isn't in any timetable snapshot.
   - `no_service_date`: the ping falls outside the trip's time window (1 h
     before the first stop to 1.5 h after the last) on every service day the
     calendar allows. The candidate days are the local date and the day
     before, so that trips running past midnight are handled.
   - `candidate`: none of the above.
3. **Detect arrivals per trip instance**, grouped by `(vehicle, trip_id,
   service_date)`:
   - Each ping is projected onto the route line. The search is limited to
     near the previous position, so routes that double back on themselves
     don't jump between passes.
   - Pings more than **100 m** off the line are dropped (`--max-offset-m`).
   - Progress along the route is made non-decreasing, so GPS jitter doesn't
     look like the bus reversing.
   - For each stop, the arrival time is interpolated linearly between the
     last ping before the bus was within 20 m of the stop and the first ping
     after (`--tolerance-m`).
   - There is no extrapolation, and the job skips stops where the two pings
     are more than 10 minutes apart (`--max-gap-s`).
4. **Route line.** It uses the trip's GTFS shape. If the shape is missing, or
   any stop lies more than 150 m from it, the line is built from the stop
   sequence instead.
5. **Duplicate claims.** If two buses claim the same trip on the same day,
   the one with more on-route pings wins for each stop.
6. **Join `stop_times`** to get the scheduled time and the delay, then write
   Parquet partitioned by `service_date`. Partitions are overwritten
   dynamically, so a rerun replaces only the dates it processed.

## GTFS Static snapshots

The upstream Kota Bharu GTFS is unreliable, so dated snapshots are kept in
[`data/gtfs_static/mybas-kota-bharu/`](../data/gtfs_static/mybas-kota-bharu/)
and merged **oldest first**. A trip in a newer snapshot replaces the same trip
in an older one.

| snapshot | routes | trips | notes |
|---|---|---|---|
| `2026-08-09.zip` | 8 (D10–D31) | 416, weekday and weekend | all 16 shapes |
| `2026-11-14.zip` | 16 (D10–D62) | 376, **weekend only** | only 16 of the 32 shapes it references |

Even merged, **weekday trips for routes 249–256 (D32–D62) are missing.**
Real pings on those trips are reported as `unknown_trip`.

## Run it

Install the dependencies from the repo root (needs Python 3.11 or later and
Java 17 or later):

```bash
pip install -e ".[collector,transform,test]"
```

**Synthetic, end to end.** Generate the data:

```bash
python -m transform.synthetic --out data/synthetic --days 3 --trips-per-day 40
```

Then run the job on it:

```bash
python -m transform.arrivals_job --raw data/synthetic/raw/vehicle_positions --gtfs data/gtfs_static/mybas-kota-bharu/2026-08-09.zip data/gtfs_static/mybas-kota-bharu/2026-11-14.zip --out data/synthetic/processed/arrivals --label SYNTHETIC
```

**Real history from Supabase.** The export is read-only and needs
`realtime/.env`:

```bash
python -m transform.export_supabase --out data/real --from 2026-08-09 --to 2026-08-12
```

```bash
python -m transform.arrivals_job --raw data/real/raw/vehicle_positions --gtfs data/gtfs_static/mybas-kota-bharu/2026-08-09.zip data/gtfs_static/mybas-kota-bharu/2026-11-14.zip --out data/real/processed/arrivals --label REAL
```

**Real data from the collector bucket** (with Docker, see below):

```bash
docker compose -f transform/docker-compose.yml run --rm arrivals --raw gs://BUCKET/raw/vehicle_positions --out gs://BUCKET/processed/arrivals --label REAL
```

`--label` is required and printed on the report, so synthetic numbers can't
be mistaken for real ones.

### Docker

[`Dockerfile`](Dockerfile) provides Python 3.11, Java 17, PySpark and the
Cloud Storage connector. [`docker-compose.yml`](docker-compose.yml) mounts
`data/` and your gcloud application-default credentials (read-only). Build
the image:

```bash
docker compose -f transform/docker-compose.yml build
```

Run the tests inside it:

```bash
docker compose -f transform/docker-compose.yml run --rm --entrypoint pytest arrivals
```

### Windows without Docker

On Windows, Hadoop needs `winutils.exe` and `hadoop.dll` (third-party
binaries) to read or write local files. Without them, and without
`HADOOP_HOME` set, `--reader auto` and `--writer auto` handle local paths
differently:

- **Reading:** the gzipped NDJSON is read in Python and handed to Spark.
- **Writing:** the arrivals (a small table) are collected and written in the
  same partitioned Parquet layout with pyarrow.

Spark still does the deduplicating, classifying, matching, joining and
reporting. `gs://` paths, Linux and Docker always use Spark's own reader and
writer.

If Spark fails to start with `Unable to establish loopback connection`,
point the JVM's Unix-socket temp directory at a short path:

```bash
export JAVA_TOOL_OPTIONS="-Djdk.net.unixdomain.tmpdir=C:\\jtmp"
```

## Tests

```bash
pytest tests/test_matching.py tests/test_arrivals_job.py
```

- `test_matching.py` covers GTFS time parsing past 24:00, snapshot merging,
  calendars, projection (including routes that double back), choosing the
  service date after midnight, interpolation, off-route pings, gaps and
  jitter.
- `test_arrivals_job.py` runs the full Spark job:
  - on synthetic trips, checked against ground truth (MAE under 20 s, p95
    under 60 s)
  - on a hand-made trip from 23:50 to 24:10
  - for deduplication

  It is skipped if PySpark or Java is missing.

## Results so far

Both runs were on 2026-11-14, with the job run locally on Windows (Spark
4.0.1 on Java 21).

| | SYNTHETIC (3 days, 120 trips) | REAL (Supabase export, 9–12 Aug 2026) |
|---|---|---|
| pings matched to a trip | 85.6% | 41.1% |
| ping status | 13% idle, 1.4% off-route | 36.9% `unknown_trip`, 10.7% idle, 9.7% off-route |
| stops detected / stops of observed trips | 97.5% | 8.1% |
| stops detected / stops passed while observed | 100% | 28.3%; the other 5,736 fell in gaps of 17 to 90 minutes between pings |
| arrival error vs. ground truth | MAE 9.6 s, p95 24 s, bias +6.6 s | no ground truth exists |

On the real data the low coverage comes from **missing data, not failed
detection**:

- The Supabase history was collected on and off by a laptop, so it has long
  holes.
- 37% of pings are on weekday trips for D32–D62, which no timetable snapshot
  contains.

Every stop that had pings within 10 minutes on both sides was scored.

## Known limits

- **Arrival vs. interpolation.** Interpolation assumes constant speed between
  pings. A bus that stops at a stop and dwells there will have its arrival
  time slightly overestimated. On synthetic data this is about +7 s of bias.
- **Coarse shapes.** The Kota Bharu shapes are mostly straight lines between
  stops, so on curved roads a correctly placed bus can be more than 100 m off
  the line. The quality report shows how many pings that rule drops, and
  `--max-offset-m` can be raised if real data shows it is too strict.
- **First stop.** A stop is only scored when there is a ping before it, so
  the first stop of a trip is rarely detected; the bus usually starts there.
