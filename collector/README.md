# Always-on raw collector

Polls the myBAS Kota Bharu GTFS-Realtime vehicle-position feed (and any
other feed listed in [`config/feeds.toml`](../config/feeds.toml)) every 30 seconds and lands the raw
data in Cloud Storage. It runs under systemd on one always-free e2-micro VM,
separate from the FastAPI app (`realtime/`). The Supabase collector and the
live API are unchanged.

```
data.gov.my feeds ──(30 s, parallel)──> local spool (gzipped NDJSON, per agency per UTC hour)
                                              │ once per hour, after the hour ends
                                              ▼
              gs://<bucket>/raw/vehicle_positions/agency=<a>/date=<YYYY-MM-DD>/hour=<HH>/part-<host>-<run>.jsonl.gz
```

## What a record looks like

There is one JSON line per vehicle entity per poll. Every field the feed
sent is kept, using protobuf's JSON mapping with spec field names. Nothing is
deduplicated or cleaned.

```json
{"agency":"mybas-kota-bharu",
 "feed_url":"https://api.data.gov.my/gtfs-realtime/vehicle-position/mybas-kota-bharu",
 "fetched_at":"2026-11-13T21:25:05.660Z",
 "header":{"gtfs_realtime_version":"2.0","incrementality":"FULL_DATASET","timestamp":"1794605102"},
 "entity":{"id":"DFG4355","vehicle":{"position":{"latitude":6.12,"longitude":102.23,"bearing":90.0},
           "timestamp":"1794605095","vehicle":{"id":"DFG4355","label":"DFG4355","license_plate":"DFG4355"}}}}
```

- All timestamps are UTC. `fetched_at` is the collector's own clock. The
  `header.timestamp` and `entity.vehicle.timestamp` fields are the feed's own
  values, as epoch seconds stored as strings (that's how protobuf's JSON
  mapping writes uint64).
- Partitions use the UTC date and hour. A Kuala Lumpur service day spans two
  UTC dates, and Phase 2 derives `service_date` in Asia/Kuala_Lumpur.
- A field missing from `entity` means the feed did not send it.
- An empty feed writes nothing. Empty feeds at night are normal; the poll
  still shows up in `health.json` and the journal.

## Guarantees

- **One feed failing never stops the others.** Each feed is fetched on its own
  thread. A network error, HTTP error or bad payload only backs that feed off:
  30 s, then 60 s, 120 s and so on, up to 10 min. The backoff resets on the
  next success.
- **Local files are deleted only after a confirmed upload.** That means the
  upload returned success, with an MD5 checksum the server verified. A failed
  upload stays on disk and is retried at the next hour boundary and on
  restart.
- **Uploads never overwrite.** Each upload uses `if_generation_match=0`. A file
  name is unique per host, run and hour, so a `412` only happens when the same
  file was uploaded just before a crash. In that case it counts as done.
- **Startup** first uploads leftover spool files from earlier runs.
  **SIGTERM** (`systemctl stop`, reboot, redeploy) finishes the current poll,
  then uploads everything, including the hour still in progress.
- **Crash safety:** each poll is appended as a complete gzip member and
  fsynced, so a crash can damage at most the last poll.

## Free-tier budget

| Resource | Use | Always-free limit |
|---|---|---|
| e2-micro VM in us-central1 | 1 | 1 |
| Standard persistent disk | 30 GB | 30 GB |
| Uploads (Class A ops) | 1 feed × 24 × ~30 days ≈ 720/month | 5,000/month |
| Regional Standard storage | grows with fleet size; check monthly | 5 GB-months |

Each extra feed adds about 720 uploads a month, so the free tier fits about
6 feeds at hourly rotation. Check current pricing for in-use external IPv4
addresses before applying.

## Deploy

You run these yourself. Nothing in this repo creates cloud resources
automatically. You need `gcloud` (logged in) and Terraform 1.6 or later.

**1. Authenticate Terraform as you**, not with a key file:

```bash
gcloud auth application-default login
```

**2. Fill in variables.** `terraform.tfvars` is gitignored.

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars
```

Set `project_id`, `bucket_name` and `repo_ref` (use the feature branch until
it is merged).

**3. Review the plan, then apply:**

```bash
terraform init
terraform plan -out tfplan
terraform apply tfplan
```

Terraform creates the bucket, a service account with only
`roles/storage.objectCreator` on that bucket, and the e2-micro (Debian 12,
Python 3.11, 30 GB `pd-standard`) with the service account attached. The
startup script then adds 1 GB of swap, clones the repo at `repo_ref`, builds
the venv in `/opt/basmy/venv`, writes `/etc/basmy-collector.env`, and enables
and starts `basmy-collector.service`.

The VM clones over anonymous HTTPS, so the repo must be public.

**To redeploy new code**, push to `repo_ref`, then reboot the VM. The startup
script runs on every boot: it pulls, reinstalls and restarts the service.

```bash
gcloud compute instances reset basmy-collector --zone us-central1-a
```

## Health checks

SSH in first (the `terraform output ssh` value):

```bash
gcloud compute ssh basmy-collector --zone us-central1-a
```

Is it running, and how often has it restarted?

```bash
systemctl status basmy-collector
```

```bash
systemctl show basmy-collector -p NRestarts -p MemoryCurrent
```

Live log, with one line per feed per poll and one line per upload:

```bash
journalctl -u basmy-collector -f
```

First-boot install log:

```bash
sudo journalctl -u google-startup-scripts.service --no-pager | tail -50
```

Per-feed status: last success, entity count, failures and last error.

```bash
sudo cat /var/lib/basmy-collector/health.json
```

Local spool (it should only hold the current hour, plus anything waiting to
retry):

```bash
sudo find /var/lib/basmy-collector/spool -name '*.jsonl.gz' | sort
```

From your machine, check that hourly files are arriving:

```bash
gcloud storage ls -r "gs://BUCKET/raw/vehicle_positions/agency=mybas-kota-bharu/date=$(date -u +%F)/"
```

What healthy looks like:

- `health.json`'s `written_at` is less than about 40 s old.
- Every feed has `consecutive_failures` at 0 or low, with `last_entity_count`
  at 0 at night.
- `pending_spool_files` is no more than the number of feeds.
- Hourly objects keep appearing in GCS.

A feed stuck at a high `consecutive_failures` with a `last_error` means that
feed is down or has changed URL; the other feeds keep collecting.

## Add a feed

1. Add an entry to [`config/feeds.toml`](../config/feeds.toml):
   ```toml
   [[feeds]]
   agency = "rapid-bus-kuantan"           # becomes agency=<...> in GCS; never rename later
   path   = "prasarana?category=rapid-bus-kuantan"
   ```
   `agency` may contain only lowercase letters, digits and `-`.
2. Test it locally (see below).
3. Push to `repo_ref` and reboot the VM.

## Run locally

From the repo root, with Python 3.11 or later:

```bash
pip install -e ".[collector,test]"
```

```bash
pytest
```

Two-minute run against the real feeds, writing to `./out` instead of GCS:

```bash
python -m collector.main --sink local --out ./out --spool-dir ./spool --duration 120
```

`./out`, `./spool` and `health.json` are gitignored.

## Code layout

- `basmy/gtfs_rt.py` holds the shared fetch, parse and record logic, also used
  by `realtime/collector.py` and `realtime/inspect_feed.py`.
- `basmy/feeds.py` loads `config/feeds.toml`.
- `collector/main.py` has the poll loop, per-feed isolation and backoff,
  signals and `health.json`.
- `collector/spool.py` has the gzip NDJSON spool, hourly rotation and
  partition paths.
- `collector/sinks.py` has `GcsSink`, `LocalSink`, and upload-then-delete.
- `collector/basmy-collector.service` is the systemd unit.
- `infra/` holds the Terraform: bucket, service account, VM and startup
  script.
