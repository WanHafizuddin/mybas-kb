# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`bas.my` — a Kota Bharu public transit tracker. It started as a static GTFS schedule viewer and is being
evolved into a real-time GPS tracking system. The project has two independent halves that only touch
each other at one seam:

- **Frontend** (repo root): Vite + React 19, renders the map. Consumes pre-processed static GTFS JSON
  and polls a small REST API for live vehicle positions.
- **Realtime backend** (`realtime/`): Python. Polls Malaysia's official GTFS-Realtime feed, stores
  history in Postgres (Supabase), and serves it over FastAPI. Runs and is deployed independently of the
  frontend — nothing in `realtime/` is built or bundled by Vite.

Deployed frontend: https://kb-mybastracker-simulatedlive.onrender.com and
https://kb-my-bas-tracker-simulated-live.vercel.app/ (see `vite.config.js` `allowedHosts` for the Render
domain). The Python backend is local-only for now; only the Supabase database is cloud-hosted.

## Commands

Frontend (run from repo root):
```
npm run dev       # Vite dev server
npm run build     # production build
npm run lint      # eslint
npm run preview   # preview a production build
node scripts/process-gtfs.js   # re-download+reprocess static GTFS -> public/data/*.json
```

Realtime backend (run from `realtime/`, after `pip install -r requirements.txt`):
```
python inspect_feed.py                 # one-shot: dump every field the live feed currently sends
python collector.py                    # poll forever (30s interval) and append to Postgres
python collector.py --once             # single poll, for testing
uvicorn api:app --reload --port 8000   # REST API over collected data
```
Both `collector.py` and `api.py` read `DATABASE_URL` from `realtime/.env` (see `.env.example`; not committed).

There is no test suite in this repo yet.

## Architecture

### Static GTFS pipeline (JS)
`scripts/process-gtfs.js` downloads the GTFS-static zip from `data.gov.my` (`mybas-kota-bharu`), unpacks
it into `temp_gtfs/`, and flattens routes/stops/shapes/trips/stop_times/calendar into
`public/data/{routes,stops,shapes,schedule,calendar}.json`. This is a build-time/offline step, not run at
request time. `src/utils/gtfs.js` (`loadData()`) fetches those JSON files client-side and derives
per-stop schedules; `src/components/Map.jsx` renders stops, route polylines, and a schedule-driven
"next bus" legend from that data.

### Realtime pipeline (Python, `realtime/`)
```
data.gov.my GTFS-Realtime (protobuf) -> collector.py -> Postgres (Supabase, vehicle_positions)
                                                              -> api.py (FastAPI) -> useRealtimeVehicles.js -> Map.jsx
```
- `collector.py` polls the vehicle-position feed every 30s and **appends** one row per vehicle per poll
  (never overwrites) — the table is meant to accumulate history for future ETA/delay modeling. On any
  `psycopg.Error` it closes and reopens the connection rather than assuming `rollback()` is safe; a dead
  connection (laptop sleep, network drop) must not permanently kill the process.
- `api.py` is intentionally minimal: `/vehicles/current` (latest position per vehicle) and
  `/vehicles/{vehicle_id}/history`. It does not expose trip/route/ETA endpoints.
- **Important, non-obvious fact about the live feed** (found by inspecting real traffic, not assumed):
  during active service `trip_id` is populated, but `route_id`, `stop_id`, `current_stop_sequence`,
  `direction_id`, `current_status`, and `speed` are not. Any feature needing route/stop context (next
  stop, ETA) must join `trip_id` against the static GTFS trip data from the pipeline above — the
  real-time feed will not hand back `route_id` directly. Don't assume other fields are populated without
  re-checking `inspect_feed.py`'s output; feed behavior can differ by time of day (buses report far more
  fields once actually in service vs. idle).
- The frontend only talks to `api.py`, never touches Postgres directly. `useRealtimeVehicles.js` polls
  `VITE_API_URL` (defaults to `http://localhost:8000`) every 15s.

### Two separate env/dependency worlds
Root `package.json`/`node_modules` (frontend + `scripts/process-gtfs.js`) and `realtime/requirements.txt`
(Python) are independent — installing one does not affect the other. `realtime/.env` (Postgres
`DATABASE_URL`) is separate from any frontend env config and is gitignored.
