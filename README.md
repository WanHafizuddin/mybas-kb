# bas.my — Kota Bharu Bus Tracker

A web app for exploring **myBAS Kota Bharu** public-transit routes, stops, and schedules. It combines static GTFS data with an optional GTFS-Realtime pipeline for live vehicle locations.

## Live demo

- [Render](https://kb-mybastracker-simulatedlive.onrender.com)
- [Vercel](https://mybas-kb.vercel.app/)

## Features

- Browse Kota Bharu bus routes, stops, and route shapes on an interactive map
- View schedule-based next-bus information
- Use pre-processed GTFS static data in the browser
- Optionally collect and serve GTFS-Realtime vehicle positions through a small FastAPI service
- Preserve vehicle-position history in PostgreSQL for future analysis

## Tech stack

| Area | Tools |
| --- | --- |
| Frontend | React 19, Vite, Leaflet, React Leaflet, Tailwind CSS |
| Static data | GTFS data from [data.gov.my](https://data.gov.my) |
| Real-time service | Python, FastAPI, GTFS-Realtime |
| Storage | PostgreSQL / Supabase |

## Getting started

### Frontend

```bash
git clone https://github.com/WanHafizuddin/mybas-kb.git
cd mybas-kb
npm install
npm run dev
```

The development server starts at the URL printed by Vite. Other useful commands:

```bash
npm run build    # create a production build
npm run lint     # run ESLint
npm run preview  # preview the production build
```

### Refresh static GTFS data

The application reads generated JSON files from `public/data/`. To download and process the latest static GTFS feed:

```bash
node scripts/process-gtfs.js
```

## Real-time backend (optional)

The frontend and real-time service are independent. To run the service locally, first create `realtime/.env` from `realtime/.env.example` and set `DATABASE_URL`.

```bash
cd realtime
pip install -r requirements.txt

# Create the database schema before starting the collector.
# Then collect a single GTFS-Realtime poll:
python collector.py --once

# Or collect continuously (30-second interval):
python collector.py

# Serve the API:
uvicorn api:app --reload --port 8000
```

The frontend polls `VITE_API_URL`; it defaults to `http://localhost:8000`.

### Data flow

```text
GTFS static feed → processing script → public/data/*.json → React map
GTFS-Realtime feed → collector → PostgreSQL → FastAPI → React map
```

The real-time feed may provide a `trip_id` without full route or stop context. Route-aware features should join that ID with the static GTFS trip data.

## Project structure

```text
src/         React map UI, hooks, and GTFS utilities
public/data/ Generated static GTFS JSON
scripts/     Static GTFS processing script
realtime/    FastAPI API, collector, schema, and Python dependencies
```

## Data sources

- [myBAS Kota Bharu static GTFS](https://data.gov.my/data-catalogue/gtfs_mybas_kota_bharu)
- Malaysia GTFS-Realtime feed

## License

No license has been specified yet.
