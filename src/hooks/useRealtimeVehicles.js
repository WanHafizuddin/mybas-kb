import { useEffect, useState } from 'react';

// realtime/api.py, run locally via `uvicorn api:app --reload --port 8000`.
const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';
const POLL_INTERVAL_MS = 15000; // feed itself only refreshes every 30s server-side

// Polls GET /vehicles/current -- real GPS-tracked buses from the
// GTFS-Realtime feed, as opposed to the schedule-simulated ones in gtfs.js.
export function useRealtimeVehicles() {
    const [vehicles, setVehicles] = useState([]);
    const [error, setError] = useState(null);
    const [lastFetched, setLastFetched] = useState(null);

    useEffect(() => {
        let cancelled = false;

        async function poll() {
            try {
                const res = await fetch(`${API_BASE}/vehicles/current`);
                if (!res.ok) throw new Error(`realtime API returned ${res.status}`);
                const data = await res.json();
                if (!cancelled) {
                    setVehicles(data);
                    setError(null);
                    setLastFetched(new Date());
                }
            } catch (err) {
                if (!cancelled) setError(err.message);
            }
        }

        poll();
        const interval = setInterval(poll, POLL_INTERVAL_MS);
        return () => {
            cancelled = true;
            clearInterval(interval);
        };
    }, []);

    return { vehicles, error, lastFetched };
}
