import { useEffect, useState } from 'react';

// realtime/api.py -- same base URL as useRealtimeVehicles.
const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';

// /predict-delay `source` -> the small label shown next to the time.
const SOURCE_LABELS = {
    model: 'predicted',
    live_delay: 'live delay',
    timetable: 'scheduled',
};

function formatLocalTime(iso) {
    return new Date(iso).toLocaleTimeString('en-MY', {
        hour: 'numeric',
        minute: '2-digit',
        timeZone: 'Asia/Kuala_Lumpur',
    });
}

// Predicted arrival of the next bus on `routeId` at `stopId`, fetched only
// while `active` (i.e. while that stop's tooltip is open -- there are ~750
// stops, so nothing is fetched up front). Renders nothing until there is a
// prediction, so the existing timetable text is never replaced by a spinner
// or an error.
export function NextBusPrediction({ routeId, stopId, active, fetchImpl = fetch }) {
    const [prediction, setPrediction] = useState(null);

    useEffect(() => {
        if (!active || !routeId || !stopId) return undefined;
        let cancelled = false;
        const url = `${API_BASE}/predict-delay?route_id=${encodeURIComponent(routeId)}&stop_id=${encodeURIComponent(stopId)}`;
        fetchImpl(url)
            .then(res => (res.ok ? res.json() : null))
            .then(data => { if (!cancelled) setPrediction(data); })
            .catch(() => { if (!cancelled) setPrediction(null); });
        return () => { cancelled = true; };
    }, [active, routeId, stopId, fetchImpl]);

    if (!active || !prediction || !prediction.predicted_arrival || !SOURCE_LABELS[prediction.source]) {
        return null;
    }
    return (
        <div className="mt-1 text-gray-700" data-testid="next-bus-prediction">
            Predicted <span className="font-semibold">{formatLocalTime(prediction.predicted_arrival)}</span>
            <span className="ml-1 text-[10px] text-gray-400">({SOURCE_LABELS[prediction.source]})</span>
        </div>
    );
}
