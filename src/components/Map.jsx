import React, { useEffect, useState, useMemo, useRef } from 'react';
import { MapContainer, TileLayer, Polyline, CircleMarker, Circle, Popup, Tooltip, Marker } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import { loadData, getCurrentTimeSeconds, getNextArrival, getNextRouteTrip } from '../utils/gtfs';
import { useRealtimeVehicles } from '../hooks/useRealtimeVehicles';
import { useGeolocation } from '../hooks/useGeolocation';

const KotaBharuCenter = [6.1256, 102.2386];

// Lighten (pct>0) or darken (pct<0) a #rrggbb hex toward white/black by pct percent.
// Used to derive the lit roof (lighter) and shaded front (darker) faces of the
// isometric live-bus marker from its single base color, so the route-color theming
// stays a one-value input (route color / amber / grey, decided below).
function shade(hex, pct) {
    const n = parseInt(hex.slice(1), 16);
    let r = (n >> 16) & 255, g = (n >> 8) & 255, b = n & 255;
    const t = pct < 0 ? 0 : 255, p = Math.abs(pct) / 100;
    r = Math.round((t - r) * p) + r;
    g = Math.round((t - g) * p) + g;
    b = Math.round((t - b) * p) + b;
    return '#' + (1 << 24 | r << 16 | g << 8 | b).toString(16).slice(1);
}

export default function MapView() {
    const [data, setData] = useState(null);
    const [error, setError] = useState(null);
    const { vehicles: realVehicles, error: realtimeError, lastFetched: realtimeLastFetched } = useRealtimeVehicles();
    const { position: myPosition, error: geoError } = useGeolocation();
    const mapRef = useRef(null);

    // Real wall-clock time (seconds since midnight) -- only used to keep
    // "next scheduled bus" predictions at stops/routes accurate as time passes.
    const [currentTime, setCurrentTime] = useState(() => getCurrentTimeSeconds());

    useEffect(() => {
        loadData()
            .then(setData)
            .catch(err => {
                console.error("Failed to load GTFS data:", err);
                setError(err.message);
            });
    }, []);

    useEffect(() => {
        const interval = setInterval(() => setCurrentTime(getCurrentTimeSeconds()), 1000);
        return () => clearInterval(interval);
    }, []);

    const routeShapes = useMemo(() => {
        if (!data) return [];
        const shapes = [];
        const processedShapes = new Set();

        Object.keys(data.schedule).forEach(routeId => {
            const route = data.routes.find(r => r.id === routeId);
            const trips = data.schedule[routeId];
            trips.forEach(trip => {
                if (trip.shapeId && !processedShapes.has(trip.shapeId)) {
                    if (data.shapes[trip.shapeId]) {
                        shapes.push({
                            id: trip.shapeId,
                            points: data.shapes[trip.shapeId],
                            route: route
                        });
                        processedShapes.add(trip.shapeId);
                    }
                }
            });
        });
        return shapes;
    }, [data]);

    if (error) return (
        <div className="flex items-center justify-center h-screen w-full bg-red-900 text-white p-4">
            <div className="text-center">
                <h2 className="text-xl font-bold mb-2">Error Loading Data</h2>
                <p>{error}</p>
                <p className="text-sm mt-4 opacity-75">Check console for details.</p>
            </div>
        </div>
    );

    if (!data) return <div className="flex items-center justify-center h-screen w-full text-white bg-slate-900">Loading Transport Data...</div>;

    return (
        <div className="w-full h-full relative isolate">
            <div className="absolute inset-0 z-0">
                <MapContainer
                    ref={mapRef}
                    center={KotaBharuCenter}
                    zoom={13}
                    scrollWheelZoom={true}
                    style={{ height: '100%', width: '100%' }}
                >
                    <TileLayer
                        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                        url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
                        maxZoom={19}
                    />

                    {/* Draw Bus Stops */}
                    {data && Object.values(data.stops).map(stop => {
                        const nextBus = getNextArrival(stop.stopId, currentTime, data.stopSchedules, data.calendar);
                        // Calculate time diff in minutes
                        let minsAway = null;
                        if (nextBus) {
                            minsAway = Math.floor((nextBus.time - currentTime) / 60);
                        }

                        return (
                            <CircleMarker
                                key={stop.stopId || stop.name}
                                center={[stop.lat, stop.lon]}
                                radius={5}
                                pathOptions={{
                                    fillColor: '#ffffff',
                                    color: '#3b82f6', // Blue border
                                    weight: 2,
                                    fillOpacity: 0.9
                                }}
                            >
                                <Tooltip direction="top" offset={[0, -5]} opacity={0.95}>
                                    <div className="min-w-[120px]">
                                        <div className="font-bold text-sm border-b pb-1 mb-1 border-gray-200">{stop.name}</div>
                                        {nextBus ? (
                                            <div className="text-xs">
                                                <div className="font-semibold text-blue-600">Next Bus: {nextBus.route.shortName}</div>
                                                <div className="text-gray-600">{nextBus.headsign}</div>
                                                <div className="mt-1 font-mono bg-gray-100 px-1 rounded w-fit">
                                                    {minsAway !== null && minsAway <= 0 ? 'Due' : `${minsAway} min`}
                                                </div>
                                            </div>
                                        ) : (
                                            <div className="text-xs text-gray-400 italic">No scheduled buses soon</div>
                                        )}
                                    </div>
                                </Tooltip>
                            </CircleMarker>
                        )
                    })}

                    {/* Draw Static Route Network */}
                    {routeShapes.map((shape, idx) => (
                        <Polyline
                            key={`shape-${shape.id}-${idx}`}
                            positions={shape.points}
                            pathOptions={{
                                color: shape.route.color,
                                weight: 3,
                                opacity: 0.6
                            }}
                        >
                            <Tooltip sticky>
                                <div className="text-xs">
                                    <span className="font-bold">{shape.route.shortName}</span>: {shape.route.longName}
                                </div>
                            </Tooltip>
                        </Polyline>
                    ))}

                    {/* Draw Real GPS-Tracked Buses (from realtime/api.py) */}
                    {realVehicles.map(v => {
                        const reportedAt = new Date(v.vehicle_timestamp || v.collected_at);
                        const ageMinutes = (Date.now() - reportedAt.getTime()) / 60000;
                        const isStale = ageMinutes > 5;
                        // Feed gives trip_id but never route_id -- resolve the route via the
                        // static schedule's trip_id -> route lookup (data.tripIndex).
                        const tripInfo = v.trip_id ? data.tripIndex[v.trip_id] : null;
                        // Fallback (unresolved route) uses amber -- the route palette in gtfs.js
                        // only assigns Red/Blue/Green/Orange/Purple/Cyan/Pink/Indigo to the 8
                        // current routes, so amber/yellow never collides with an actual route line.
                        const dotColor = isStale ? '#6b7280' : (tripInfo ? tripInfo.route.color : '#eab308');

                        // Isometric 3/4-view bus, themed from the single `dotColor`:
                        // side = base, roof = lighter (lit from above), front = darker.
                        // Fresh buses get a pulsing ground ellipse; stale ones just the
                        // static shadow (matches the old "no pulse when stale" behavior).
                        const roof = shade(dotColor, 20);
                        const front = shade(dotColor, -18);
                        const door = shade(dotColor, -8);
                        const edge = shade(dotColor, -30);
                        const pulse = isStale ? '' : `<ellipse class="live-bus-pulse" cx="34" cy="53" rx="18" ry="3" fill="${dotColor}" />`;

                        const liveIcon = new L.DivIcon({
                            className: 'custom-live-bus-icon',
                            html: `<svg viewBox="0 0 72 60" width="44" height="37" xmlns="http://www.w3.org/2000/svg" style="overflow: visible;">
                                    ${pulse}
                                    <ellipse cx="34" cy="53" rx="24" ry="3.6" fill="#000" opacity="0.22" />
                                    <rect x="10" y="22" width="42" height="22" rx="4" fill="${dotColor}" />
                                    <polygon points="10,22 52,22 61,16 19,16" fill="${roof}" />
                                    <polygon points="52,22 61,16 61,38 52,44" fill="${front}" />
                                    <rect x="14" y="26" width="24" height="6.5" rx="1.5" fill="#e6f0fa" opacity="0.95" />
                                    <line x1="22" y1="26" x2="22" y2="32.5" stroke="${dotColor}" stroke-width="1" />
                                    <line x1="30" y1="26" x2="30" y2="32.5" stroke="${dotColor}" stroke-width="1" />
                                    <rect x="41" y="27" width="7" height="13" rx="1" fill="${door}" />
                                    <line x1="44.5" y1="27" x2="44.5" y2="40" stroke="${edge}" stroke-width="0.8" />
                                    <ellipse cx="20" cy="45" rx="4.2" ry="4.2" fill="#1f2937" />
                                    <ellipse cx="20" cy="45" rx="1.6" ry="1.6" fill="#9ca3af" />
                                    <ellipse cx="43" cy="45" rx="4.2" ry="4.2" fill="#1f2937" />
                                    <ellipse cx="43" cy="45" rx="1.6" ry="1.6" fill="#9ca3af" />
                                    <circle cx="57.5" cy="34" r="1.8" fill="#fde68a" />
                                   </svg>`,
                            iconSize: [44, 37],
                            iconAnchor: [21, 33]
                        });

                        return (
                            <Marker
                                key={`real-${v.vehicle_id || v.entity_id}`}
                                position={[v.latitude, v.longitude]}
                                icon={liveIcon}
                            >
                                <Popup>
                                    <div className="p-1 min-w-[140px]">
                                        <div className="flex items-center gap-1.5 text-sm font-bold text-gray-900">
                                            {v.vehicle_label || v.vehicle_id}
                                            <span className="text-[9px] font-bold uppercase tracking-wide px-1.5 py-0.5 rounded-full bg-amber-100 text-amber-700">Live GPS</span>
                                        </div>
                                        {v.license_plate && <div className="text-xs text-gray-600">Plate: {v.license_plate}</div>}
                                        <div className={`text-xs mt-1 ${isStale ? 'text-gray-500' : 'text-amber-600 font-semibold'}`}>
                                            {isStale
                                                ? `Last seen ${Math.round(ageMinutes)} min ago — likely idle`
                                                : `Updated ${Math.round(ageMinutes)} min ago`}
                                        </div>
                                        {tripInfo ? (
                                            <div className="text-xs mt-1 pt-1 border-t border-gray-100">
                                                <span className="font-bold" style={{ color: tripInfo.route.color }}>{tripInfo.route.shortName}</span>
                                                <span className="text-gray-600"> &middot; {tripInfo.headsign}</span>
                                            </div>
                                        ) : v.trip_id ? (
                                            <div className="text-xs text-gray-400 italic mt-0.5">Trip {v.trip_id} not in static schedule</div>
                                        ) : (
                                            <div className="text-xs text-gray-400 italic mt-0.5">No trip assigned (feed doesn't report one yet)</div>
                                        )}
                                    </div>
                                </Popup>
                            </Marker>
                        );
                    })}

                    {/* Draw "You Are Here" -- the browser's own GPS position */}
                    {myPosition && (
                        <>
                            <Circle
                                center={[myPosition.lat, myPosition.lon]}
                                radius={myPosition.accuracy}
                                pathOptions={{ color: '#8b5cf6', fillColor: '#8b5cf6', fillOpacity: 0.1, weight: 1 }}
                            />
                            <Marker
                                position={[myPosition.lat, myPosition.lon]}
                                icon={new L.DivIcon({
                                    className: 'custom-my-location-icon',
                                    html: `<div style="position: relative; width: 20px; height: 20px;">
                                            <div style="position: absolute; inset: 0; border-radius: 9999px; background-color: #8b5cf6; opacity: 0.4; animation: pulse-ring 1.6s ease-out infinite;"></div>
                                            <div style="position: relative; background-color: #8b5cf6; border: 2px solid white; border-radius: 9999px; width: 20px; height: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.4);"></div>
                                           </div>`,
                                    iconSize: [20, 20],
                                    iconAnchor: [10, 10]
                                })}
                            >
                                <Popup>
                                    <div className="text-xs font-semibold text-gray-900">You are here</div>
                                    <div className="text-[10px] text-gray-500">Accuracy: &plusmn;{Math.round(myPosition.accuracy)}m</div>
                                </Popup>
                            </Marker>
                        </>
                    )}
                </MapContainer>
            </div>

            {/* Locate Me button -- sits below Leaflet's built-in zoom controls */}
            <button
                onClick={() => {
                    if (myPosition && mapRef.current) {
                        mapRef.current.flyTo([myPosition.lat, myPosition.lon], 16);
                    }
                }}
                disabled={!myPosition}
                title={myPosition ? 'Center on my location' : (geoError || 'Locating…')}
                className="fixed z-[9999] flex items-center justify-center w-[34px] h-[34px] rounded-md shadow-lg glass-panel border border-slate-700/50 disabled:opacity-40 disabled:cursor-not-allowed hover:bg-white/10 transition-colors"
                style={{ top: '96px', left: '10px' }}
            >
                <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke={myPosition ? '#8b5cf6' : '#94a3b8'} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <circle cx="12" cy="12" r="3" />
                    <path d="M12 2v3M12 19v3M2 12h3M19 12h3" />
                </svg>
            </button>

            {/* Realtime connection status badge */}
            <div className="fixed top-6 right-6 z-[9999] glass-panel px-3 py-1.5 rounded-lg shadow-lg text-xs border border-slate-700/50">
                {realtimeError ? (
                    <span className="text-red-400">&#9888; Realtime API unreachable</span>
                ) : (
                    <span className="text-gray-200">
                        <span className="text-green-500">&#9679;</span> {realVehicles.length} live vehicle{realVehicles.length === 1 ? '' : 's'}
                        {realtimeLastFetched && (
                            <span className="text-slate-300"> &middot; updated {realtimeLastFetched.toLocaleTimeString()}</span>
                        )}
                    </span>
                )}
            </div>

            {/* Route Legend */}
            {data && data.routes && (
                <div className="fixed bottom-4 right-4 z-[9999] glass-panel p-4 rounded-xl shadow-2xl max-h-[40vh] overflow-y-auto w-[320px] border border-slate-700/50">
                    <h3 className="text-white font-bold mb-2 text-sm border-b border-gray-600 pb-1 flex justify-between items-center">
                        Route Guide
                        <span className="text-xs text-slate-200 font-normal">{data.routes.length} Routes</span>
                    </h3>
                    <div className="space-y-2">
                        {data.routes.map(route => (
                            <div key={route.id} className="flex items-start gap-2 text-xs hover:bg-white/5 p-1 rounded transition-colors group">
                                <div
                                    className="w-3 h-3 rounded-full shadow-sm flex-shrink-0 mt-0.5"
                                    style={{ backgroundColor: route.color }}
                                />
                                <div className="flex flex-col w-full">
                                    <div className="flex items-baseline gap-2 flex-wrap">
                                        <span className="text-gray-200 font-bold group-hover:text-white transition-colors whitespace-nowrap min-w-[30px]">
                                            {route.shortName}
                                        </span>
                                        <span className="text-slate-200 text-xs leading-tight group-hover:text-white transition-colors pt-0.5">
                                            {route.longName}
                                        </span>
                                    </div>
                                    {(() => {
                                        const nextTrip = getNextRouteTrip(route.id, data.schedule, data.calendar, currentTime);
                                        if (nextTrip) {
                                            return (
                                                <div className="flex items-center gap-3 mt-1 text-xs font-mono border-t border-slate-600 pt-1 w-full text-slate-200">
                                                    <span className="flex items-center gap-1">
                                                        <span className="w-1 h-1 rounded-full bg-green-500"></span>
                                                        Dep: <span className="text-gray-200">{nextTrip.startTime}</span>
                                                    </span>
                                                    <span className="flex items-center gap-1">
                                                        <span className="w-1 h-1 rounded-full bg-blue-500"></span>
                                                        Arr: <span className="text-gray-200">{nextTrip.endTime}</span>
                                                    </span>
                                                </div>
                                            );
                                        } else {
                                            return (
                                                <div className="mt-1 text-xs text-slate-300 italic border-t border-slate-600 pt-1">
                                                    End of Service
                                                </div>
                                            );
                                        }
                                    })()}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}
