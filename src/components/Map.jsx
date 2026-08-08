import React, { useEffect, useState, useMemo } from 'react';
import { MapContainer, TileLayer, Polyline, CircleMarker, Popup, Tooltip, Marker } from 'react-leaflet';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import { loadData, getCurrentTimeSeconds, getNextArrival, getNextRouteTrip } from '../utils/gtfs';
import { useRealtimeVehicles } from '../hooks/useRealtimeVehicles';

const KotaBharuCenter = [6.1256, 102.2386];

export default function MapView() {
    const [data, setData] = useState(null);
    const [error, setError] = useState(null);
    const { vehicles: realVehicles, error: realtimeError, lastFetched: realtimeLastFetched } = useRealtimeVehicles();

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
                    center={KotaBharuCenter}
                    zoom={13}
                    scrollWheelZoom={true}
                    style={{ height: '100%', width: '100%' }}
                >
                    <TileLayer
                        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                        url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
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
                        const dotColor = isStale ? '#6b7280' : '#22c55e'; // gray if stale, green if fresh

                        const liveIcon = new L.DivIcon({
                            className: 'custom-live-bus-icon',
                            html: `<div style="position: relative; width: 26px; height: 26px;">
                                    ${isStale ? '' : `<div style="position: absolute; inset: 0; border-radius: 9999px; background-color: ${dotColor}; opacity: 0.4; animation: pulse-ring 1.6s ease-out infinite;"></div>`}
                                    <div style="position: relative; background-color: ${dotColor}; border: 2px solid white; border-radius: 9999px; width: 26px; height: 26px; display: flex; align-items: center; justify-content: center; box-shadow: 0 2px 4px rgba(0,0,0,0.4);">
                                        <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
                                            <path d="M8 6v6"/><path d="M15 6v6"/><path d="M2 12h19.6"/>
                                            <path d="M18 18h3s.5-1.7.8-2.8c.1-.4.2-.8.2-1.2 0-.4-.1-.8-.2-1.2l-1.4-5C20.1 6.8 19.1 6 18 6H4a2 2 0 0 0-2 2v10h3"/>
                                            <circle cx="7" cy="18" r="2"/><path d="M9 18h5"/><circle cx="16" cy="18" r="2"/>
                                        </svg>
                                    </div>
                                   </div>`,
                            iconSize: [26, 26],
                            iconAnchor: [13, 13]
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
                                            <span className="text-[9px] font-bold uppercase tracking-wide px-1.5 py-0.5 rounded-full bg-green-100 text-green-700">Live GPS</span>
                                        </div>
                                        {v.license_plate && <div className="text-xs text-gray-600">Plate: {v.license_plate}</div>}
                                        <div className={`text-xs mt-1 ${isStale ? 'text-gray-500' : 'text-green-600 font-semibold'}`}>
                                            {isStale
                                                ? `Last seen ${Math.round(ageMinutes)} min ago — likely idle`
                                                : `Updated ${Math.round(ageMinutes)} min ago`}
                                        </div>
                                        {!v.trip_id && (
                                            <div className="text-xs text-gray-400 italic mt-0.5">No trip assigned (feed doesn't report one yet)</div>
                                        )}
                                    </div>
                                </Popup>
                            </Marker>
                        );
                    })}
                </MapContainer>
            </div>

            {/* Realtime connection status badge */}
            <div className="fixed top-6 right-6 z-[9999] glass-panel px-3 py-1.5 rounded-lg shadow-lg text-xs border border-slate-700/50">
                {realtimeError ? (
                    <span className="text-red-400">&#9888; Realtime API unreachable</span>
                ) : (
                    <span className="text-gray-200">
                        <span className="text-green-500">&#9679;</span> {realVehicles.length} live vehicle{realVehicles.length === 1 ? '' : 's'}
                        {realtimeLastFetched && (
                            <span className="text-gray-500"> &middot; updated {realtimeLastFetched.toLocaleTimeString()}</span>
                        )}
                    </span>
                )}
            </div>

            {/* Route Legend */}
            {data && data.routes && (
                <div className="fixed bottom-4 right-4 z-[9999] glass-panel p-4 rounded-xl shadow-2xl max-h-[40vh] overflow-y-auto w-[320px] border border-slate-700/50">
                    <h3 className="text-white font-bold mb-2 text-sm border-b border-gray-600 pb-1 flex justify-between items-center">
                        Route Guide
                        <span className="text-[10px] text-gray-400 font-normal">{data.routes.length} Routes</span>
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
                                        <span className="text-gray-400 text-[10px] leading-tight group-hover:text-gray-300 transition-colors pt-0.5">
                                            {route.longName}
                                        </span>
                                    </div>
                                    {(() => {
                                        const nextTrip = getNextRouteTrip(route.id, data.schedule, data.calendar, currentTime);
                                        if (nextTrip) {
                                            return (
                                                <div className="flex items-center gap-3 mt-1 text-[10px] font-mono border-t border-white/5 pt-1 w-full text-gray-400">
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
                                                <div className="mt-1 text-[10px] text-gray-500 italic border-t border-white/5 pt-1">
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
