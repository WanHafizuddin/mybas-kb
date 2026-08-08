// Loaders
export async function loadData() {
    const [routes, stops, shapes, schedule, calendar] = await Promise.all([
        fetch('/data/routes.json').then(r => r.json()),
        fetch('/data/stops.json').then(r => r.json()),
        fetch('/data/shapes.json').then(r => r.json()),
        fetch('/data/schedule.json').then(r => r.json()),
        fetch('/data/calendar.json').then(r => r.json()),
    ]);

    // Enhance routes with distinct colors
    const coloredRoutes = assignRouteColors(routes);

    // Pre-calculate schedules for each stop
    const stopSchedules = precalculateStopSchedules(schedule, coloredRoutes, calendar);

    return { routes: coloredRoutes, stops, shapes, schedule, calendar, stopSchedules };
}

// Helper: Build a map of stopId -> sorted array of arrivals
function precalculateStopSchedules(schedule, routes, calendar) {
    const stopSchedules = {};

    Object.keys(schedule).forEach(routeId => {
        const route = routes.find(r => r.id === routeId);
        if (!route) return;

        schedule[routeId].forEach(trip => {
            // We include all trips for now, filtering by active day happens at runtime lookup usually, 
            // but to optimize "next bus" we might want to filter by serviceId later.
            // For static view, we'll store all and filter in getNextArrival.
            trip.stops.forEach(stop => {
                if (!stopSchedules[stop.stopId]) {
                    stopSchedules[stop.stopId] = [];
                }
                stopSchedules[stop.stopId].push({
                    time: timeToSeconds(stop.departure),
                    route: route,
                    tripId: trip.tripId,
                    serviceId: trip.serviceId,
                    headsign: trip.headsign
                });
            });
        });
    });

    // Sort arrivals by time for each stop
    Object.values(stopSchedules).forEach(arrivals => {
        arrivals.sort((a, b) => a.time - b.time);
    });

    return stopSchedules;
}

// Helper: Get next arrival for a stop
export function getNextArrival(stopId, currentTime, stopSchedules, calendar) {
    const arrivals = stopSchedules[stopId];
    if (!arrivals) return null;

    const now = new Date(); // Need actual date for service check, strictly we should use simulation date but "today" is implied

    // Find first arrival after currentTime that is active today
    // Linear search is fine here as stops don't have thousands of daily trips
    const next = arrivals.find(arrival => {
        return arrival.time > currentTime && isServiceActive(arrival.serviceId, calendar, now);
    });

    return next;
}
// Helper: Get next scheduled trip for a route
export function getNextRouteTrip(routeId, schedule, calendar, currentTime) {
    const routeTrips = schedule[routeId];
    if (!routeTrips) return null;

    const now = new Date();

    // Filter active trips
    const activeTrips = routeTrips.filter(trip => isServiceActive(trip.serviceId, calendar, now));

    // Sort by departure time of the first stop
    activeTrips.sort((a, b) => {
        const t1 = timeToSeconds(a.stops[0].departure);
        const t2 = timeToSeconds(b.stops[0].departure);
        return t1 - t2;
    });

    // Find first trip after currentTime
    const nextTrip = activeTrips.find(trip => {
        const startTime = timeToSeconds(trip.stops[0].departure);
        return startTime > currentTime;
    });

    if (!nextTrip) return null;

    const firstStop = nextTrip.stops[0];
    const lastStop = nextTrip.stops[nextTrip.stops.length - 1];

    return {
        tripId: nextTrip.tripId,
        serviceId: nextTrip.serviceId,
        startTime: firstStop.departure.substring(0, 5), // "HH:MM"
        endTime: lastStop.arrival.substring(0, 5),      // "HH:MM"
        headsign: nextTrip.headsign
    };
}


// Helper: Assign distinct colors to routes
function assignRouteColors(routes) {
    const palette = [
        '#FF3B30', // Red
        '#007AFF', // Blue
        '#34C759', // Green
        '#FF9500', // Orange
        '#AF52DE', // Purple
        '#5AC8FA', // Cyan
        '#FF2D55', // Pink
        '#5856D6', // Indigo
        '#FFCC00', // Yellow
        '#8E8E93', // Gray
    ];

    return routes.map((route, index) => ({
        ...route,
        color: palette[index % palette.length]
    }));
}

// Helper to check if a service is active today
export function isServiceActive(serviceId, calendar, date) {
    const service = calendar[serviceId];
    if (!service) return false;

    // Check date range
    const nowStr = date.toISOString().split('T')[0].replace(/-/g, '');
    if (nowStr < service.startDate || nowStr > service.endDate) return false;

    // Check day of week
    const day = date.getDay(); // 0 = Sunday
    const days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
    return service[days[day]];
}

// Convert "HH:mm:ss" to seconds from midnight
export function timeToSeconds(timeStr) {
    const [h, m, s] = timeStr.split(':').map(Number);
    return h * 3600 + m * 60 + s;
}

// Get current time in seconds
export function getCurrentTimeSeconds() {
    const now = new Date();
    return now.getHours() * 3600 + now.getMinutes() * 60 + now.getSeconds();
}



