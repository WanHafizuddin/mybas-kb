import fs from 'fs';
import path from 'path';
import https from 'https';
import AdmZip from 'adm-zip';
import csv from 'csv-parser';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const GTFS_URL = 'https://api.data.gov.my/gtfs-static/mybas-kota-bharu';
const DATA_DIR = path.join(__dirname, '../public/data');
const TEMP_DIR = path.join(__dirname, '../temp_gtfs');

// Ensure directories exist
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(TEMP_DIR)) fs.mkdirSync(TEMP_DIR, { recursive: true });

async function downloadFile(url, dest) {
    return new Promise((resolve, reject) => {
        const getFile = (currentUrl) => {
            https.get(currentUrl, (response) => {
                // Handle redirects
                if (response.statusCode === 301 || response.statusCode === 302) {
                    const location = response.headers.location;
                    const nextUrl = new URL(location, currentUrl).toString();
                    console.log(`Redirecting to: ${nextUrl}`);
                    return getFile(nextUrl);
                }

                if (response.statusCode !== 200) {
                    reject(new Error(`Failed to download: ${response.statusCode}`));
                    return;
                }

                const file = fs.createWriteStream(dest);
                response.pipe(file);
                file.on('finish', () => {
                    file.close(() => {
                        console.log('Download completed.');
                        resolve();
                    });
                });
            }).on('error', (err) => {
                fs.unlink(dest, () => { });
                reject(err);
            });
        };
        getFile(url);
    });
}

function parseCSV(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => resolve(results))
            .on('error', (err) => reject(err));
    });
}

// Guards against silently overwriting good public/data/*.json with a broken
// or truncated upstream download -- this has already happened once (the
// operator's GTFS export briefly contained header-only CSVs with zero rows).
function assertNotSuspiciouslySmaller(newRoutes, newStops) {
    const routesPath = path.join(DATA_DIR, 'routes.json');
    const stopsPath = path.join(DATA_DIR, 'stops.json');
    if (!fs.existsSync(routesPath) || !fs.existsSync(stopsPath)) return; // first run, nothing to compare

    const existingRoutes = JSON.parse(fs.readFileSync(routesPath, 'utf-8'));
    const existingStops = JSON.parse(fs.readFileSync(stopsPath, 'utf-8'));

    const checks = [
        { name: 'routes', existing: existingRoutes.length, incoming: newRoutes.length },
        { name: 'stops', existing: Object.keys(existingStops).length, incoming: newStops.length },
    ];

    for (const { name, existing, incoming } of checks) {
        if (existing === 0) continue; // nothing worth protecting
        if (incoming === 0 || incoming < existing / 2) {
            throw new Error(
                `Refusing to overwrite public/data/*.json: new ${name} count (${incoming}) is suspiciously ` +
                `smaller than the existing count (${existing}). This usually means the upstream GTFS feed is ` +
                `currently broken or incomplete, not that the local data is stale. Aborting without writing any files.`
            );
        }
    }
}

async function processGTFS() {
    try {
        console.log('Downloading GTFS data...');
        const zipPath = path.join(TEMP_DIR, 'gtfs.zip');
        await downloadFile(GTFS_URL, zipPath);

        console.log('Extracting zip...');
        const zip = new AdmZip(zipPath);
        zip.extractAllTo(TEMP_DIR, true);

        console.log('Parsing CSV files...');
        const routes = await parseCSV(path.join(TEMP_DIR, 'routes.txt'));
        const trips = await parseCSV(path.join(TEMP_DIR, 'trips.txt'));
        const stops = await parseCSV(path.join(TEMP_DIR, 'stops.txt'));
        const shapes = await parseCSV(path.join(TEMP_DIR, 'shapes.txt'));
        const stopTimes = await parseCSV(path.join(TEMP_DIR, 'stop_times.txt'));
        const calendar = await parseCSV(path.join(TEMP_DIR, 'calendar.txt'));

        assertNotSuspiciouslySmaller(routes, stops);

        // 1. Process Stops
        console.log('Processing stops...');
        const stopsMap = {};
        stops.forEach(stop => {
            stopsMap[stop.stop_id] = {
                name: stop.stop_name,
                lat: parseFloat(stop.stop_lat),
                lon: parseFloat(stop.stop_lon)
            };
        });
        fs.writeFileSync(path.join(DATA_DIR, 'stops.json'), JSON.stringify(stopsMap));

        // 2. Process Shapes (Route Geometries)
        console.log('Processing shapes...');
        const shapesMap = {};
        shapes.forEach(shape => {
            if (!shapesMap[shape.shape_id]) shapesMap[shape.shape_id] = [];
            shapesMap[shape.shape_id].push({
                lat: parseFloat(shape.shape_pt_lat),
                lon: parseFloat(shape.shape_pt_lon),
                seq: parseInt(shape.shape_pt_sequence)
            });
        });
        // Sort shapes by sequence
        Object.values(shapesMap).forEach(arr => arr.sort((a, b) => a.seq - b.seq));
        // Flatten to array of [lat, lon]
        const simplifiedShapes = {};
        Object.keys(shapesMap).forEach(id => {
            simplifiedShapes[id] = shapesMap[id].map(p => [p.lat, p.lon]);
        });
        fs.writeFileSync(path.join(DATA_DIR, 'shapes.json'), JSON.stringify(simplifiedShapes));

        // 3. Process Routes & Link to Trips
        console.log('Processing routes...');
        const routesData = routes.map(route => ({
            id: route.route_id,
            shortName: route.route_short_name,
            longName: route.route_long_name,
            color: route.route_color ? `#${route.route_color}` : '#3b82f6',
            textColor: route.route_text_color ? `#${route.route_text_color}` : '#ffffff'
        }));
        fs.writeFileSync(path.join(DATA_DIR, 'routes.json'), JSON.stringify(routesData));

        // 4. Process Schedule (Trips + Stop Times)
        console.log('Processing schedule...');
        // We need to group by route to make frontend lookups easier
        const schedule = {};

        // Create a calendar lookup
        const calendarMap = {};
        calendar.forEach(c => {
            calendarMap[c.service_id] = {
                monday: c.monday === '1',
                tuesday: c.tuesday === '1',
                wednesday: c.wednesday === '1',
                thursday: c.thursday === '1',
                friday: c.friday === '1',
                saturday: c.saturday === '1',
                sunday: c.sunday === '1',
                startDate: c.start_date,
                endDate: c.end_date
            };
        });

        // Helper to sort stop times
        const sortedStopTimes = {};
        stopTimes.forEach(st => {
            if (!sortedStopTimes[st.trip_id]) sortedStopTimes[st.trip_id] = [];
            sortedStopTimes[st.trip_id].push({
                stopId: st.stop_id,
                arrival: st.arrival_time,
                departure: st.departure_time,
                seq: parseInt(st.stop_sequence)
            });
        });
        Object.values(sortedStopTimes).forEach(arr => arr.sort((a, b) => a.seq - b.seq));

        trips.forEach(trip => {
            const routeId = trip.route_id;
            if (!schedule[routeId]) schedule[routeId] = [];

            if (sortedStopTimes[trip.trip_id]) {
                schedule[routeId].push({
                    tripId: trip.trip_id,
                    serviceId: trip.service_id,
                    shapeId: trip.shape_id,
                    headsign: trip.trip_headsign,
                    stops: sortedStopTimes[trip.trip_id]
                });
            }
        });

        fs.writeFileSync(path.join(DATA_DIR, 'schedule.json'), JSON.stringify(schedule));
        fs.writeFileSync(path.join(DATA_DIR, 'calendar.json'), JSON.stringify(calendarMap));

        console.log('GTFS processing complete!');

    } catch (error) {
        console.error('Error processing GTFS:', error);
    }
}

processGTFS();
