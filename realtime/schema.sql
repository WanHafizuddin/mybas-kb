-- vehicle_positions: append-only log of GTFS-Realtime vehicle-position polls
-- for BAS.MY Kota Bharu. One row per vehicle entity per poll -- never
-- overwritten -- so history accumulates for later ETA/delay modeling.
--
-- Columns mirror exactly what realtime/inspect_feed.py confirmed the feed
-- can carry (see that script's output for what is/isn't populated in
-- practice). Only entity_id, latitude, longitude, and collected_at are
-- NOT NULL: those are the fields the GTFS-Realtime spec effectively
-- guarantees for any vehicle entity. Everything else is optional because
-- we've already observed it can be absent (trip_id, route_id, stop_id,
-- speed, etc. were all empty in our first live poll).

CREATE TABLE IF NOT EXISTS vehicle_positions (
    id                      BIGSERIAL PRIMARY KEY,

    entity_id               TEXT NOT NULL,
    vehicle_id              TEXT,
    vehicle_label           TEXT,
    license_plate           TEXT,

    latitude                DOUBLE PRECISION NOT NULL,
    longitude               DOUBLE PRECISION NOT NULL,
    bearing                 DOUBLE PRECISION,
    speed                   DOUBLE PRECISION,

    trip_id                 TEXT,
    route_id                TEXT,
    direction_id            SMALLINT,
    current_stop_sequence   INTEGER,
    stop_id                 TEXT,
    current_status          TEXT,
    congestion_level        TEXT,
    occupancy_status        TEXT,
    occupancy_percentage    INTEGER,

    vehicle_timestamp       TIMESTAMPTZ,   -- vehicle.timestamp, as reported by the feed
    feed_header_timestamp   TIMESTAMPTZ,   -- feed.header.timestamp, when the feed was generated
    collected_at            TIMESTAMPTZ NOT NULL DEFAULT now()  -- when our collector polled
);

CREATE INDEX IF NOT EXISTS idx_vehicle_positions_vehicle_id_collected_at
    ON vehicle_positions (vehicle_id, collected_at);

CREATE INDEX IF NOT EXISTS idx_vehicle_positions_trip_id
    ON vehicle_positions (trip_id)
    WHERE trip_id IS NOT NULL;
