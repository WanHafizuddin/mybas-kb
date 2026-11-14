-- delay_predictions: one row per /predict-delay response (Phase 5).
-- NEW table only; vehicle_positions is not touched. Apply once, by hand,
-- after review:  psql "$DATABASE_URL" -f realtime/schema_predictions.sql
--
-- Used by the daily monitoring task (ml/monitor.py), which joins these rows
-- to the arrivals actually detected (trip_id, service_date, stop_sequence)
-- and alerts if the model's daily MAE drifts 30% above its test MAE.

CREATE TABLE IF NOT EXISTS delay_predictions (
    id                       BIGSERIAL PRIMARY KEY,
    requested_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    route_id                 TEXT NOT NULL,
    stop_id                  TEXT NOT NULL,
    trip_id                  TEXT,            -- the trip the prediction is for
    vehicle_id               TEXT,            -- null for timetable predictions
    service_date             DATE,            -- Kuala Lumpur service day of that trip
    stop_sequence            INTEGER,

    scheduled_arrival        TIMESTAMPTZ,
    predicted_arrival        TIMESTAMPTZ NOT NULL,
    predicted_delay_seconds  DOUBLE PRECISION,
    current_delay_seconds    DOUBLE PRECISION, -- bus's delay when predicted (live sources)

    source                   TEXT NOT NULL CHECK (source IN ('model', 'live_delay', 'timetable')),
    model_version            TEXT,            -- null unless source = 'model'
    model_dataset            TEXT,            -- 'REAL' / 'SYNTHETIC'
    features                 JSONB            -- model inputs, for debugging and drift checks
);

CREATE INDEX IF NOT EXISTS idx_delay_predictions_match
    ON delay_predictions (service_date, trip_id, stop_sequence);

CREATE INDEX IF NOT EXISTS idx_delay_predictions_requested_at
    ON delay_predictions (requested_at);
