-- =============================================================
-- init_db.sql
-- Runs automatically when the Postgres container first starts.
-- Creates the raw ingestion layer and Airflow metadata schema.
-- =============================================================

-- =============================================================
-- RAW LAYER
-- Exactly what arrives from the GTFS Realtime API.
-- No cleaning, no transformation — preserves source truth.
-- =============================================================

-- Each fetch run is logged here first
CREATE TABLE IF NOT EXISTS raw_feed_fetches (
    fetch_id        BIGSERIAL PRIMARY KEY,
    feed_name       TEXT        NOT NULL,           -- e.g. 'london_buses'
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    record_count    INT,
    status          TEXT        NOT NULL,           -- 'success' | 'error'
    error_message   TEXT,
    duration_ms     INT
);

-- Raw trip updates: scheduled vs actual arrival/departure
CREATE TABLE IF NOT EXISTS raw_trip_updates (
    id              BIGSERIAL   PRIMARY KEY,
    fetch_id        BIGINT      REFERENCES raw_feed_fetches(fetch_id),
    fetched_at      TIMESTAMPTZ NOT NULL,

    -- GTFS fields (snake_case of protobuf field names)
    trip_id         TEXT,
    route_id        TEXT,
    direction_id    INT,
    start_time      TEXT,                           -- "HH:MM:SS" string from feed
    start_date      TEXT,                           -- "YYYYMMDD" string from feed
    schedule_relationship TEXT,                     -- SCHEDULED | ADDED | UNSCHEDULED

    -- Stop-level update (one row per stop)
    stop_sequence   INT,
    stop_id         TEXT,
    arrival_delay   INT,                            -- seconds, negative = early
    departure_delay INT,                            -- seconds
    arrival_time    BIGINT,                         -- unix timestamp
    departure_time  BIGINT,                         -- unix timestamp

    -- Pipeline metadata
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Raw vehicle positions
CREATE TABLE IF NOT EXISTS raw_vehicle_positions (
    id              BIGSERIAL   PRIMARY KEY,
    fetch_id        BIGINT      REFERENCES raw_feed_fetches(fetch_id),
    fetched_at      TIMESTAMPTZ NOT NULL,

    vehicle_id      TEXT,
    trip_id         TEXT,
    route_id        TEXT,
    latitude        NUMERIC(9, 6),
    longitude       NUMERIC(9, 6),
    bearing         NUMERIC(5, 1),
    speed           NUMERIC(6, 2),                 -- m/s
    current_stop_sequence INT,
    current_status  TEXT,                           -- INCOMING_AT | STOPPED_AT | IN_TRANSIT_TO
    timestamp       BIGINT,                         -- unix timestamp from vehicle

    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================
-- INDEXES — optimise the most common staging queries
-- =============================================================

-- Dedup + staging queries filter on fetched_at ranges
CREATE INDEX IF NOT EXISTS idx_raw_trip_updates_fetched
    ON raw_trip_updates (fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_trip_updates_trip_route
    ON raw_trip_updates (trip_id, route_id);

CREATE INDEX IF NOT EXISTS idx_raw_trip_updates_fetch_id
    ON raw_trip_updates (fetch_id);

CREATE INDEX IF NOT EXISTS idx_raw_vehicle_positions_fetched
    ON raw_vehicle_positions (fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_vehicle_positions_vehicle
    ON raw_vehicle_positions (vehicle_id, trip_id);

CREATE INDEX IF NOT EXISTS idx_raw_feed_fetches_name_time
    ON raw_feed_fetches (feed_name, fetched_at DESC);

-- =============================================================
-- STAGING LAYER (created empty — dbt populates these)
-- =============================================================

CREATE TABLE IF NOT EXISTS stg_trip_updates (
    surrogate_key   TEXT        PRIMARY KEY,        -- md5 of trip_id+stop_id+fetched_at
    trip_id         TEXT        NOT NULL,
    route_id        TEXT        NOT NULL,
    direction_id    INT,
    service_date    DATE,                           -- parsed from start_date
    stop_id         TEXT        NOT NULL,
    stop_sequence   INT,
    arrival_delay_s INT,                            -- cleaned, nulls handled
    departure_delay_s INT,
    is_delayed      BOOLEAN     GENERATED ALWAYS AS (arrival_delay_s > 60) STORED,
    is_early        BOOLEAN     GENERATED ALWAYS AS (arrival_delay_s < -30) STORED,
    fetched_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS stg_vehicle_positions (
    surrogate_key   TEXT        PRIMARY KEY,
    vehicle_id      TEXT        NOT NULL,
    trip_id         TEXT,
    route_id        TEXT,
    latitude        NUMERIC(9, 6),
    longitude       NUMERIC(9, 6),
    speed_ms        NUMERIC(6, 2),
    current_status  TEXT,
    position_time   TIMESTAMPTZ,
    fetched_at      TIMESTAMPTZ NOT NULL
);

-- =============================================================
-- MART LAYER (created empty — dbt populates these)
-- =============================================================

-- Per-route delay summary (refreshed each DAG run)
CREATE TABLE IF NOT EXISTS mart_route_delays (
    route_id            TEXT        NOT NULL,
    service_date        DATE        NOT NULL,
    direction_id        INT,
    total_updates       INT,
    delayed_count       INT,
    early_count         INT,
    on_time_count       INT,
    avg_delay_s         NUMERIC(8, 2),
    p50_delay_s         NUMERIC(8, 2),              -- median
    p95_delay_s         NUMERIC(8, 2),              -- worst 5%
    max_delay_s         INT,
    delay_rate_pct      NUMERIC(5, 2),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (route_id, service_date, direction_id)
);

-- Hourly performance across all routes
CREATE TABLE IF NOT EXISTS mart_hourly_performance (
    service_date        DATE        NOT NULL,
    hour_of_day         INT         NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    total_updates       INT,
    delayed_count       INT,
    avg_delay_s         NUMERIC(8, 2),
    worst_route_id      TEXT,                       -- route with highest avg delay that hour
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (service_date, hour_of_day)
);

-- Per-stop delay hotspots
CREATE TABLE IF NOT EXISTS mart_stop_hotspots (
    stop_id             TEXT        NOT NULL,
    service_date        DATE        NOT NULL,
    route_id            TEXT        NOT NULL,
    avg_delay_s         NUMERIC(8, 2),
    delay_count         INT,
    delay_rank          INT,                        -- 1 = worst stop that day
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stop_id, service_date, route_id)
);

-- =============================================================
-- AUDIT / DATA QUALITY LOG
-- dbt tests write failures here via on-run-end hook
-- =============================================================
CREATE TABLE IF NOT EXISTS dq_test_results (
    id              BIGSERIAL   PRIMARY KEY,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dbt_test_name   TEXT        NOT NULL,
    model_name      TEXT,
    status          TEXT        NOT NULL,           -- 'pass' | 'fail' | 'warn'
    failure_count   INT         DEFAULT 0,
    message         TEXT
);
