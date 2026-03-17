CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS fact_traffic_events (
    time TIMESTAMPTZ NOT NULL,
    camera_id VARCHAR(64) NOT NULL,
    object_type VARCHAR(32) NOT NULL,
    direction VARCHAR(8) NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 1,
    event_id UUID,
    confidence DOUBLE PRECISION,
    location VARCHAR(128),
    lane_id INTEGER,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('fact_traffic_events', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_fact_traffic_events_camera_time
    ON fact_traffic_events (camera_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_fact_traffic_events_object_direction_time
    ON fact_traffic_events (object_type, direction, time DESC);

CREATE TABLE IF NOT EXISTS fact_traffic_agg_1m (
    bucket TIMESTAMPTZ NOT NULL,
    camera_id VARCHAR(64) NOT NULL,
    object_type VARCHAR(32) NOT NULL,
    direction VARCHAR(8) NOT NULL,
    location VARCHAR(128) NOT NULL,
    lane_id INTEGER NOT NULL,
    total_events BIGINT NOT NULL,
    avg_confidence DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bucket, camera_id, object_type, direction, location, lane_id)
);

SELECT create_hypertable('fact_traffic_agg_1m', 'bucket', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_fact_traffic_agg_1m_camera_bucket
    ON fact_traffic_agg_1m (camera_id, bucket DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS agg_traffic_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    camera_id,
    object_type,
    direction,
    SUM(event_count) AS total_events
FROM fact_traffic_events
GROUP BY bucket, camera_id, object_type, direction;

DO $$
BEGIN
    PERFORM add_continuous_aggregate_policy(
        'agg_traffic_1m',
        start_offset => INTERVAL '2 hours',
        end_offset => INTERVAL '1 minute',
        schedule_interval => INTERVAL '1 minute'
    );
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'Policy already exists, skipping';
END
$$;
