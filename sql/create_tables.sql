-- DDL script to create target schema for India Air Quality Index readings
-- This script is executed during target database initialization or migration

CREATE TABLE IF NOT EXISTS air_quality_readings (
    composite_key VARCHAR(255) PRIMARY KEY, -- Unique key built from (station + pollutant_id + last_update)
    country VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    station TEXT NOT NULL,
    last_update TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    pollutant_id VARCHAR(50) NOT NULL,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    avg_value DOUBLE PRECISION,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexing for performance on analytical queries
CREATE INDEX IF NOT EXISTS idx_air_quality_state_city ON air_quality_readings (state, city);
CREATE INDEX IF NOT EXISTS idx_air_quality_last_update ON air_quality_readings (last_update);
CREATE INDEX IF NOT EXISTS idx_air_quality_pollutant ON air_quality_readings (pollutant_id);
