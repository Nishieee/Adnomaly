-- Initialize Adnomaly database schema
-- This script runs when the PostgreSQL container starts for the first time

-- Create tables for storing clickstream data
CREATE TABLE IF NOT EXISTS clickstream_events (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    user_id_hash VARCHAR(64) NOT NULL,
    ad_id VARCHAR(20) NOT NULL,
    campaign_id VARCHAR(20) NOT NULL,
    geo VARCHAR(2) NOT NULL,
    platform VARCHAR(10) NOT NULL,
    user_agent TEXT NOT NULL,
    cpc DECIMAL(10,3) NOT NULL,
    ctr DECIMAL(10,4) NOT NULL,
    conversion INTEGER NOT NULL,
    bounce_rate DECIMAL(10,3) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_clickstream_timestamp ON clickstream_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_clickstream_geo ON clickstream_events(geo);
CREATE INDEX IF NOT EXISTS idx_clickstream_platform ON clickstream_events(platform);
CREATE INDEX IF NOT EXISTS idx_clickstream_campaign ON clickstream_events(campaign_id);
CREATE INDEX IF NOT EXISTS idx_clickstream_ad ON clickstream_events(ad_id);

-- Create table for aggregated features
CREATE TABLE IF NOT EXISTS feature_aggregates (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    geo VARCHAR(2),
    platform VARCHAR(10),
    avg_ctr DECIMAL(10,4) NOT NULL,
    avg_bounce_rate DECIMAL(10,3) NOT NULL,
    event_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for feature aggregates
CREATE INDEX IF NOT EXISTS idx_features_window ON feature_aggregates(window_start, window_end);
CREATE INDEX IF NOT EXISTS idx_features_geo ON feature_aggregates(geo);
CREATE INDEX IF NOT EXISTS idx_features_platform ON feature_aggregates(platform);

-- Create table for anomaly detection results
CREATE TABLE IF NOT EXISTS anomaly_results (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    geo VARCHAR(2),
    platform VARCHAR(10),
    anomaly_score DECIMAL(10,6),
    anomaly_type VARCHAR(50),
    is_anomaly BOOLEAN DEFAULT FALSE,
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for anomaly results
CREATE INDEX IF NOT EXISTS idx_anomaly_timestamp ON anomaly_results(timestamp);
CREATE INDEX IF NOT EXISTS idx_anomaly_geo ON anomaly_results(geo);
CREATE INDEX IF NOT EXISTS idx_anomaly_is_anomaly ON anomaly_results(is_anomaly);

-- Create a view for recent events (last 24 hours)
CREATE OR REPLACE VIEW recent_events AS
SELECT * FROM clickstream_events 
WHERE timestamp >= NOW() - INTERVAL '24 hours'
ORDER BY timestamp DESC;

-- Create a view for daily statistics
CREATE OR REPLACE VIEW daily_stats AS
SELECT 
    DATE(timestamp) as date,
    geo,
    platform,
    COUNT(*) as total_events,
    AVG(ctr) as avg_ctr,
    AVG(bounce_rate) as avg_bounce_rate,
    AVG(cpc) as avg_cpc,
    SUM(conversion) as total_conversions,
    COUNT(CASE WHEN conversion = 1 THEN 1 END) as conversion_count
FROM clickstream_events 
GROUP BY DATE(timestamp), geo, platform
ORDER BY date DESC, geo, platform;

-- Grant permissions to the application user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO adnomaly_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO adnomaly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO adnomaly_user;
