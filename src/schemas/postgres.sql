-- PostgreSQL Schema for Database Migration Simulation
-- This schema defines the target database structure for the CDC migration

-- Create the target database
CREATE DATABASE migration_db;

\c migration_db;

-- Create the main target table for enriched user data
CREATE TABLE IF NOT EXISTS users_enriched (
    id BIGINT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    email_domain VARCHAR(100),
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    source_system VARCHAR(50) DEFAULT 'legacy_system',
    migration_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cdc_operation VARCHAR(10) DEFAULT 'INSERT' -- INSERT, UPDATE, DELETE
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_users_enriched_email ON users_enriched(email);
CREATE INDEX IF NOT EXISTS idx_users_enriched_email_domain ON users_enriched(email_domain);
CREATE INDEX IF NOT EXISTS idx_users_enriched_created_at ON users_enriched(created_at);
CREATE INDEX IF NOT EXISTS idx_users_enriched_updated_at ON users_enriched(updated_at);
CREATE INDEX IF NOT EXISTS idx_users_enriched_country ON users_enriched(country);
CREATE INDEX IF NOT EXISTS idx_users_enriched_cdc_operation ON users_enriched(cdc_operation);

-- Create a table to track migration metadata
CREATE TABLE IF NOT EXISTS migration_metadata (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(100),
    source_topic VARCHAR(100),
    records_processed INTEGER,
    records_successful INTEGER,
    records_failed INTEGER,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20) DEFAULT 'RUNNING', -- RUNNING, COMPLETED, FAILED
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a view for analytics and monitoring
CREATE OR REPLACE VIEW user_analytics AS
SELECT 
    email_domain,
    country,
    COUNT(*) as user_count,
    COUNT(DISTINCT state) as state_count,
    MIN(created_at) as first_user_created,
    MAX(created_at) as last_user_created,
    AVG(EXTRACT(EPOCH FROM (updated_at - created_at))/3600) as avg_hours_to_update
FROM users_enriched
WHERE cdc_operation != 'DELETE'
GROUP BY email_domain, country
ORDER BY user_count DESC;

-- Create a view for migration monitoring
CREATE OR REPLACE VIEW migration_stats AS
SELECT 
    DATE(migration_timestamp) as migration_date,
    cdc_operation,
    COUNT(*) as record_count,
    COUNT(DISTINCT email_domain) as unique_domains,
    COUNT(DISTINCT country) as unique_countries
FROM users_enriched
GROUP BY DATE(migration_timestamp), cdc_operation
ORDER BY migration_date DESC, cdc_operation;

-- Create a function to clean up old migration metadata
CREATE OR REPLACE FUNCTION cleanup_old_metadata()
RETURNS void AS $$
BEGIN
    DELETE FROM migration_metadata 
    WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '30 days'
    AND status = 'COMPLETED';
END;
$$ LANGUAGE plpgsql;

-- Create a trigger to automatically update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_users_enriched_updated_at
    BEFORE UPDATE ON users_enriched
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();