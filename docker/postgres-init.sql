-- Initialize the target database schema
CREATE DATABASE migration_db;

\c migration_db;

-- Create the target table for migrated data
CREATE TABLE IF NOT EXISTS users_enriched (
    id BIGINT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    email_domain VARCHAR(100),
    phone VARCHAR(20),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    postal_code VARCHAR(20),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_system VARCHAR(50),
    migration_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_users_enriched_email ON users_enriched(email);
CREATE INDEX IF NOT EXISTS idx_users_enriched_email_domain ON users_enriched(email_domain);
CREATE INDEX IF NOT EXISTS idx_users_enriched_created_at ON users_enriched(created_at);

-- Create a view for analytics
CREATE OR REPLACE VIEW user_analytics AS
SELECT 
    email_domain,
    COUNT(*) as user_count,
    COUNT(DISTINCT country) as country_count,
    MIN(created_at) as first_user_created,
    MAX(created_at) as last_user_created
FROM users_enriched
GROUP BY email_domain
ORDER BY user_count DESC;