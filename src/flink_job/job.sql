-- Flink SQL Job for CDC Database Migration
-- This job processes CDC events from Kafka and writes enriched data to PostgreSQL

-- =============================================================================
-- SETUP AND CONFIGURATION
-- =============================================================================

-- Set execution mode
SET 'execution.runtime-mode' = 'streaming';

-- Enable checkpointing for exactly-once processing
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.timeout' = '10min';
SET 'execution.checkpointing.min-pause' = '500';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';

-- Configure table execution
SET 'table.exec.resource.default-parallelism' = '2';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '1000';

-- =============================================================================
-- CREATE KAFKA SOURCE TABLE
-- =============================================================================

CREATE TABLE cdc_events (
    id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    postal_code STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    source_system STRING,
    cdc_operation STRING,
    cdc_timestamp TIMESTAMP(3),
    cdc_sequence BIGINT,
    cdc_source STRING,
    cdc_schema STRING,
    cdc_table STRING,
    processing_notes STRING,
    -- Watermark for event time processing
    proc_time AS PROCTIME(),
    event_time AS cdc_timestamp,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'cdc-events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-cdc-migration',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- =============================================================================
-- CREATE POSTGRESQL SINK TABLE
-- =============================================================================

CREATE TABLE users_enriched (
    id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    email_domain STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    postal_code STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    source_system STRING,
    migration_timestamp TIMESTAMP(3),
    cdc_operation STRING,
    full_name STRING,
    email_category STRING,
    inferred_country STRING,
    is_valid_email_flag BOOLEAN,
    normalized_email STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/migration_db',
    'table-name' = 'users_enriched',
    'driver' = 'org.postgresql.Driver',
    'username' = 'postgres',
    'password' = 'postgres',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

-- =============================================================================
-- CREATE INTERMEDIATE PROCESSING TABLE
-- =============================================================================

CREATE TABLE processed_events (
    id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    email_domain STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    country STRING,
    postal_code STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    source_system STRING,
    migration_timestamp TIMESTAMP(3),
    cdc_operation STRING,
    full_name STRING,
    email_category STRING,
    inferred_country STRING,
    is_valid_email_flag BOOLEAN,
    normalized_email STRING,
    processing_timestamp TIMESTAMP(3),
    event_time TIMESTAMP(3),
    proc_time TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- =============================================================================
-- DATA PROCESSING AND ENRICHMENT
-- =============================================================================

-- Insert enriched data into the processing table
INSERT INTO processed_events
SELECT 
    id,
    first_name,
    last_name,
    email,
    -- Extract email domain using Python UDF
    extract_email_domain(email) as email_domain,
    phone,
    address,
    city,
    state,
    country,
    postal_code,
    created_at,
    updated_at,
    source_system,
    CURRENT_TIMESTAMP as migration_timestamp,
    cdc_operation,
    -- Create full name
    CONCAT(first_name, ' ', last_name) as full_name,
    -- Categorize email domain
    categorize_email_domain(extract_email_domain(email)) as email_category,
    -- Infer country from postal code if country is missing
    CASE 
        WHEN country IS NULL OR country = '' THEN extract_country_from_postal_code(postal_code)
        ELSE country
    END as inferred_country,
    -- Validate email
    is_valid_email(email) as is_valid_email_flag,
    -- Normalize email
    normalize_email(email) as normalized_email,
    CURRENT_TIMESTAMP as processing_timestamp,
    event_time,
    proc_time
FROM cdc_events
WHERE 
    -- Filter out invalid records
    id IS NOT NULL 
    AND first_name IS NOT NULL 
    AND last_name IS NOT NULL
    AND email IS NOT NULL
    AND is_valid_email(email) = true;

-- =============================================================================
-- WRITE TO POSTGRESQL
-- =============================================================================

-- Insert enriched data into PostgreSQL
INSERT INTO users_enriched
SELECT 
    id,
    first_name,
    last_name,
    email,
    email_domain,
    phone,
    address,
    city,
    state,
    COALESCE(inferred_country, country) as country,
    postal_code,
    created_at,
    updated_at,
    source_system,
    migration_timestamp,
    cdc_operation,
    full_name,
    email_category,
    COALESCE(inferred_country, country) as inferred_country,
    is_valid_email_flag,
    normalized_email
FROM processed_events;

-- =============================================================================
-- MONITORING QUERIES (Optional - for debugging and monitoring)
-- =============================================================================

-- Create a monitoring view for real-time statistics
CREATE VIEW migration_stats AS
SELECT 
    cdc_operation,
    email_category,
    COUNT(*) as record_count,
    COUNT(DISTINCT email_domain) as unique_domains,
    COUNT(DISTINCT country) as unique_countries,
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE) as window_end
FROM processed_events
GROUP BY 
    cdc_operation,
    email_category,
    TUMBLE(event_time, INTERVAL '1' MINUTE);

-- Create a view for error monitoring
CREATE VIEW error_events AS
SELECT 
    id,
    email,
    cdc_operation,
    event_time,
    'Invalid email format' as error_type
FROM cdc_events
WHERE 
    id IS NOT NULL 
    AND (first_name IS NULL OR last_name IS NULL OR email IS NULL OR is_valid_email(email) = false);

-- =============================================================================
-- ADDITIONAL ANALYTICS QUERIES
-- =============================================================================

-- Top email domains by volume
CREATE VIEW top_email_domains AS
SELECT 
    email_domain,
    email_category,
    COUNT(*) as user_count,
    COUNT(DISTINCT country) as country_count,
    MIN(created_at) as first_user,
    MAX(created_at) as last_user
FROM processed_events
WHERE email_domain IS NOT NULL
GROUP BY email_domain, email_category
ORDER BY user_count DESC;

-- Geographic distribution
CREATE VIEW geographic_distribution AS
SELECT 
    country,
    state,
    COUNT(*) as user_count,
    COUNT(DISTINCT email_domain) as domain_count,
    AVG(CASE WHEN is_valid_email_flag THEN 1.0 ELSE 0.0 END) as email_validity_rate
FROM processed_events
WHERE country IS NOT NULL
GROUP BY country, state
ORDER BY user_count DESC;

-- Processing performance metrics
CREATE VIEW processing_metrics AS
SELECT 
    TUMBLE_START(processing_timestamp, INTERVAL '1' MINUTE) as window_start,
    TUMBLE_END(processing_timestamp, INTERVAL '1' MINUTE) as window_end,
    COUNT(*) as events_processed,
    COUNT(DISTINCT cdc_operation) as operation_types,
    AVG(EXTRACT(EPOCH FROM (processing_timestamp - event_time))) as avg_processing_latency_seconds,
    COUNT(CASE WHEN is_valid_email_flag THEN 1 END) as valid_emails,
    COUNT(CASE WHEN is_valid_email_flag = false THEN 1 END) as invalid_emails
FROM processed_events
GROUP BY TUMBLE(processing_timestamp, INTERVAL '1' MINUTE)
ORDER BY window_start DESC;