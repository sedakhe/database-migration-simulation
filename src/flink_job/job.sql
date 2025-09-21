
-- 1. Enabling checkpoints for consistency
SET 'execution.checkpointing.interval' = '10s';  -- Flink will snapshot its state every 10s, exactly-once in Kafka context


-- 2. Defining source table: Kafka topic with CDC JSON
CREATE TABLE users_cdc_raw (
  op STRING,  -- operation: c = create, u = update, d = delete
  `before` ROW<user_id BIGINT, name STRING, email STRING, country_code STRING>,
  `after`  ROW<user_id BIGINT, name STRING, email STRING, country_code STRING>,
  timestamp_ms BIGINT,
  ts AS TO_TIMESTAMP_LTZ(timestamp_ms, 3),  -- convert ms -> TIMESTAMP(3)
  WATERMARK FOR ts AS ts - INTERVAL '10' SECOND  -- allow 10s lateness
) WITH (
  'connector' = 'kafka',
  'topic' = 'users_cdc',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);


-- 3. Normalize CDC events (flatten before/after into a single row)

-- Create (c) - only after is populated
-- Update (u) - both before and after are populated
-- Delete (d) - only before is populated

CREATE TEMPORARY VIEW users_changes AS
SELECT
  CASE WHEN op='d' THEN `before`.user_id ELSE `after`.user_id END AS user_id,
  CASE WHEN op='d' THEN `before`.name    ELSE `after`.name    END AS name,
  CASE WHEN op='d' THEN `before`.email   ELSE `after`.email   END AS email,
  CASE WHEN op='d' THEN `before`.country_code ELSE `after`.country_code END AS country_code,
  (op = 'd') AS is_deleted,
  ts
FROM users_cdc_raw;


-- 4. Deduplicating by event-time: keeping latest event per user_id
CREATE TEMPORARY VIEW users_latest AS
SELECT * FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) AS rn
  FROM users_changes
)
WHERE rn = 1;


-- 5. Enrichment: deriving email domain, marking corporates, joining country names
CREATE TEMPORARY VIEW country_dim AS
SELECT * FROM (VALUES
  ('US','United States'),
  ('IN','India'),
  ('DE','Germany'),
  ('SG','Singapore')
) AS t(country_code, country_name);

CREATE TEMPORARY VIEW users_enriched_view AS
SELECT
  u.user_id,
  u.name,
  u.email,
  SUBSTRING(u.email FROM POSITION('@' IN u.email)+1) AS email_domain,
  u.country_code,
  d.country_name,
  CASE
    WHEN SUBSTRING(u.email FROM POSITION('@' IN u.email)+1)
         IN ('contoso.com','autodesk.com','company.com')
    THEN TRUE ELSE FALSE
  END AS is_corporate,
  u.is_deleted,
  u.ts AS updated_at
FROM users_latest u
LEFT JOIN country_dim d USING (country_code);

-- Other possible enrichments:
    -- Mapping country to currency
    -- Adding timezone based on country
    -- Day of week based on timestamp
    -- Postal code extraction

    -- Data quality enrichments:
    -- Name or email NULL checks
    -- Name or email length checks
    -- Flagging if 'updated_at' is more than X days in the past, stale CDC events


-- 6. Defining sink: Postgres table with upsert
CREATE TABLE users_enriched_sink (
  user_id BIGINT,
  name STRING,
  email STRING,
  email_domain STRING,
  country_code STRING,
  country_name STRING,
  is_corporate BOOLEAN,
  is_deleted BOOLEAN,
  updated_at TIMESTAMP_LTZ(3),
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/appdb',
  'table-name' = 'users_enriched',
  'driver' = 'org.postgresql.Driver',
  'username' = 'app',
  'password' = 'app',
  'sink.max-retries' = '3',
  'sink.buffer-flush.max-rows' = '1'
);


-- 7. Pipeline: stream enriched user events into Postgres
INSERT INTO users_enriched_sink
SELECT * FROM users_enriched_view;
