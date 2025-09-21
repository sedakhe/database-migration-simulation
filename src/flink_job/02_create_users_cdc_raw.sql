CREATE TABLE users_cdc_raw (
  op STRING,
  `before` ROW<user_id BIGINT, name STRING, email STRING, country_code STRING>,
  `after`  ROW<user_id BIGINT, name STRING, email STRING, country_code STRING>,
  timestamp_ms BIGINT,
  ts AS TO_TIMESTAMP_LTZ(timestamp_ms, 3),
  WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'users_cdc',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);