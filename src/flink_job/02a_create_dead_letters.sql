CREATE TABLE dead_letters (
  raw STRING
) WITH (
  'connector'='kafka',
  'topic'='users_cdc_dlq',
  'properties.bootstrap.servers'='kafka:9092',
  'format'='raw'
);

INSERT INTO dead_letters
SELECT TO_JSON_STRING(*) FROM users_cdc_raw
WHERE (op NOT IN ('c','u','d')) OR (timestamp_ms IS NULL);
