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