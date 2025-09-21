CREATE TEMPORARY VIEW users_changes AS
SELECT
  CASE WHEN op='d' THEN `before`.user_id ELSE `after`.user_id END AS user_id,
  CASE WHEN op='d' THEN `before`.name    ELSE `after`.name    END AS name,
  CASE WHEN op='d' THEN `before`.email   ELSE `after`.email   END AS email,
  CASE WHEN op='d' THEN `before`.country_code ELSE `after`.country_code END AS country_code,
  (op = 'd') AS is_deleted,
  ts
FROM users_cdc_raw;