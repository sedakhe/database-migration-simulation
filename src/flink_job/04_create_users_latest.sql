CREATE TEMPORARY VIEW users_latest AS
SELECT * FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts DESC) AS rn
  FROM users_changes
)
WHERE rn = 1;