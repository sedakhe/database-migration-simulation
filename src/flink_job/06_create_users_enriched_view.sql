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