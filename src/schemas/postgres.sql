-- Creating target table for enriched users
CREATE TABLE IF NOT EXISTS users_enriched (
  user_id        BIGINT PRIMARY KEY,
  name           TEXT,
  email          TEXT,
  email_domain   TEXT,                        -- derived: domain part of email
  country_code   TEXT,
  country_name   TEXT,
  is_corporate   BOOLEAN DEFAULT FALSE,       -- Flag for enrichment
  is_deleted     BOOLEAN DEFAULT FALSE,       -- Added soft-delete flag for deleted users
  updated_at     TIMESTAMPTZ NOT NULL
);

-- Function to ensure only the latest event wins (handles out-of-order CDC)
CREATE OR REPLACE FUNCTION guarded_upsert(
  p_user_id BIGINT,
  p_name TEXT,
  p_email TEXT,
  p_email_domain TEXT,
  p_country_code TEXT,
  p_country_name TEXT,
  p_is_corporate BOOLEAN,
  p_is_deleted BOOLEAN,
  p_updated_at TIMESTAMPTZ
) RETURNS VOID AS $$
BEGIN
  -- Try to insert into target table
  INSERT INTO users_enriched AS t (
    user_id, name, email, email_domain, country_code,
    country_name, is_corporate, is_deleted, updated_at
  )
  VALUES (
    p_user_id, p_name, p_email, p_email_domain, p_country_code,
    p_country_name, p_is_corporate, p_is_deleted, p_updated_at
  )
  -- If conflict on primary key (user_id), update
  ON CONFLICT (user_id) DO UPDATE
  SET
    name = EXCLUDED.name,
    email = EXCLUDED.email,
    email_domain = EXCLUDED.email_domain,
    country_code = EXCLUDED.country_code,
    country_name = EXCLUDED.country_name,
    is_corporate = EXCLUDED.is_corporate,
    is_deleted = EXCLUDED.is_deleted,
    updated_at = EXCLUDED.updated_at
  -- Only overwriting if the incoming row is newer
  WHERE EXCLUDED.updated_at >= t.updated_at;
END;
$$ LANGUAGE plpgsql;
