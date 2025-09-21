CREATE TEMPORARY VIEW country_dim AS
SELECT * FROM (VALUES
  ('US','United States'),
  ('IN','India'),
  ('DE','Germany'),
  ('SG','Singapore')
) AS t(country_code, country_name);