CREATE VIEW fraud_lake.dim_country_latlon AS
SELECT *
FROM
  (
 VALUES 
     ROW ('BR', -1.4235E1, -5.19253E1)
   , ROW ('US', 3.70902E1, -9.57129E1)
   , ROW ('GB', 5.53781E1, -3.436E0)
   , ROW ('AR', -3.84161E1, -6.36167E1)
)  t (country_iso2, lat, lon)
