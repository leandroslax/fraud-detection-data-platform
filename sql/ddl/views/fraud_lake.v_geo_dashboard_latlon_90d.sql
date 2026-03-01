CREATE VIEW fraud_lake.v_geo_dashboard_latlon_90d AS
SELECT
  g.ds
, g.country_iso2
, g.country_name
, g.events_total
, g.transactions_total
, g.approved_total
, g.declined_total
, g.amount_sum
, g.risk_score_avg
, g.international_total
, d.lat
, d.lon
FROM
  (fraud_lake.gold_fraud_events_geo_iceberg g
LEFT JOIN fraud_lake.dim_country_latlon d ON (g.country_iso2 = d.country_iso2))
WHERE ((g.ds >= date_add('day', -90, current_date)) AND (g.country_iso2 <> 'UNK'))
