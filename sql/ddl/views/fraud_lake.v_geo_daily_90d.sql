CREATE VIEW fraud_lake.v_geo_daily_90d AS
SELECT
  ds
, country_iso2
, SUM(events_total) events_total
, SUM(amount_sum) amount_sum
, AVG(risk_score_avg) risk_score_avg
, SUM(international_total) international_total
FROM
  fraud_lake.gold_fraud_events_geo_iceberg
WHERE ((ds >= date_add('day', -90, current_date)) AND (country_iso2 IS NOT NULL))
GROUP BY 1, 2
