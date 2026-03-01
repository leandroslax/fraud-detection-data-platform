CREATE VIEW fraud_lake.silver_fraud_events_geo_v AS
SELECT
  ds
, (CASE WHEN (is_international_int = 0) THEN 'BR' ELSE element_at(ARRAY['US','AR','GB'], (1 + (abs(from_big_endian_64(xxhash64(to_utf8(event_id)))) % 3))) END) country_iso2
, amount_double
, risk_score_int
, approved_int
, is_international_int
FROM
  fraud_lake.silver_fraud_events
