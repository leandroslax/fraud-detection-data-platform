CREATE VIEW fraud_lake.v_fraud_kpis_daily AS
SELECT
  ds
, events_total
, transactions_total
, distinct_cards
, approved_total
, declined_total
, amount_sum
, amount_avg
, amount_min
, amount_max
, risk_score_avg
, risk_score_max
, international_total
, ((1E2 * declined_total) / NULLIF(transactions_total, 0)) decline_rate_pct
, ((1E2 * international_total) / NULLIF(transactions_total, 0)) international_rate_pct
FROM
  fraud_lake.gold_fraud_events
