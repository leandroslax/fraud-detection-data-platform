CREATE VIEW fraud_lake.v_gold_fraud_dashboard AS
SELECT
  ds
, events_total
, transactions_total
, approved_total
, declined_total
, amount_sum
, amount_avg
, risk_score_avg
, international_total
, (CAST(approved_total AS double) / transactions_total) approval_rate
, (CAST(declined_total AS double) / transactions_total) decline_rate
FROM
  fraud_lake.gold_fraud_events
