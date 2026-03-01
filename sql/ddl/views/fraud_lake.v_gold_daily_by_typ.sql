CREATE VIEW fraud_lake.v_gold_daily_by_type AS
SELECT
  year
, month
, day
, event_type
, SUM(total_transactions) total_transactions
, SUM(total_amount) total_amount
FROM
  fraud_lake.gold_fraud_events
GROUP BY 1, 2, 3, 4
