CREATE VIEW fraud_lake.gold_fraud_events_v AS
SELECT *
FROM
  fraud_lake.gold_fraud_events_iceberg
