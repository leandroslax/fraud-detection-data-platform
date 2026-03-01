CREATE TABLE fraud_lake.gold_fraud_events_iceberg (
  events_total bigint,
  transactions_total bigint,
  distinct_cards bigint,
  approved_total bigint,
  declined_total bigint,
  amount_avg double,
  amount_min double,
  amount_max double,
  amount_sum double,
  risk_score_avg double,
  risk_score_max int,
  international_total bigint,
  ds date)
PARTITIONED BY (day(`ds`))
LOCATION 's3://fraud-datalake-dev-use1-139961319000/gold_iceberg/fraud_events'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='snappy',
  'format'='parquet'
);
