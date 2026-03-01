CREATE EXTERNAL TABLE `fraud_lake.analytics_fraud_kpis_daily`(
  `ds` string, 
  `events_total` bigint, 
  `transactions_total` bigint, 
  `distinct_cards` bigint, 
  `approved_total` bigint, 
  `declined_total` bigint, 
  `amount_sum` double, 
  `amount_avg` double, 
  `amount_min` double, 
  `amount_max` double, 
  `risk_score_avg` double, 
  `risk_score_max` int, 
  `international_total` bigint, 
  `decline_rate_pct` double, 
  `international_rate_pct` double)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://fraud-datalake-dev-use1-139961319000/analytics/fraud_kpis_daily/'
TBLPROPERTIES (
  'auto.purge'='false', 
  'has_encrypted_data'='false', 
  'numFiles'='-1', 
  'parquet.compression'='GZIP', 
  'totalSize'='-1', 
  'transactional'='false', 
  'trino_query_id'='20260226_013734_00277_mmuu8', 
  'trino_version'='0.215-24526-g02c3358')
