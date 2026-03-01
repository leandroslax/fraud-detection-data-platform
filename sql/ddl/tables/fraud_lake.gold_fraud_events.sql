CREATE EXTERNAL TABLE `fraud_lake.gold_fraud_events`(
  `events_total` bigint, 
  `transactions_total` bigint, 
  `distinct_cards` bigint, 
  `approved_total` bigint, 
  `declined_total` bigint, 
  `amount_avg` double, 
  `amount_min` double, 
  `amount_max` double, 
  `amount_sum` double, 
  `risk_score_avg` double, 
  `risk_score_max` int, 
  `international_total` bigint, 
  `ds` string)
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://fraud-datalake-dev-use1-139961319000/gold/fraud_events'
TBLPROPERTIES (
  'transient_lastDdlTime'='1772086663')
