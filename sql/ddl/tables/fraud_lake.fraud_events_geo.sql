CREATE EXTERNAL TABLE `fraud_lake.fraud_events_geo`(
  `ds` string, 
  `country_name` string, 
  `country_iso2` string, 
  `events_total` bigint, 
  `transactions_total` bigint, 
  `approved_total` bigint, 
  `declined_total` bigint, 
  `amount_sum` double, 
  `risk_score_avg` double, 
  `international_total` bigint)
PARTITIONED BY ( 
  `ds_day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://fraud-datalake-dev-use1-139961319000/gold_iceberg/fraud_events_geo/data'
TBLPROPERTIES (
  'transient_lastDdlTime'='1772361841')
