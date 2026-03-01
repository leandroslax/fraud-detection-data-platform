CREATE EXTERNAL TABLE `fraud_lake.gold_year_2026`(
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
  `month` string, 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://fraud-datalake-dev-use1-139961319000/gold/fraud_events/year=2026/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='a9c8cdb5-328c-49ff-b328-fccdd337b4fc', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='crawler_gold_fraud_events', 
  'averageRecordSize'='457', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='2', 
  'partition_filtering.enabled'='true', 
  'recordCount'='1', 
  'sizeKey'='3785', 
  'typeOfData'='file')
