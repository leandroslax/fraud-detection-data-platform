CREATE EXTERNAL TABLE `fraud_lake.silver_fraud_events`(
  `raw_json` string, 
  `event_id` string, 
  `transaction_id` string, 
  `customer_id` string, 
  `card_id` string, 
  `event_time` string, 
  `ingest_time` string, 
  `amount` string, 
  `currency` string, 
  `channel` string, 
  `payment_method` string, 
  `merchant` string, 
  `merchant_id` string, 
  `mcc` string, 
  `device` string, 
  `device_os` string, 
  `device_ip` string, 
  `velocity_1h` string, 
  `distance_km_from_home` string, 
  `is_international` string, 
  `risk_score` string, 
  `approved` string, 
  `decline_reason` string, 
  `metadata` string, 
  `source` string, 
  `schema_version` string, 
  `event_timestamp` timestamp, 
  `ingest_timestamp` timestamp, 
  `amount_double` double, 
  `approved_int` int, 
  `is_international_int` int, 
  `risk_score_int` int, 
  `ds` string, 
  `event_date` date)
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
  's3://fraud-datalake-dev-use1-139961319000/silver/fraud_events/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='1c8c1579-4db0-47e6-a38f-1eb1c44eb0fa', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='crawler_silver_fraud_events', 
  'averageRecordSize'='1047', 
  'classification'='parquet', 
  'compressionType'='none', 
  'objectCount'='12', 
  'partition_filtering.enabled'='true', 
  'recordCount'='7333', 
  'sizeKey'='2918731', 
  'typeOfData'='file')
