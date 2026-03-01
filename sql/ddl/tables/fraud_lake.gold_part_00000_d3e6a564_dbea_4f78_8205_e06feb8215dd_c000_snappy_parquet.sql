CREATE EXTERNAL TABLE `fraud_lake.gold_part_00000_d3e6a564_dbea_4f78_8205_e06feb8215dd_c000_snappy_parquet`(
  `events_count` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://fraud-datalake-dev-use1-139961319000/gold/fraud_events/part-00000-d3e6a564-dbea-4f78-8205-e06feb8215dd-c000.snappy.parquet'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='a9c8cdb5-328c-49ff-b328-fccdd337b4fc', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='crawler_gold_fraud_events', 
  'averageRecordSize'='31', 
  'classification'='parquet', 
  'compressionType'='none', 
  'recordCount'='1', 
  'sizeKey'='502', 
  'typeOfData'='file')
