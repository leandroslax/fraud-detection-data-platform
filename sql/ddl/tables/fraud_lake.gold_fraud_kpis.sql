CREATE EXTERNAL TABLE `fraud_lake.gold_fraud_kpis`(
  `event_date` date, 
  `total_transacoes` bigint, 
  `total_aprovadas` bigint, 
  `total_negadas` bigint, 
  `media_risk_score` double)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://fraud-datalake-dev-use1-139961319000/athena-results/Unsaved/2026/02/26/tables/f68d551a-6612-421d-b091-d390dd464ba3'
TBLPROPERTIES (
  'auto.purge'='false', 
  'has_encrypted_data'='false', 
  'numFiles'='-1', 
  'parquet.compression'='GZIP', 
  'totalSize'='-1', 
  'transactional'='false', 
  'trino_query_id'='20260226_055453_00025_k3xnq', 
  'trino_version'='0.215-24526-g02c3358')
