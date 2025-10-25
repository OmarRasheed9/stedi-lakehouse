CREATE EXTERNAL TABLE `accelerometer_landing`(
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` float COMMENT 'from deserializer', 
  `y` float COMMENT 'from deserializer', 
  `z` float COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://lakehouse-stedi-human-balance-analytics/landing/accelerometer/'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='false', 
  'classification'='json', 
  'last_modified_by'='hadoop', 
  'last_modified_time'='1761397756', 
  'numFiles'='9', 
  'numRows'='-1', 
  'rawDataSize'='-1', 
  'totalSize'='6871328')