-- Use our VCLUSTER and SCHEMA
USE VCLUSTER Three_Layer_DWH_VC;
USE SCHEMA Three_Layer_DWH_SCH;

CREATE PIPE IF NOT EXISTS PIPE_FOR_CUSTOMER
  VIRTUAL_CLUSTER = 'Three_Layer_DWH_VC'
  --执行获取最新文件使用扫描文件模式
  INGEST_MODE = 'LIST_PURGE'
AS
COPY INTO raw_customer FROM VOLUME VOLUME_FOR_RAW_CUSTOMER (
    customer_id INT,
    name STRING,
    email STRING,
    country STRING,
    customer_type STRING,
    registration_date STRING,
    age INT,
    gender STRING,
    total_purchases INT,
    ingestion_timestamp TIMESTAMP_NTZ
)
USING CSV OPTIONS (
  'header'='true'
)
--必须添加purge参数导入成功后删除数据 
PURGE=true
;

SHOW PIPES;
DESC PIPE PIPE_FOR_CUSTOMER;