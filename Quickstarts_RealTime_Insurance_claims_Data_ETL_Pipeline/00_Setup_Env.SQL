-- RealTime_Insurance_ETL_VC virtual cluster
CREATE VCLUSTER IF NOT EXISTS RealTime_Insurance_ETL_VC
   VCLUSTER_SIZE = XSMALL
   VCLUSTER_TYPE = GENERAL
   AUTO_SUSPEND_IN_SECOND = 60
   AUTO_RESUME = TRUE
   COMMENT  'RealTime_Insurance_ETL_VC VCLUSTER for test';

-- Use our VCLUSTER for data house
USE VCLUSTER RealTime_Insurance_ETL_VC;

-- Create and Use SCHEMA
CREATE SCHEMA IF NOT EXISTS  RealTime_Insurance_ETL_SCH;
USE SCHEMA RealTime_Insurance_ETL_SCH;

--external Connection for data lake
--创建数据湖Connection,到数据湖的连接
CREATE STORAGE CONNECTION if not exists aws_s3_connection_demo
    TYPE S3
    ENDPOINT = 's3.us-east-1.amazonaws.com'
    REGION = 'us-east-1'
    ACCESS_KEY = ${ACCESS_KEY}
    SECRET_KEY = ${SECRET_KEY}
    comments = 'us-east-1 storage connection for etl demo';