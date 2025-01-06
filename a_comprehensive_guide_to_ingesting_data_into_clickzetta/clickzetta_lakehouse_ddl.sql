-- data ingest virtual cluster
CREATE VCLUSTER IF NOT EXISTS INGEST
   VCLUSTER_SIZE = XSMALL
   VCLUSTER_TYPE = ANALYTICS
   AUTO_SUSPEND_IN_SECOND = 60
   AUTO_RESUME = TRUE
   COMMENT  'data ingest VCLUSTER for test';
   
CREATE VCLUSTER IF NOT EXISTS INGEST_VC
   VCLUSTER_SIZE = XSMALL
   VCLUSTER_TYPE = ANALYTICS
   AUTO_SUSPEND_IN_SECOND = 60
   AUTO_RESUME = TRUE
   COMMENT  'data ingest VCLUSTER for batch/real time ingestion job';

-- Use our VCLUSTER
USE VCLUSTER INGEST;

-- Create and Use SCHEMA
CREATE SCHEMA IF NOT EXISTS  INGEST;
USE SCHEMA INGEST;

--external data lake
--创建数据湖Connection,到数据湖的连接
CREATE STORAGE CONNECTION if not exists hz_ingestion_demo
    TYPE oss
    ENDPOINT = 'oss-cn-hangzhou-internal.aliyuncs.com'
    access_id = '请输入您的access_id'
    access_key = '请输入您的access_key'
    comments = 'hangzhou oss private endpoint for ingest demo'

--创建Volume,数据湖存储文件的位置
CREATE EXTERNAL VOLUME  if not exists ingest_demo
  LOCATION 'oss://YOUR_BUCKET_NAME/YOUR_VOLUME_PATH' 
  USING connection hz_ingestion_demo  -- storage Connection
  DIRECTORY = (
    enable = TRUE
  ) 
  recursive = TRUE

--同步数据湖Volume的目录到Lakehouse
ALTER volume ingest_demo refresh;

--查看云器Lakehouse数据湖Volume上的文件
SELECT * from directory(volume ingest_demo);