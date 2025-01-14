-- Use our VCLUSTER and SCHEMA
USE VCLUSTER Three_Layer_DWH_VC;
USE SCHEMA Three_Layer_DWH_SCH;


--创建Volume,数据湖存储文件的位置
CREATE EXTERNAL VOLUME  if not exists VOLUME_FOR_RAW_ORDER
  LOCATION 'oss://yourbucketname/VOLUME_FOR_RAW_ORDER' 
  USING connection hz_ingestion_demo  -- storage Connection
  DIRECTORY = (
    enable = TRUE
  ) 
  recursive = TRUE;

--同步数据湖Volume的目录到Lakehouse
ALTER volume VOLUME_FOR_RAW_ORDER refresh;

--查看云器Lakehouse数据湖Volume上的文件
SELECT * from directory(volume VOLUME_FOR_RAW_ORDER);