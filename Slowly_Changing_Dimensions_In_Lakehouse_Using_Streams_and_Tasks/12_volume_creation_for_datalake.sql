--LAKEHOUSE SQL
--********************************************************************--
-- author: qiliang
-- create time: 2024-12-27 16:53:50
--********************************************************************--
USE VCLUSTER SCD_VC;
USE SCHEMA SCD_SCH;

--创建Volume,数据湖存储文件的位置
CREATE EXTERNAL VOLUME  if not exists scd_demo
  LOCATION 'oss://czsampledatahz/scd_demo' 
  USING connection hz_ingestion_demo  -- storage Connection
  DIRECTORY = (
    enable = TRUE
  ) 
  recursive = TRUE;

--同步数据湖Volume的目录到Lakehouse
ALTER volume scd_demo refresh;

--查看云器Lakehouse数据湖Volume上的文件
SELECT * from directory(volume scd_demo);
  
show volumes;


