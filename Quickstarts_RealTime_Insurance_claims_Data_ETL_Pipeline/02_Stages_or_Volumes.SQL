--创建Volume,数据湖存储文件的位置
CREATE EXTERNAL VOLUME  if not exists policy_data_stage
  LOCATION 's3://insurance-data-clickzetta-etl-project/policy' 
  USING connection aws_s3_connection_demo  -- storage Connection
  DIRECTORY = (
    enable = TRUE
  ) 
  recursive = TRUE;

--同步数据湖Volume的目录到Lakehouse
ALTER volume policy_data_stage refresh;

--查看云器Lakehouse数据湖Volume上的文件
SELECT * from directory(volume policy_data_stage);

--********************************************************************--
--创建Volume,数据湖存储文件的位置
CREATE EXTERNAL VOLUME  if not exists vehicles_data_stage
  LOCATION 's3://insurance-data-clickzetta-etl-project/vehicles' 
  USING connection aws_s3_connection_demo  -- storage Connection
  DIRECTORY = (
    enable = TRUE
  ) 
  recursive = TRUE;

--同步数据湖Volume的目录到Lakehouse
ALTER volume vehicles_data_stage refresh;

--查看云器Lakehouse数据湖Volume上的文件
SELECT * from directory(volume vehicles_data_stage);

--********************************************************************--
--创建Volume,数据湖存储文件的位置
CREATE EXTERNAL VOLUME  if not exists customers_data_stage
  LOCATION 's3://insurance-data-clickzetta-etl-project/customers' 
  USING connection aws_s3_connection_demo  -- storage Connection
  DIRECTORY = (
    enable = TRUE
  ) 
  recursive = TRUE;

--同步数据湖Volume的目录到Lakehouse
ALTER volume customers_data_stage refresh;

--查看云器Lakehouse数据湖Volume上的文件
SELECT * from directory(volume customers_data_stage);

--********************************************************************--
--创建Volume,数据湖存储文件的位置
CREATE EXTERNAL VOLUME  if not exists claims_data_stage
  LOCATION 's3://insurance-data-clickzetta-etl-project/claims' 
  USING connection aws_s3_connection_demo  -- storage Connection
  DIRECTORY = (
    enable = TRUE
  ) 
  recursive = TRUE;

--同步数据湖Volume的目录到Lakehouse
ALTER volume claims_data_stage refresh;

--查看云器Lakehouse数据湖Volume上的文件
SELECT * from directory(volume claims_data_stage);

-- DROP VOLUME IF EXISTS policy_data_stage;
-- DROP VOLUME IF EXISTS vehicles_data_stage;
-- DROP VOLUME IF EXISTS customers_data_stage;
-- DROP VOLUME IF EXISTS claims_data_stage;