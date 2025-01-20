create pipe policy_pipe
  VIRTUAL_CLUSTER = 'RealTime_Insurance_ETL_VC'
  --执行获取最新文件使用扫描文件模式
  INGEST_MODE = 'LIST_PURGE'
  as
copy into staging_policy from volume policy_data_stage(
    policy_id VARCHAR(15) ,
    subscription_length FLOAT,
    region_code VARCHAR(5),
    segment VARCHAR(10))
using csv OPTIONS(
  'header'='true'
)
--必须添加purge参数导入成功后删除数据 
purge=true
;
--********************************************************************--
create pipe vehicles_pipe
  VIRTUAL_CLUSTER = 'RealTime_Insurance_ETL_VC'
  --执行获取最新文件使用扫描文件模式
  INGEST_MODE = 'LIST_PURGE'
  as
copy into staging_vehicles from volume vehicles_data_stage(
    vehicle_id VARCHAR(15) ,
    vehicle_age FLOAT,
    fuel_type VARCHAR(10),
    parking_sensors VARCHAR(5),
    parking_camera VARCHAR(5),
    rear_brakes_type VARCHAR(10),
    displacement INT,
    trasmission_type VARCHAR(20),
    steering_type VARCHAR(15),
    turning_radius FLOAT,
    gross_weight INT,
    front_fog_lights VARCHAR(5),
    rear_window_wiper VARCHAR(5), 
    rear_window_washer VARCHAR(5),
    rear_window_defogger VARCHAR(5), 
    brake_assist VARCHAR(5), 
    central_locking VARCHAR(5),
    power_steering VARCHAR(5),      
    day_night_rear_view_mirror VARCHAR(5),  
    is_speed_alert VARCHAR(5),
    ncap_rating INT)
using csv OPTIONS(
  'header'='true'
)
--必须添加purge参数导入成功后删除数据 
purge=true
;
--********************************************************************--
create pipe customers_pipe
  VIRTUAL_CLUSTER = 'RealTime_Insurance_ETL_VC'
  --执行获取最新文件使用扫描文件模式
  INGEST_MODE = 'LIST_PURGE'
  as
copy into staging_customers from volume customers_data_stage(
    customer_id VARCHAR(20),
    customer_age INT,
    region_density INT)
using csv OPTIONS(
  'header'='true'
)
--必须添加purge参数导入成功后删除数据 
purge=true
;
--********************************************************************--
create pipe claims_pipe
  VIRTUAL_CLUSTER = 'RealTime_Insurance_ETL_VC'
  --执行获取最新文件使用扫描文件模式
  INGEST_MODE = 'LIST_PURGE'
  as
copy into staging_claims from volume claims_data_stage(
    claim_id VARCHAR(20) ,
    claim_status INT)
using csv OPTIONS(
  'header'='true'
)
--必须添加purge参数导入成功后删除数据 
purge=true
;
--********************************************************************--
-- Describe pipe
DESC pipe policy_pipe;
DESC pipe vehicles_pipe;
DESC pipe customers_pipe;
DESC pipe claims_pipe;

DESC PIPE policy_pipe;

-- 暂停和启动PIPE
-- 暂停
ALTER pipe policy_pipe SET PIPE_EXECUTION_PAUSED = true;
-- 启动
ALTER pipe policy_pipe SET PIPE_EXECUTION_PAUSED = false;

-- 查看pipe copy作业执行情况
-- 七天之内的，延迟半小时
SELECT * FROM INFORMATION_SCHEMA.JOB_HISTORY WHERE QUERY_TAG="pipe.qiliang_ws.realtime_insurance_etl_sch.policy_pipe";
-- 实时的
SHOW JOBS IN VCLUSTER SCD_VC WHERE QUERY_TAG="pipe.qiliang_ws.realtime_insurance_etl_sch.policy_pipe";

SHOW JOBS where length(QUERY_TAG)>10;

-- 查看copy作业导入的历史文件
select * from load_history('RealTime_Insurance_ETL_SCH.staging_policy');
select * from load_history('RealTime_Insurance_ETL_SCH.staging_vehicles');
select * from load_history('RealTime_Insurance_ETL_SCH.staging_customers');
select * from load_history('RealTime_Insurance_ETL_SCH.staging_claims');