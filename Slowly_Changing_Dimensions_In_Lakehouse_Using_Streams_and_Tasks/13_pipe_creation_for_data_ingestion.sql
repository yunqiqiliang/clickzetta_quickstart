--LAKEHOUSE SQL
--********************************************************************--
-- author: qiliang
-- create time: 2024-12-27 16:53:50
--********************************************************************--
USE VCLUSTER SCD_VC;
USE SCHEMA SCD_SCH;

create pipe volume_pipe_scd_demo
  VIRTUAL_CLUSTER = 'scd_vc'
  --执行获取最新文件使用扫描文件模式
  INGEST_MODE = 'LIST_PURGE'
  as
copy into customer_raw from volume scd_demo(customer_id string,
     first_name varchar,
     last_name varchar,
     email varchar,
     street varchar,
     city varchar,
     state varchar,
     country varchar) 
using csv OPTIONS(
  'header'='true'
)
--必须添加purge参数导入成功后删除数据 
purge=true
;

show pipes;

DESC PIPE volume_pipe_scd_demo;

-- 暂停和启动PIPE
-- 暂停
ALTER pipe volume_pipe_scd_demo SET PIPE_EXECUTION_PAUSED = true;
-- 启动
ALTER pipe volume_pipe_scd_demo SET PIPE_EXECUTION_PAUSED = false;



-- 查看pipe copy作业执行情况
SHOW JOBS IN VCLUSTER SCD_VC WHERE QUERY_TAG="pipe.ql_ws.scd_sch.volume_pipe_scd_demo" LIMIT 10;


-- 查看copy作业导入的历史文件
SELECT * FROM load_history('scd_sch.customer_raw');


