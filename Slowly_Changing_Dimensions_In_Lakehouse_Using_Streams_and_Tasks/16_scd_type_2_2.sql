--LAKEHOUSE SQL
--********************************************************************--
-- author: qiliang
-- create time: 2024-12-27 16:53:50
--********************************************************************--
USE VCLUSTER SCD_VC;
USE SCHEMA SCD_SCH;

merge into customer_history ch -- 目标表，将 NATION 中的变化合并到此表中
using v_customer_change_data ccd -- v_customer_change_data 是一个视图，包含插入/更新到 customer_history 表的逻辑。
   on ch.CUSTOMER_ID = ccd.CUSTOMER_ID -- CUSTOMER_ID 和 start_time 确定 customer_history 表中是否存在唯一记录
   and ch.start_time = ccd.start_time
when matched and ccd.dml_type = 'U' then update -- 表示记录已被更新且不再是当前记录，需要标记 end_time
    set ch.end_time = ccd.end_time,
        ch.is_current = FALSE
when matched and ccd.dml_type = 'D' then update -- 删除实际上是逻辑删除。记录会被标记且不会插入新版本
   set ch.end_time = ccd.end_time,
       ch.is_current = FALSE
when not matched and ccd.dml_type = 'I' then insert -- 插入一个新的 CUSTOMER_ID 或更新现有 CUSTOMER_ID 都会产生一个插入操作
          (CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, start_time, end_time, is_current)
    values (ccd.CUSTOMER_ID, ccd.FIRST_NAME, ccd.LAST_NAME, ccd.EMAIL, ccd.STREET, ccd.CITY, ccd.STATE, ccd.COUNTRY, ccd.start_time, ccd.end_time, ccd.is_current);

-- 统计 customer_history 表中的记录数
select count(*) from customer_history;

-- 查询 customer_history 表中的所有记录
select * from customer_history;


