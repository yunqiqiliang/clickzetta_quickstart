--LAKEHOUSE SQL
--********************************************************************--
-- author: qiliang
-- create time: 2024-12-27 16:53:50
--********************************************************************--
USE VCLUSTER SCD_VC;
USE SCHEMA SCD_SCH;
     
create table stream if not exists customer_table_changes on table customer WITH PROPERTIES('TABLE_STREAM_MODE' = 'STANDARD');