--LAKEHOUSE SQL
--********************************************************************--
-- author: qiliang
-- create time: 2024-12-27 16:53:50
--********************************************************************--
drop table if exists customer_raw;
drop table if exists customer;
drop table stream if exists customer_table_changes;
drop view IF EXISTS v_customer_change_data;
