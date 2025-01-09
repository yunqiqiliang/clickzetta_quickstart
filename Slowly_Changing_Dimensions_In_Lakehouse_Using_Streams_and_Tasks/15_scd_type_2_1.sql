--LAKEHOUSE SQL
--********************************************************************--
-- author: qiliang
-- create time: 2024-12-27 16:53:50
--********************************************************************--
USE VCLUSTER SCD_VC;
USE SCHEMA SCD_SCH;

-- 创建视图 v_customer_change_data
CREATE VIEW IF NOT EXISTS v_customer_change_data AS
-- 这个子查询用于处理插入到 customer 表的数据
-- 插入到 customer 表的数据会在 customer_HISTORY 表中产生一条新的插入记录
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY,
       start_time, end_time, is_current, 'I' AS dml_type
FROM (
    SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY,
           update_timestamp AS start_time,
           LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp DESC) AS end_time_raw,
           CASE WHEN end_time_raw IS NULL THEN '9999-12-31' ELSE end_time_raw END AS end_time,
           CASE WHEN end_time_raw IS NULL THEN TRUE ELSE FALSE END AS is_current
    FROM (
        SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, UPDATE_TIMESTAMP
        FROM customer_table_changes
        WHERE __change_type = 'INSERT'
    )
)
UNION
-- 这个子查询用于处理更新到 customer 表的数据
-- 更新到 customer 表的数据会在 customer_HISTORY 表中产生一条更新记录和一条插入记录
-- 下面的子查询会生成两条记录，每条记录有不同的 dml_type
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, start_time, end_time, is_current, dml_type
FROM (
    SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY,
           update_timestamp AS start_time,
           LAG(update_timestamp) OVER (PARTITION BY customer_id ORDER BY update_timestamp DESC) AS end_time_raw,
           CASE WHEN end_time_raw IS NULL THEN '9999-12-31' ELSE end_time_raw END AS end_time,
           CASE WHEN end_time_raw IS NULL THEN TRUE ELSE FALSE END AS is_current, 
           dml_type
    FROM (
        -- 识别需要插入到 customer_history 表的数据
        SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, STREET, CITY, STATE, COUNTRY, update_timestamp, 'I' AS dml_type
        FROM customer_table_changes
        WHERE __change_type = 'INSERT'
        UNION
        -- 识别 customer_HISTORY 表中需要更新的数据
        SELECT CUSTOMER_ID, null AS FIRST_NAME, null AS LAST_NAME, null AS EMAIL, null AS STREET, null AS CITY, null AS STATE, null AS COUNTRY, start_time, 'U' AS dml_type
        FROM customer_history
        WHERE customer_id IN (
            SELECT DISTINCT customer_id 
            FROM customer_table_changes
            WHERE __change_type = 'DELETE'
        )
        AND is_current = TRUE
    )
)
UNION
-- 这个子查询用于处理从 customer 表中删除的数据
-- 从 customer 表中删除的数据会在 customer_HISTORY 表中产生一条更新记录
SELECT ctc.CUSTOMER_ID, null AS FIRST_NAME, null AS LAST_NAME, null AS EMAIL, null AS STREET, null AS CITY, null AS STATE, null AS COUNTRY, ch.start_time, current_timestamp() AS end_time, NULL AS is_current, 'D' AS dml_type
FROM customer_history ch
INNER JOIN customer_table_changes ctc
ON ch.customer_id = ctc.customer_id
WHERE ctc.__change_type = 'DELETE'
AND ch.is_current = TRUE;

-- 查询 customer_table_changes 表中的前 10 条记录
SELECT * FROM customer_table_changes LIMIT 10;

-- 查询 customer_table_changes 表中不同 __change_type 的记录数量
SELECT DISTINCT(__change_type) FROM customer_table_changes LIMIT 10;

-- 查询 customer_raw 表中不同 CUSTOMER_ID 的记录数量
SELECT COUNT(DISTINCT CUSTOMER_ID) FROM customer_raw;

-- 查询 customer 表中不同 CUSTOMER_ID 的记录数量
SELECT COUNT(DISTINCT CUSTOMER_ID) FROM customer;

-- 查询 customer_table_changes 表中不同 CUSTOMER_ID 的记录数量
SELECT COUNT(DISTINCT CUSTOMER_ID) FROM customer_table_changes;

-- 查询 customer_history 表中不同 CUSTOMER_ID 的记录数量
SELECT COUNT(DISTINCT CUSTOMER_ID) FROM customer_history;

-- 查询 v_customer_change_data 视图中的前 10 条记录
SELECT * FROM v_customer_change_data LIMIT 10;


