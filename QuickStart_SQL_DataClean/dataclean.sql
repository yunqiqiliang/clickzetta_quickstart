--LAKEHOUSE SQL
--********************************************************************--
-- author: qiliang
-- create time: 2024-12-27 16:53:50
--********************************************************************--
-- 使用 COALESCE 填充默认值
SELECT id, sale_date, COALESCE(customer_id, 0) AS customer_id, product_id, quantity, price, COALESCE(total_amount, 0) AS total_amount, region FROM sales_data;

-- 使用 IFNULL 填充默认值
SELECT id, sale_date, IFNULL(customer_id, 0) AS customer_id, product_id, quantity, price, IFNULL(total_amount, 0) AS total_amount, region FROM sales_data;

-- 使用 CASE 语句处理缺失值
SELECT id, 
       CASE 
           WHEN sale_date IS NULL THEN '2025-01-01'
           ELSE sale_date
       END AS sale_date,
       customer_id, 
       product_id, 
       quantity, 
       price, 
       total_amount, 
       region 
FROM sales_data;

-- 移除特殊字符
SELECT id, sale_date, customer_id, 
       REGEXP_REPLACE(product_id, '[^a-zA-Z0-9]', '') AS cleaned_product_id,
       quantity, 
       price, 
       total_amount, 
       region
FROM sales_data;

-- 将字符串转换为日期
SELECT id, CAST(sale_date AS DATE) AS sale_date, customer_id, product_id, quantity, CAST(price AS DECIMAL(10, 2)) AS price, CAST(total_amount AS DECIMAL(10, 2)) AS total_amount, region FROM sales_data;

-- 将字符串转换为数字
SELECT id, sale_date, customer_id, product_id, quantity, price, total_amount, region FROM sales_data;


-- 删除空白值
SELECT id, 
       TRIM(sale_date) AS sale_date, 
       customer_id, 
       product_id, 
       quantity, 
       price, 
       total_amount, 
       TRIM(region) AS region
FROM sales_data;

-- 将区域字段转换为小写
SELECT id, 
       sale_date, 
       customer_id, 
       product_id, 
       quantity, 
       price, 
       total_amount, 
       LOWER(region) AS region
FROM sales_data;

-- 删除销售金额小于50的记录
DELETE FROM sales_data WHERE total_amount < -500;


-- 使用 DISTINCT 去重
SELECT DISTINCT customer_id, product_id, region FROM sales_data;

-- 使用 ROW_NUMBER() 去重
WITH RowNumCTE AS (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY customer_id, product_id, region ORDER BY id) AS row_num
    FROM sales_data
)
SELECT id, sale_date, customer_id, product_id, quantity, price, total_amount, region
FROM RowNumCTE
WHERE row_num = 1;

-- 筛选出销售金额大于500的记录
SELECT * FROM sales_data WHERE total_amount > 500;

-- 按销售金额排序
SELECT * FROM sales_data ORDER BY total_amount DESC;

-- 合并产品ID和区域字段
SELECT id, 
       sale_date, 
       customer_id, 
       product_id || '-' || region AS combined_field, 
       quantity, 
       price, 
       total_amount
FROM sales_data;

-- 合并两个结果集
SELECT id, sale_date, customer_id, product_id, quantity, price, total_amount, region FROM sales_data where quantity<0
UNION
SELECT id, sale_date, customer_id, product_id, quantity, price, total_amount, region FROM sales_data where quantity>5000;
