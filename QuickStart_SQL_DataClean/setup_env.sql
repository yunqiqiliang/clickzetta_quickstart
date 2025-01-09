--LAKEHOUSE SQL
--********************************************************************--
-- author: qiliang
-- create time: 2024-12-27 16:53:50
--********************************************************************--
-- Data_Clean virtual cluster
CREATE VCLUSTER IF NOT EXISTS Data_Clean
   VCLUSTER_SIZE = XSMALL
   VCLUSTER_TYPE = GENERAL
   AUTO_SUSPEND_IN_SECOND = 60
   AUTO_RESUME = TRUE
   COMMENT  'Data_Clean VCLUSTER for test';

-- Use our VCLUSTER
USE VCLUSTER Data_Clean;

-- Create and Use SCHEMA
CREATE SCHEMA IF NOT EXISTS  Data_Clean;
USE SCHEMA Data_Clean;

DROP TABLE IF EXISTS sales_data;
-- 创建名为 "sales_data" 的示例表
CREATE TABLE sales_data (
    id INT,
    sale_date DATE,
    customer_id INT,
    product_id VARCHAR(50),
    quantity INT,
    price DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    region VARCHAR(50)
);

-- 插入20行包含脏数据的示例数据
INSERT INTO sales_data (id, sale_date, customer_id, product_id, quantity, price, total_amount, region) VALUES
(1, Date '2025-01-01', 101, '201A', 5, 100.00, 500.00, 'North'),
(2, Date '2025-01-02', 102, '202', 3, 150.00, 450.00, 'East'),
(3, Date '2025-01-03', NULL, '203', 8, 200.00, 1600.00, 'South'), -- 缺失customer_id
(4, Date '2025-01-04', 104, '204', -10, 50.00, 500.00, 'West'), -- customer_id为字符串, quantity负数
(5, Date '2025-01-05', 105, '201@#', 7, 75.00, 525.00, 'North'), -- product_id包含特殊字符
(6, Date '2025-01-06', 106, '202', 9, NULL, 1080.00, 'East'), -- 缺失price
(7, Date '2025-01-07', 107, '203', 4, 60.00, 240.00, 'South'),
(8, Date '2025-01-08', 108, '204', 6, 80.00, 480.00, ''), -- region为空
(9, Date '2025-01-09', 109, '201', 2, 110.00, 220.00, 'North'),
(10, Date '2025-01-10', 110, '202', 1, 130.00, 130.00, 'East'),
(11, Date '2025-01-11', 111, '203', 5, 140.00, 700.00, 'South'),
(12, Date '2025-01-12', 112, '204', 3, 70.00, 210.00, 'NULL'), -- region包含非法字符
(13, Date '2025-01-13', 113, '201', 8, 160.00, 1280.00, 'North'),
(14, Date '2025-01-14', 114, '202A', 6, 90.00, 540.00, 'East'), -- product_id包含特殊字符
(15, Date '2025-01-15', 115, '203', 7, 170.00, 1190.00, 'South'),
(16, Date '2025-01-16', 116, '204', 4, 180.00, 720.00, 'West'),
(17, Date '2025-01-17', 117, '201', 5, 85.00, 425.00, 'North'),
(18, Date '2025-01-18', 118, '202', 9, 190.00, 1710.00, 'East'),
(19, Date '2025-01-19', 119, '203', 2, 200.00, 400.00, 'South'),
(20, Date '2025-01-20', 120, '204', -1, 210.00, 210.00, 'West'); -- quantity负数