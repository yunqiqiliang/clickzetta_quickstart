-- Use our VCLUSTER and SCHEMA
USE VCLUSTER Three_Layer_DWH_VC;
USE SCHEMA Three_Layer_DWH_SCH;

SHOW tables;
SHOW volumes;
SHOW pipes;
SHOW table streams;

SELECT * FROM raw_customer;
SELECT * FROM raw_order;
SELECT * FROM raw_product;

SELECT * FROM raw_product;
SELECT * FROM silver_orders;
SELECT * FROM silver_product;

SELECT * FROM dt_productanalysis;
SELECT * FROM dt_region_analysis;

DESC TABLE  EXTENDED raw_customer;
DESC VOLUME volume_for_raw_customer;
DESC PIPE pipe_for_customer;
DESC TABLE STREAM customer_changes_stream;
DESC TABLE EXTENDED dt_productanalysis;