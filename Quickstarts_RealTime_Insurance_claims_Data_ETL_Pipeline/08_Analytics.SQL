USE VCLUSTER RealTime_Insurance_ETL_VC;
USE SCHEMA RealTime_Insurance_ETL_SCH;
--********************************************************************--
---querying the results for staging table

SELECT * FROM staging_policy;

SELECT * FROM staging_vehicles;

SELECT * FROM staging_customers;

SELECT * FROM staging_claims;

--********************************************************************--
---querying results for table streams
SELECT * FROM STREAM_policy;

SELECT * FROM STREAM_vehicles;

SELECT * FROM STREAM_customers;

SELECT * FROM STREAM_claims;

--********************************************************************--
---querying results for final tables
SELECT * FROM policy;

SELECT * FROM vehicles;

SELECT * FROM customers;

SELECT * FROM claims;