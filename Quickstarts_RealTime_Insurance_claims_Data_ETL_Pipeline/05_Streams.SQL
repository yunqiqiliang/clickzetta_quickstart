USE VCLUSTER RealTime_Insurance_ETL_VC;
USE SCHEMA RealTime_Insurance_ETL_SCH;
--********************************************************************--
--- CREATING TABLE STREAM OBJECTS FOR EACH STAGING TABLES TO CAPTURE NEW DATA
--  creating TABLE STREAM objects for staging tables
CREATE TABLE STREAM IF NOT EXISTS STREAM_policy ON TABLE staging_policy
WITH
  PROPERTIES ('TABLE_STREAM_MODE' = 'APPEND_ONLY');
--********************************************************************--
CREATE TABLE STREAM IF NOT EXISTS STREAM_vehicles ON TABLE staging_vehicles
WITH
  PROPERTIES ('TABLE_STREAM_MODE' = 'APPEND_ONLY');
--********************************************************************--
CREATE TABLE STREAM IF NOT EXISTS STREAM_customers ON TABLE staging_customers
WITH
  PROPERTIES ('TABLE_STREAM_MODE' = 'APPEND_ONLY');
--********************************************************************--
CREATE TABLE STREAM IF NOT EXISTS STREAM_claims ON TABLE staging_claims
WITH
  PROPERTIES ('TABLE_STREAM_MODE' = 'APPEND_ONLY');
--********************************************************************--
-- check total streams
SHOW TABLE STREAMS;

GRANT ALL PRIVILEGES ON `yuxing`.* TO `metabase`@`%`;