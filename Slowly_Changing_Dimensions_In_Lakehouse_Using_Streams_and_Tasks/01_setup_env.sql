--LAKEHOUSE SQL
--********************************************************************--
-- author: qiliang
-- create time: 2024-12-27 16:53:50
--********************************************************************--
-- SCD virtual cluster
CREATE VCLUSTER IF NOT EXISTS SCD_VC
   VCLUSTER_SIZE = XSMALL
   VCLUSTER_TYPE = ANALYTICS
   AUTO_SUSPEND_IN_SECOND = 60
   AUTO_RESUME = TRUE
   COMMENT  'SCD VCLUSTER for test';

-- Use our VCLUSTER
USE VCLUSTER SCD_VC;

-- Create and Use SCHEMA
CREATE SCHEMA IF NOT EXISTS  SCD_SCH;
USE SCHEMA SCD_SCH;

