--LAKEHOUSE SQL
--********************************************************************--
-- author: qiliang
-- create time: 2024-12-27 16:53:50
--********************************************************************--
-- virtual cluster
CREATE VCLUSTER IF NOT EXISTS data_engineering_best_practices_vc_etl
   VCLUSTER_SIZE = XSMALL
   VCLUSTER_TYPE = GENERAL
   AUTO_SUSPEND_IN_SECOND = 10
   AUTO_RESUME = TRUE
   COMMENT  'data_engineering_best_practices_vc_etl';

CREATE VCLUSTER IF NOT EXISTS data_engineering_best_practices_vc_bi
   VCLUSTER_SIZE = XSMALL
   VCLUSTER_TYPE = ANALYTICS
   AUTO_SUSPEND_IN_SECOND = 300
   AUTO_RESUME = TRUE
   COMMENT  'data_engineering_best_practices_vc_bi';

CREATE VCLUSTER IF NOT EXISTS data_engineering_best_practices_vc_dqc
   VCLUSTER_SIZE = XSMALL
   VCLUSTER_TYPE = GENERAL
   AUTO_SUSPEND_IN_SECOND = 10
   AUTO_RESUME = TRUE
   COMMENT  'data_engineering_best_practices_vc_dqc';

-- Use our VCLUSTER
USE VCLUSTER data_engineering_best_practices_vc_etl;

-- Create and Use SCHEMA
CREATE SCHEMA IF NOT EXISTS  data_engineering_best_practices;
USE SCHEMA data_engineering_best_practices;
