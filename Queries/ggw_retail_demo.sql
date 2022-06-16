---------------------------------------------------------------------------------------------------------------------------
-- Create a database to play around in
CREATE DATABASE ggw_retail_sandbox;
---------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------
-- Channel Reference/Master
---------------------------------------------------------------------------------------------------------------------------

-- Read the CSV File directly with DB SQL
SELECT * 
  FROM csv.`/FileStore/tables/ggw_dlt_wshp/channels.csv`
-- OPTIONS (header "true", inferSchema "true", mode "FAILFAST") -- How to make this work???
;

-- Register a table directly against the csv file
CREATE TABLE ggw_retail_sandbox.channels_csv_table 
 USING CSV
OPTIONS (path "/FileStore/tables/ggw_dlt_wshp/channels.csv", header "true", inferSchema "true", mode "FAILFAST")
;
-- DROP TABLE ggw_retail_sandbox.channels_csv_table;

-- Use this to look at ingested source (is inferred types accurate?)
SHOW CREATE TABLE ggw_retail_sandbox.channels_csv_table;


-- AFTER DLT runs, then use the workshop db:
USE ggw_retail_wshp;

-- AFTER INGEST METHODS (see wizards and notebooks)
-- Read from ingested (csv to Delta) tables 
SELECT *
  FROM ggw_retail_wshp.channel
;
-- DROP TABLE ggw_retail_wshp.channel;

SELECT *
  FROM ggw_retail_wshp.channel_master
;
-- DLT Logical CDC Tables are physically views against underlying physical CDC Tables 
-- DROP VIEW ggw_retail_wshp.channel_master;
-- DROP TABLE ggw_retail_wshp.__apply_changes_storage_channel_master


---------------------------------------------------------------------------------------------------------------------------
-- Customer Master
---------------------------------------------------------------------------------------------------------------------------

-- Read the CSV File directly with DB SQL
SELECT * 
  FROM csv.`/FileStore/tables/ggw_dlt_wshp/customer*.csv`
-- OPTIONS (header "true", inferSchema "true", mode "FAILFAST") -- How to make this work???
;

-- Register a table directly against the csv file
CREATE TABLE ggw_retail_sandbox.customer_csv_table 
 USING CSV
OPTIONS (path "/FileStore/tables/ggw_dlt_wshp/customer*.csv", header "true", inferSchema "true", mode "FAILFAST")
;
-- DROP TABLE ggw_retail_sandbox.channels_csv_table;

-- Query from this sandbox CSV Table
SELECT *
  FROM ggw_retail_sandbox.customer_csv_table
;

-- Use this to look at ingested source (is inferred types accurate?)
SHOW CREATE TABLE ggw_retail_sandbox.customer_csv_table;

-- AFTER DLT runs, then use the workshop db:
USE ggw_retail_wshp;

SELECT *
  FROM ggw_retail_wshp.customer_bronze
;



---------------------------------------------------------------------------------------------------------------------------
-- Reset
---------------------------------------------------------------------------------------------------------------------------
DROP TABLE ggw_retail_wshp.channel
DROP VIEW ggw_retail_wshp.channel_master
DROP TABLE ggw_retail_wshp.customer_bronze
DROP TABLE ggw_retail_wshp.__apply_changes_storage_channel_master






