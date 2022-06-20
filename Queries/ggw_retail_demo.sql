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
-- DLT Event Log & Data Quality Scores
---------------------------------------------------------------------------------------------------------------------------
CREATE TABLE ggw_retail_wshp.event_log
USING delta
LOCATION '/Users/glenn.wiebe@databricks.com/dlt_retail_wshp/system/events'
;

SELECT id,
    --   sequence,
       timestamp,
       level,
       event_type,
       message,
       details
  FROM ggw_retail_wshp.event_log
-- Uncomment this to leverage query widget
--  WHERE level IN ({{ level }})
;

----------------------------------------------------------------------------------------
-- Lineage
----------------------------------------------------------------------------------------
SELECT details:flow_definition.output_dataset,
       details:flow_definition.input_datasets,
       details:flow_definition.flow_type,
       details:flow_definition.schema 
    --  , details:flow_definition.explain_text,
    --   details:flow_definition
  FROM ggw_retail_wshp.event_log
 WHERE details:flow_definition IS NOT NULL
 ORDER BY timestamp

----------------------------------------------------------------------------------------
-- Pipeline Data Components
----------------------------------------------------------------------------------------
SELECT details:flow_definition.output_dataset,
       details:flow_definition.input_datasets
  FROM ggw_retail_wshp.event_log
 WHERE details:flow_definition IS NOT NULL

----------------------------------------------------------------------------------------
-- Flow Progress & Data Quality Results
----------------------------------------------------------------------------------------
SELECT 
  id,
  details:flow_progress,
  details:flow_progress.status,
  details:flow_progress.metrics.num_output_rows,
  details:flow_progress.metrics.backlog_bytes,
  details:flow_progress.data_quality.dropped_records,
  details:flow_progress:data_quality:expectations
--   explode(from_json(details:flow_progress:data_quality:expectations
--           ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations,
FROM ggw_retail_wshp.event_log
WHERE details:flow_progress.metrics IS NOT NULL
ORDER BY timestamp

----------------------------------------------------------------------------------------
-- Data Quality Expectation Metrics
----------------------------------------------------------------------------------------
SELECT id,
       expectations.dataset,
       expectations.name,
       expectations.failed_records,
       expectations.passed_records
  FROM (
        SELECT id,
               timestamp,
               details:flow_progress.metrics,
               details:flow_progress.data_quality.dropped_records,
               explode(from_json(details:flow_progress:data_quality:expectations
                        ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
          FROM ggw_retail_wshp.event_log
         WHERE details:flow_progress.metrics IS NOT NULL
        ) data_quality
;


---------------------------------------------------------------------------------------------------------------------------
-- Reset
---------------------------------------------------------------------------------------------------------------------------
DROP TABLE ggw_retail_wshp.channel;
DROP VIEW ggw_retail_wshp.channel_master;
DROP TABLE ggw_retail_wshp.customer_bronze;
DROP TABLE ggw_retail_wshp.customer_silver;
DROP TABLE ggw_retail_wshp.__apply_changes_storage_channel_master;
