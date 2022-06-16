-- Databricks notebook source
-- MAGIC %md ## Delta Live Table - Retail Channels Reference Load
-- MAGIC   
-- MAGIC ![DLT Process Flow](https://raw.githubusercontent.com/ggwiebe/db-fe-dlt/main/dlt/applychanges/images/DLT_Process_Flow.png)

-- COMMAND ----------

-- MAGIC %md # Silver Reference/Master - use DLT to automatically load Channels csv file
-- MAGIC   
-- MAGIC **Common Storage Format:** Delta  
-- MAGIC **Data Types:** Cast & check Nulls

-- COMMAND ----------

-- MAGIC %md ## Step 0 - Get Table info from select/table create command  
-- MAGIC   
-- MAGIC -- This is the syntax from the CSV Table  
-- MAGIC -- We will not run this, but it serves to direct our table names (if we want) and datatypes (if we agree)
-- MAGIC 
-- MAGIC ```
-- MAGIC CREATE TABLE ggw_retail_sandbox.channels_csv_table ( 
-- MAGIC   channelId INT, 
-- MAGIC   channelName STRING, 
-- MAGIC   description STRING) 
-- MAGIC    USING CSV 
-- MAGIC  OPTIONS ( 'header' = 'true', 'inferSchema' = 'true', 'mode' = 'FAILFAST')
-- MAGIC LOCATION 'dbfs:/FileStore/tables/ggw_dlt_wshp/channels.csv'
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md ## Step 1-a - simplest query

-- COMMAND ----------

-- CREATE STREAMING LIVE TABLE channel
-- TBLPROPERTIES ("quality" = "reference")
-- COMMENT "Channel Reference dataset ingested from cloud object storage landing zone"
-- AS 
-- SELECT *
--   FROM cloud_files('/FileStore/tables/ggw_dlt_wshp/channel*.csv', 'csv', map('header', 'true', 'cloudFiles.inferColumnTypes', 'true') )
-- ;

-- COMMAND ----------

-- MAGIC %md ## Step 1-b - simple query  
-- MAGIC   
-- MAGIC Added comments and specific columns (if of interest)

-- COMMAND ----------

-- CREATE STREAMING LIVE TABLE channel
--   (
--     channelId int                 COMMENT 'ID of Sales Channel casted to int',
--     channelName string            COMMENT 'Name of Retail Sales Channel',
--     description string            COMMENT 'Description of Retail Sales Channel'
--   )
-- TBLPROPERTIES ("quality" = "reference")
-- COMMENT "Channel Reference dataset ingested from cloud object storage landing zone"
-- AS 
-- SELECT 
--     channelId,
--     channelName,
--     description
--   FROM cloud_files('/FileStore/tables/ggw_dlt_wshp/channel*.csv', 'csv', map('header', 'true', 'cloudFiles.inferColumnTypes', 'true') )
-- ;

-- COMMAND ----------

-- MAGIC %md ## Step 2 - enrich with COMMENTS & Data Lineage

-- COMMAND ----------

-- CREATE STREAMING LIVE TABLE channel
--   (
--     channelId int                 COMMENT 'ID of Sales Channel casted to int',
--     channelName string            COMMENT 'Name of Retail Sales Channel',
--     description string            COMMENT 'Description of Retail Sales Channel',
--     input_file_name string        COMMENT 'Name of file in raw storage bucket',
--     dlt_ingest_dt timestamp       COMMENT 'timestamp of dlt ingest', 
--     dlt_ingest_procedure string   COMMENT 'name of the routine used to load table',
--     dlt_ingest_principal string   COMMENT 'name of principal running load routine'
--   )
-- TBLPROPERTIES ("quality" = "reference")
-- COMMENT "Channel Reference dataset ingested from cloud object storage landing zone"
-- AS 
-- SELECT 
--     channelId,
--     channelName,
--     description,
--     input_file_name() input_file_name,
--     current_timestamp() dlt_ingest_dt,
--     "ChannelReference-Live" dlt_ingest_procedure,
--     current_user() dlt_ingest_principal
--   FROM cloud_files('/FileStore/tables/ggw_dlt_wshp/channel*.csv', 'csv', map('header', 'true', 'cloudFiles.inferColumnTypes', 'true') )
-- ;

-- COMMAND ----------

-- MAGIC %md ## Step 3 - Add basic data constraints

-- COMMAND ----------

CREATE STREAMING LIVE TABLE channel
  (
    channelId int                 COMMENT 'ID of Sales Channel casted to int',
    channelName string            COMMENT 'Name of Retail Sales Channel',
    description string            COMMENT 'Description of Retail Sales Channel',
    input_file_name string        COMMENT 'Name of file in raw storage bucket',
    dlt_ingest_dt timestamp       COMMENT 'timestamp of dlt ingest', 
    dlt_ingest_procedure string   COMMENT 'name of the routine used to load table',
    dlt_ingest_principal string   COMMENT 'name of principal running load routine',
    CONSTRAINT valid_channel_id   EXPECT (channelId IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_channel_name EXPECT (channelName IS NOT NULL) ON VIOLATION DROP ROW
  )
TBLPROPERTIES ("quality" = "reference")
COMMENT "Channel Reference dataset ingested from cloud object storage landing zone"
AS 
SELECT 
    channelId,
    channelName,
    description,
    input_file_name() input_file_name,
    current_timestamp() dlt_ingest_dt,
    "RetailReference_Live" dlt_ingest_procedure,
    current_user() dlt_ingest_principal
  FROM cloud_files('/FileStore/tables/ggw_dlt_wshp/channel*.csv', 'csv', map('header', 'true', 'cloudFiles.inferColumnTypes', 'true') )
;

-- COMMAND ----------

-- MAGIC %md ## Step 4 - Add SCD Type Tracking to a Silver Master Channel table

-- COMMAND ----------

-- SILVER - View against Bronze that will be used to load silver incrementally with APPLY CHANGES INTO
CREATE TEMPORARY STREAMING LIVE VIEW channel_master_v (
  CONSTRAINT valid_file         EXPECT (input_file_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_procedure    EXPECT (dlt_ingest_procedure IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "View of cleansed Channel (bronze tier) for loading into / mastering in Silver."
AS SELECT channelId,
          channelName,
          description,
          input_file_name,
          dlt_ingest_dt,
          dlt_ingest_procedure,
          dlt_ingest_principal
     FROM STREAM(LIVE.channel) c
;

-- COMMAND ----------

-- SILVER [CDC] - Ingest changes via APPLY CHANGES INTO syntax
CREATE STREAMING LIVE TABLE channel_master;
 APPLY CHANGES INTO LIVE.channel_master
  FROM STREAM(LIVE.channel_master_v)
  KEYS (channelId)
  SEQUENCE BY dlt_ingest_dt
  STORED AS SCD TYPE 2
;
