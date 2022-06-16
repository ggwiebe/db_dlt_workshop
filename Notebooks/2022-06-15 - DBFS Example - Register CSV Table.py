# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook shows how a CSV file can be the storage for a table in spark/DB SQL.  
# MAGIC You simply are registering a table (via CREATE TABLE) against a different storeage type (with the USING [type] syntax) and the unmanaged storage path.

# COMMAND ----------

# Recall a standard spark read looks like this:

# File location and type
file_location = "/FileStore/tables/channels.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can register a table against the csv file using the USING clause to set the storage type
# MAGIC 
# MAGIC -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
# MAGIC CREATE TABLE ggw_retail_sandbox.channels_csv_table 
# MAGIC  USING CSV
# MAGIC OPTIONS (path "/FileStore/tables/channels.csv", header "true", mode "FAILFAST")
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM ggw_retail_sandbox.channels_csv_table
# MAGIC ;
