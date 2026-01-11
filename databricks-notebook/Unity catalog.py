# Databricks notebook source
# MAGIC %sql
# MAGIC DESCRIBE CATALOG EXTENDED devenv;

# COMMAND ----------

# MAGIC %md
# MAGIC ## create catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS uatenv
# MAGIC comment 'Created using SQL';

# COMMAND ----------

# MAGIC %sql
# MAGIC describe catalog extended uatenv;

# COMMAND ----------

# MAGIC %md
# MAGIC ## DROP catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC drop catalog uatenv cascade;

# COMMAND ----------

# MAGIC %md
# MAGIC ## create external location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION `ext-catalog`
# MAGIC URL 'adfss://data1.dfs.core.windows.net/adbcatalog' -- this is external storage location
# MAGIC WITH (STORAGE CREDENTIAL `ext-credential`);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## create catalog with external location

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog devenv_ext
# MAGIC managed location 'adfss://data1.dfs.core.windows.net/adbcatalog' -- this is external storage location
# MAGIC COMMENT 'Created using SQL';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema in Unity catalog
# MAGIC ### create schema wihtout external location in dev catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema devenv.bronze
# MAGIC comment 'this is bronze schema';

# COMMAND ----------

# MAGIC %md
# MAGIC ### create schema without external location in dev_ext external location catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema devenv_ext.bronze
# MAGIC comment 'this is schema in devenv_ext';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create external location for Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION `ext_Schema`
# MAGIC URL 'adfss://data1.dfs.core.windows.net/adbSchema' -- this is external schema location
# MAGIC WITH (STORAGE CREDENTIAL `ext-credential`);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Schema with external location in devenv_ext catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema devenv_ext.bronze_ext
# MAGIC managed location 'adfss://data1.dfs.core.windows.net/adbSchema' -- this is external storage location
# MAGIC COMMENT 'Created using SQL';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Table under dev schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS devenv.bronze.RAW_SALE(
# MAGIC Invoice STRING,
# MAGIC Cust_id STRING,
# MAGIC prod_code STRING,
# MAGIC qty INT,
# MAGIC price DECIMAL(10,2)
# MAGIC );
# MAGIC
# MAGIC insert into devenv.bronze.RAW_SALE VALUES('1001','CUST001', 'P001', 10, 100.50);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.RAW_SALE;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended devenv.bronze.raw_sale;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Undrop tables

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog devenv;
# MAGIC show tables DROPPED in devenv.bronze;
