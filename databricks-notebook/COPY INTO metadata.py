# Databricks notebook source
# MAGIC %sql
# MAGIC create volume devenv.bronze.landing
# MAGIC comment 'landing zone';

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/devenv/bronze/landing/input")

# COMMAND ----------

dbutils.fs.cp("databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-01.csv",
              "/Volumes/devenv/bronze/landing/input")

# COMMAND ----------

dbutils.fs.cp("databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-02.csv",
              "/Volumes/devenv/bronze/landing/input")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table devenv.bronze.invoice_cp;

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into devenv.bronze.invoice_cp
# MAGIC from '/Volumes/devenv/bronze/landing/input'
# MAGIC fileformat = CSV
# MAGIC pattern = '*.csv'
# MAGIC format_options (
# MAGIC   'mergeSchema' = 'true',
# MAGIC   'header' = 'true'
# MAGIC )
# MAGIC copy_options (
# MAGIC   'mergeSchema' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.invoice_cp;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended devenv.bronze.invoice_cp;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table devenv.bronze.invoice_cp_alt(
# MAGIC   InvoiceNo string,
# MAGIC   StockCode string,
# MAGIC   Quantity int,
# MAGIC   _insertdate timestamp
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into devenv.bronze.invoice_cp_alt
# MAGIC from ( 
# MAGIC   select 
# MAGIC   InvoiceNo,
# MAGIC        StockCode,
# MAGIC        cast(Quantity as int) as Quantity,
# MAGIC        current_timestamp() as _insertdate
# MAGIC        from
# MAGIC   "/Volumes/devenv/bronze/landing/input")
# MAGIC fileformat = CSV
# MAGIC pattern = '*.csv'
# MAGIC format_options (
# MAGIC   'mergeSchema' = 'true',
# MAGIC   'header' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.invoice_cp_alt;

# COMMAND ----------

dbutils.fs.cp("databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-03.csv",
              "/Volumes/devenv/bronze/landing/input")
