# Databricks notebook source
# MAGIC %md
# MAGIC ## Streaming Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh streaming table devenv.bronze.emp_st1
# MAGIC --schedule every 12 hours
# MAGIC as
# MAGIC select * from
# MAGIC stream read_files (
# MAGIC   "/Volumes/devenv/bronze/volume1/datafiles/",
# MAGIC   format => 'csv',
# MAGIC   includeExistingFiles = false
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialized views

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %sql
# MAGIC create or replace materialized view devenv.bronze.emp_mv
# MAGIC --schedule every 12 hour
# MAGIC as
# MAGIC select 
# MAGIC   any_value(emp_id) as emp_id,
# MAGIC   any_value(emp_name) as emp_name,
# MAGIC   any_value(dept_code) as dept_code,
# MAGIC   any_value(salary) as salary,
# MAGIC   country,
# MAGIC   any_value(is_active) as is_active
# MAGIC from devenv.bronze.emp
# MAGIC group by country;
