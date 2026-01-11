# Databricks notebook source
# MAGIC %sql
# MAGIC show catalogs like 'dev*';

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas in devenv;

# COMMAND ----------

spark.catalog.tableExists("devenv.bronze.raw_sale")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS devenv.bronze.emp(
# MAGIC emp_id INT,
# MAGIC emp_name STRING,
# MAGIC dept_code STRING,
# MAGIC salary INT,
# MAGIC country STRING
# MAGIC );
# MAGIC
# MAGIC insert into devenv.bronze.emp values(1001,"John","D1", 50000,"USA"),  
# MAGIC (1002,"Mary","D2", 60000,"UK"),  
# MAGIC (1003,"Mike","D1", 55000,"Australia");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary view emp_temp_view
# MAGIC as
# MAGIC select * from devenv.bronze.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC create view devenv.bronze.emp_view
# MAGIC as
# MAGIC select * from devenv.bronze.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table devenv.bronze.emp_ctas 
# MAGIC AS
# MAGIC select * from devenv.bronze.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.emp_ctas;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deep Clone

# COMMAND ----------

# MAGIC %sql
# MAGIC create table devenv.bronze.emp_dc DEEP CLONE devenv.bronze.emp;
# MAGIC     
# MAGIC select * from devenv.bronze.emp_dc

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into devenv.bronze.emp_dc values(1004,"Peter","D2", 51000,"USA"),  
# MAGIC (1005,"Sweetie","D2", 66000,"UK");

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from devenv.bronze.emp;
# MAGIC select * from devenv.bronze.emp_dc;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shallow clone - clone only metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC create table devenv.bronze.emp_shc SHALLOW CLONE devenv.bronze.emp;
# MAGIC     
# MAGIC select * from devenv.bronze.emp_shc;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history devenv.bronze.emp_shc;
