# Databricks notebook source
# MAGIC %sql
# MAGIC select * from devenv.bronze.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe devenv.bronze.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists devenv.bronze.emp_update(
# MAGIC   emp_id int,
# MAGIC   emp_name string,
# MAGIC   dept_code string,
# MAGIC   salary int,
# MAGIC   country string
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe devenv.bronze.emp_update;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into devenv.bronze.emp_update values(1001,"John","D1", 51000,"USA"),  
# MAGIC (1002,"Mary","D2", 61000,"UK"),  
# MAGIC (1003,"Mike","D1", 56000,"Australia");

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into devenv.bronze.emp_update values(1006, "Kevin","D1", 40000,"UK");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.emp_update;

# COMMAND ----------

# MAGIC %md
# MAGIC ## merge to update existing table data

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO devenv.bronze.emp e
# MAGIC USING devenv.bronze.emp_update eu
# MAGIC ON e.emp_id = eu.emp_id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC e.salary = eu.salary
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT * ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.emp;

# COMMAND ----------

# MAGIC %md
# MAGIC ## soft delete records

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into devenv.bronze.emp values(1004,"Goerge","D1", 70000,"USA"),  
# MAGIC (1005,"Peterson","D2", 71000,"Australia");
# MAGIC     
# MAGIC select * from devenv.bronze.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE devenv.bronze.emp ADD COLUMN is_active STRING;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO devenv.bronze.emp e
# MAGIC USING devenv.bronze.emp_update eu
# MAGIC ON e.emp_id = eu.emp_id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC e.salary = eu.salary,
# MAGIC e.is_active = 'Y'
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT(
# MAGIC   emp_id,
# MAGIC   emp_name,
# MAGIC   dept_code,
# MAGIC   salary,
# MAGIC   country,
# MAGIC   is_active
# MAGIC )
# MAGIC VALUES(
# MAGIC   eu.emp_id,
# MAGIC   eu.emp_name,
# MAGIC   eu.dept_code,
# MAGIC   eu.salary,
# MAGIC   eu.country,
# MAGIC   "Y"
# MAGIC )
# MAGIC WHEN NOT MATCHED BY SOURCE THEN
# MAGIC UPDATE SET e.is_active = 'N';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.emp;
