# Databricks notebook source
# MAGIC %md
# MAGIC ## Deletion vector

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sales AS
# MAGIC SELECT * FROM read_files('dbfs:/Volumes/workspace/default/uservolume/salesdata/sales.csv', header => True, format =>'csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended sales

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sales SET TBLPROPERTIES (delta.enableDeletionVectors = TRUE)

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from sales where Invoice = '89-961-4048'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## Liquid clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sales CLUSTER BY (Invoice);
# MAGIC describe history sales

# COMMAND ----------

# MAGIC %md
# MAGIC ## TURN OFF Clustering -set - CLUSTER BY NONE;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sales CLUSTER BY NONE;
