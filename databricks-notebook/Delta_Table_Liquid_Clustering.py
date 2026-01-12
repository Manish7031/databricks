# Databricks notebook source
# MAGIC %md
# MAGIC ## Deletion Vector

# COMMAND ----------

# MAGIC %sql
# MAGIC create table devenv.bronze.sales
# MAGIC AS
# MAGIC select * from 
# MAGIC read_files('dbfs:/databricks-datasets/online_retail/data-001/data.csv', header => True, format => 'csv');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended devenv.bronze.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table devenv.bronze.sales SET tblproperties (delta.enableDeletionVectors = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from devenv.bronze.sales where InvoiceNo = '536367';

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history devenv.bronze.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from devenv.bronze.sales where InvoiceNo = '536366';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Liquid clustering

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE devenv.bronze.sales CLUSTER BY(InvoiceNo);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history devenv.bronze.sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table devenv.bronze.sales_clustering cluster by (InvoiceNo)
# MAGIC AS
# MAGIC select * from 
# MAGIC read_files('dbfs:/databricks-datasets/online_retail/data-001/data.csv', header => True, format => 'csv');

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history devenv.bronze.sales_clustering;
