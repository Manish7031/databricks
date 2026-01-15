# Databricks notebook source
# MAGIC %md
# MAGIC ### autoloader input

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/devenv/bronze/landing/autoloaderinput/2010/12/01")
dbutils.fs.mkdirs("/Volumes/devenv/bronze/landing/autoloaderinput/2010/12/02")
dbutils.fs.mkdirs("/Volumes/devenv/bronze/landing/autoloaderinput/2010/12/03")
dbutils.fs.mkdirs("/Volumes/devenv/bronze/landing/autoloaderinput/2010/12/04")
dbutils.fs.mkdirs("/Volumes/devenv/bronze/landing/autoloaderinput/2010/12/05")
dbutils.fs.mkdirs("/Volumes/devenv/bronze/landing/autoloaderinput/2010/12/06")
dbutils.fs.mkdirs("/Volumes/devenv/bronze/landing/autoloaderinput/2010/12/07")


# COMMAND ----------

# MAGIC %md
# MAGIC ### checkpoint location

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/devenv/bronze/landing/checkpoint/autoloader")

# COMMAND ----------

dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-01.csv","/Volumes/devenv/bronze/landing/autoloaderinput/2010/12/01")
dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-02.csv","/Volumes/devenv/bronze/landing/autoloaderinput/2010/12/02")
dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-03.csv","/Volumes/devenv/bronze/landing/autoloaderinput/2010/12/03")

# COMMAND ----------

dbutils.fs.cp("/databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-06.csv","/Volumes/devenv/bronze/landing/autoloaderinput/2010/12/06")

# COMMAND ----------

# MAGIC %md
# MAGIC ### read using autoloader with check point location

# COMMAND ----------

df= (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("pathGlobFilter", "*.csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/devenv/bronze/landing/checkpoint/autoloader/1/")
    .option("cloudFiles.schemaHints", "Quantity int, UnitPrice double")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/Volumes/devenv/bronze/landing/autoloaderinput/*/")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### write in delta table using autoloader available now trigger

# COMMAND ----------

from pyspark.sql.functions import col
(df.withColumn("_file", col("_metadata.file_name"))
  .writeStream
  .option("checkpointLocation", "/Volumes/devenv/bronze/landing/checkpoint/autoloader/1/")
  .outputMode("append")
  .option("mergeSchema", True)
  .trigger(availableNow=True)
  .toTable("devenv.bronze.invoice_al_1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.invoice_al_1;

# COMMAND ----------

# DBTITLE 1,Cell 9
# MAGIC %sql
# MAGIC select _file, count(1)
# MAGIC from devenv.bronze.invoice_al_1
# MAGIC group by _file;

# COMMAND ----------

# MAGIC %md
# MAGIC ### schema evolution using autoloaders

# COMMAND ----------

df= (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("pathGlobFilter", "*.csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/Volumes/devenv/bronze/landing/checkpoint/autoloader/1/")
    .option("cloudFiles.schemaHints", "Quantity int, UnitPrice double")
    .option("cloudFiles.schemaEvolutionMode", "rescue") #schema evoultion mode-addnewcolumes,rescue,None
    .load("/Volumes/devenv/bronze/landing/autoloaderinput/*/")
)
