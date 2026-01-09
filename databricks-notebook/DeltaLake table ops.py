# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName("DeltaLake") \
        .getOrCreate()

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

df_sales = spark.read.format("parquet").load("/Volumes/workspace/default/uservolume/parquetdata/sales1_data.parquet")
display(df_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## write data in hive tables

# COMMAND ----------

df_sales.write.format("parquet") \
    .mode("overwrite") \
    .option("path", "/Volumes/workspace/default/uservolume/parquetdata/updated_salesdata")
    
 

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in default

# COMMAND ----------

# MAGIC %sql
# MAGIC describe emp_final_data

# COMMAND ----------

df_sales.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("salesdelatable")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in default

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history salesdelatable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesdelatable

# COMMAND ----------

# MAGIC %sql
# MAGIC update default.salesdelatable set name ='JJennifer Welch' where email = 'lcooper@example.com'; 
# MAGIC select * from salesdelatable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesdelatable@v0 where email = 'lcooper@example.com';

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history salesdelatable

# COMMAND ----------

# MAGIC %md
# MAGIC ## delta Schema evolution

# COMMAND ----------

df_new = spark.sql("select *, current_timestamp() as time_now from default.salesdelatable where email ='lcooper@example.com'")

display(df_new)

# COMMAND ----------

df_new.write.format("delta").mode("append").option("mergeSchema", True).saveAsTable("salesdelatable")

# COMMAND ----------

# MAGIC %md
# MAGIC ## convert parquet to delta tables

# COMMAND ----------

from delta import DeltaTable
deltaTable = DeltaTable.forName(spark, "salesdelatable")
deltaTable.history()

# COMMAND ----------

deltaTable.isDeltaTable(spark, "salesdelatable/_delta_log")

# COMMAND ----------

deltaTable.convertToDelta(spark, "parquet.`/Volumes/workspace/default/uservolume/parquetdata/sales1_data.parquet`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## vaccum ops in delta tables

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

deltaTable = deltaTable.forName(spark, "salesdelatable")

deltaTable.vacuum(0)
