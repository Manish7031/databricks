# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName("DeltaLake") \
        .getOrCreate()

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/uservolume/salesdata/"))

# COMMAND ----------

# MAGIC %fs head dbfs:/Volumes/workspace/default/uservolume/salesdata/sales.csv

# COMMAND ----------

df_sales = spark.read.csv(header=True, inferSchema= True, path="/Volumes/workspace/default/uservolume/salesdata/sales.csv")

# COMMAND ----------

#write in delta tables
df_sales.repartition(10).write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("sales_delta")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_delta

# COMMAND ----------

display(dbutils.fs.ls("/workspace/default/sales_delta/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_delta where Invoice = '14-065-1598'

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(invoice), max(invoice) from sales_delta
# MAGIC group by _metadata.file_path
# MAGIC order by min(Invoice)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Z-ordering optimize

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize sales_delta zorder by(invoice)
