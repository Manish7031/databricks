# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkDev").getOrCreate()

# COMMAND ----------

_schema = "first_name string, last_name string, job_title string, dob string, email string, phone string, salary double, department_id string"

_empdf = spark.read.format("csv") \
    .option("header", "true") \
    .schema(_schema) \
    .load("/Volumes/workspace/default/uservolume/emp_records.csv") 
_empdf.printSchema()

# COMMAND ----------

from pyspark.sql.functions import max, avg, sum

df_avg = _empdf.groupBy("department_id").agg(avg("salary").alias("avg_salary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## performance benchmarking

# COMMAND ----------

df_avg.write.format("noop").mode("overwrite").save()

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 100)

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id
_empdf.withColumn("partition_id", spark_partition_id()).where("partition_id = 0").show()


# COMMAND ----------

output_path = "/Volumes/workspace/default/uservolume/parquetdata/record_output.parquet"
_empdf.write.mode("overwrite") \
        .partitionBy("department_id") \
        .option("compression", "snappy") \
        .option("header", "true") \
        .parquet(output_path)

print(f"Data successfully written to {output_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## cache or persist

# COMMAND ----------

_empdf.where("salary > 50000").show()

# COMMAND ----------

#cache the dataframe default - MEMORY_AND_DISK
_empdf.cache()
_empdf.where("salary > 50000").show()

# COMMAND ----------

_empdf.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## set persist storage level

# COMMAND ----------

import pyspark

df_persist = _empdf.persist(pyspark.StorageLevel.MEMORY_ONLY)

# COMMAND ----------

# MAGIC %md
# MAGIC ## clear all cache

# COMMAND ----------

spark.catalog.clearCache()
