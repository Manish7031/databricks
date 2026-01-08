# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkDev").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## custom schema

# COMMAND ----------

_schema = "emp_id string, department_id string, name string, age int, gender string, salary double, hire_date string, corrupt_records string"
df = spark.read \
        .format("csv") \
        .schema(_schema) \
        .option("header", "true") \
        .load("/Volumes/workspace/default/uservolume/emp.csv")

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## create python UDF

# COMMAND ----------

#function create
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
def increment(salary):
    if salary > 50000:
        return  salary + float(salary) * 0.20
    else:
        return salary + float(salary) * 0.30

increment_udf = udf(increment)
df.withColumn("increased_salary", increment_udf("salary")).show()

