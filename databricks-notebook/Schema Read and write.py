# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkDev").getOrCreate()

# COMMAND ----------

_schema = "emp_id string, department_id string, name string, age int, gender string, salary double, hire_date string, corrupt_records string"
df=  spark.read.format("csv").schema(_schema).option("header", "true").load("/Volumes/workspace/default/uservolume/emp.csv")

# COMMAND ----------

df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## multiple options

# COMMAND ----------

input_options ={
    "header": True,
    "mode": "PERMISSIVE",
    "inferSchema": True
}
df_opt = spark.read.format("csv").options(**input_options).load("/Volumes/workspace/default/uservolume/emp.csv")
df_opt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## parquet read

# COMMAND ----------

df_parquet = spark.read.format("parquet").load("/Volumes/workspace/default/uservolume/parquetdata/*.parquet")
df_parquet.count()
df_parquet.show()

# COMMAND ----------

df_parquet.count()
df_parquet.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ORC read

# COMMAND ----------

df_orc = spark.read.format("orc").load("/Volumes/workspace/default/uservolume/orcdata/*.orc")
df_orc.printSchema()
df_orc.count()


# COMMAND ----------

display(df_orc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Json read

# COMMAND ----------

df_json = spark.read.format("json").option("multiline", True).load("/Volumes/workspace/default/uservolume/jsondata/emp_data.json")
df_json.printSchema()

# COMMAND ----------

df_json.show()

# COMMAND ----------

_schema = "name string, phone_number string, company struct<name:string, industry:string>"
df_json_schema = spark.read.format("json").schema(_schema).load("/Volumes/workspace/default/uservolume/jsondata/emp_data.json")
df_json_schema.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## write by parquet

# COMMAND ----------

df_parquet_w = spark.read.format("parquet").load("/Volumes/workspace/default/uservolume/parquetdata/*.parquet")
df_parquet_w.printSchema()
output_path = "/Volumes/workspace/default/uservolume/parquetdata/sales_output.parquet"
df_parquet_w.write.mode("overwrite") \
        .partitionBy("company") \
        .option("compression", "snappy") \
        .option("header", "true") \
        .parquet(output_path)

print(f"Data successfully written to {output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## write by delta

# COMMAND ----------


output_path = "/Volumes/workspace/default/uservolume/emp_output.delta"
df.write.format("delta").mode("overwrite") \
        .partitionBy("department_id") \
        .option("header", "true") \
        .save(output_path)

print(f"Data successfully written to {output_path}")
