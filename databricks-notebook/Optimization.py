# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName("SparksharedVariables") \
        .config("spark.cores.max",16) \
        .config("spark.executor.cores",4) \
        .config("spark.executor.memory","512M") \
        .getOrCreate()

# COMMAND ----------

emp_schema = "first_name string, last_name string, job_title string, dob string, email string, phone string, salary double, city string, department_id string"
emp_df=  spark.read.format("csv") \
        .schema(emp_schema) \
        .option("header", "true") \
        .load("/Volumes/workspace/default/uservolume/employee_record.csv")
dept_schema = "department_id string, description string, city string, state string, country string"
dept_df= spark.read.format("csv") \
        .schema(dept_schema) \
        .option("header", "true") \
        .load("/Volumes/workspace/default/uservolume/dept_data.csv")

emp_df.printSchema()
dept_df.printSchema()

# COMMAND ----------

emp_df.show(10)
dept_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## broadcast variables

# COMMAND ----------

dept_name = {
  1: 'department 1',
  2: 'department 2',
  3: 'department 3',
  4: 'department 4',
  5: 'department 5',
  6: 'department 6',
  7: 'department 7',
  8: 'department 8',
  9: 'department 9',
  10: 'department 10'
}

# COMMAND ----------

brdcast_dept_name = spark.sparkContext.broadcast(dept_name)

# COMMAND ----------

df_join = emp_df.join(dept_df, 
                      emp_df.department_id == dept_df.department_id, 
                      how="left_outer")


# COMMAND ----------

df_join.write.format("noop").mode("overwrite").save()
df_join.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## broadcast join - 1 Big and 1 small braodcast df

# COMMAND ----------

from pyspark.sql.functions import broadcast
df_join2 = emp_df.join(broadcast(dept_df), 
                      emp_df.department_id == dept_df.department_id, 
                      how="left_outer")
df_join2.write.format("noop").mode("overwrite").save()
df_join2.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## JOIN Big and Big tables - sort merge

# COMMAND ----------

from pyspark.sql.functions import broadcast
df_join3 = emp_df.join(broadcast(emp_df), ## big table emp_df broadcast
                      emp_df.department_id == dept_df.department_id, 
                      how="left_outer")
df_join3.write.format("noop").mode("overwrite").save()
df_join3.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bucket
# MAGIC write emp data using bucket in table

# COMMAND ----------

table_name = "emp_bucket"
path = "/Volumes/workspace/default/uservolume/bucket/emp_bucket.parquet"

emp_df.write.format("parquet") \
      .mode("overwrite") \
      .bucketBy(4,"department_id") \
      .option("header", True) \
      .saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## skewness

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id, count, lit
df_part= df_join.withColumn("partition_num", spark_partition_id()).groupBy("partition_num").agg(count(lit(1)).alias("count"))
df_part.show()



# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "16")

# COMMAND ----------

# MAGIC %md
# MAGIC ## salting technique to fix spillage

# COMMAND ----------

import random
from pyspark.sql.functions import udf

@udf
def saltfunc_udf():
    return random.randint(0,16)

salt_df = spark.range(0,16)
salt_df.show()

# COMMAND ----------

from pyspark.sql.functions import lit, concat
salt_df = emp_df.withColumn("salted_dept_id", concat("department_id", lit("-"), saltfunc_udf()))
salt_df.select("first_name","job_title","city","department_id","salted_dept_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adaptive query Execution in spark

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adptive.coalescePartitions.enabled", True)

# fix partiion size issue to avoid Skewness
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "10MB")
spark.conf.set("spark.sql.adaptive.skewjoin.skewedPartitionThresholdInBytes", "10MB")

# COMMAND ----------

spark.conf.set("spark.sql.autobroadcastjoin.enabled", True)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
