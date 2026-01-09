# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("SparkSQL") \
    .enableHiveSupport() \
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

# MAGIC %md
# MAGIC ## spark catalog - Metadata

# COMMAND ----------

# The configuration 'spark.sql.catalogImplementation' is not available in Databricks serverless clusters.
# Instead, list available catalogs to inspect metadata context.
catalogs = spark.catalog.listCatalogs()
for cat in catalogs:
    print(cat.name)



# COMMAND ----------

df_db = spark.sql("show databases")
df_db.show()

# COMMAND ----------

spark.sql("show tables in default").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## register dataframe as views

# COMMAND ----------

emp_df.createOrReplaceTempView("emp_view")
dept_df.createOrReplaceTempView("dept_view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## query table/views

# COMMAND ----------

spark.sql("""
          select * from emp_view where salary > 95000
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## convert spark sql to dataframe

# COMMAND ----------

new_df_view = spark.sql("""
          select * from emp_view
          """)
new_df_view.limit(5).show()

# COMMAND ----------

emp_temp_df = spark.sql("""
    select e.first_name, 
           e.last_name, 
           e.salary, 
           e.dob, 
           date_format(try_to_date(e.dob, 'dd/MM/yyyy'),'yyyy') as emp_year 
           from emp_view e
""")

# COMMAND ----------

emp_temp_df.createOrReplaceTempView("emp_temp_view")
spark.sql("""
          select * from emp_temp_view
          """).limit(10).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## HINT- Join using HINTS - SHUFFLE & BROADCAST MERGE

# COMMAND ----------

spark.sql("""
          select /* + SHUFFLE_MERGE(e) */
          e.*, d.department_id as dept_department_id
          from emp_view e 
          left join 
          dept_view d 
          on e.department_id = d.department_id
          """).show()

# COMMAND ----------

df_shuffle_merge = spark.sql("""
          select /* + SHUFFLE_MERGE(e) */
          e.*, d.department_id 
          from emp_view e 
          left join 
          dept_view d 
          on e.department_id = d.department_id
          """)

df_shuffle_merge.explain()

# COMMAND ----------

df_shuffle_merge = spark.sql("""
    select /* + SHUFFLE_MERGE(e) */
    e.*, d.department_id as dept_department_id
    from emp_view e
    left join dept_view d
    on e.department_id = d.department_id
""")
df_shuffle_merge.write.format("delta").saveAsTable("emp_final_data")

# COMMAND ----------

spark.sql("show tables in default").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## read data from tables

# COMMAND ----------

emp_read = spark.read.table("emp_final_data")
emp_read.limit(5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## metadata details with extended

# COMMAND ----------

spark.sql("describe extended emp_final_data").show()
