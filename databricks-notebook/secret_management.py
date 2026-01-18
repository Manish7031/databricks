# Databricks notebook source
# MAGIC %md
# MAGIC ## secret scope management

# COMMAND ----------

emp_tbl = (
  spark.read
       .format("jdbc")
       .option("url", "dbutils.secrets.get(scope = "sqlserver", "db-url"))
       .option("dbtable", "dbo.emp")
       .option("user")
       .option("dbtable", "dbo.emp")
       .option("user", "<username>")
       .option("password", "<password>")
       .load()
)
