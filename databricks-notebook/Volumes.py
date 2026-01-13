# Databricks notebook source
# MAGIC %md
# MAGIC ## Volumes

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume devenv.bronze.volume1;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe volume devenv.bronze.volume1;

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -ltr .

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/devenv/bronze/volume1/datafiles")

# COMMAND ----------

dbutils.fs.cp("/Volumes/workspace/default/uservolume/emp.csv", "/Volumes/devenv/bronze/volume1/datafiles")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`/Volumes/devenv/bronze/volume1/datafiles/emp.csv`;

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop volume devenv.bronze.volume1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## dbutils

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# DBTITLE 1,List files in volume
display(dbutils.fs.ls("/Volumes/devenv/bronze/volume1/datafiles"))

# COMMAND ----------

dbutils.fs.head("dbfs:/Volumes/devenv/bronze/volume1/datafiles/emp.csv")

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("emp_code","10001","Employee Code")

# COMMAND ----------

dbutils.widgets.get("emp_code")

# COMMAND ----------

# MAGIC %sql
# MAGIC select ${emp_code};

# COMMAND ----------

dbutils.secrets.help()


# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

params = {
    "environment": "devenv",
    "rows_to_process": "10"
}

dbutils.notebook.run("dbfs:/Volumes/devenv/bronze/volume1/datafiles/emp.csv", 300, params)
