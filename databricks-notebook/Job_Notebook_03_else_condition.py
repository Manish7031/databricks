# Databricks notebook source
# MAGIC %md
# MAGIC ### conditional if- else check for day run

# COMMAND ----------

_day = dbutils.jobs.taskValues.get("01_set_day", "input_day")

print(_day)
