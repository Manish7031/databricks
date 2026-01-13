# Databricks notebook source
# MAGIC %md
# MAGIC ### # call child notebook in run

# COMMAND ----------

dbutils.notebook.run(
  "Parameterized widget_notebook_1",
  300,
  {"ct_var" : "Switzerland"}
)
