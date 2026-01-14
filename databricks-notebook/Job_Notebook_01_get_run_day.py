# Databricks notebook source
# MAGIC %md
# MAGIC ### get run day

# COMMAND ----------

# ISO format - yyyy-MM-ddTHH:mm:ss (2026-01-13T20:00:00)
dbutils.widgets.text("input_date", "")


# COMMAND ----------

_inputdate = dbutils.widgets.get("input_date")
print(_inputdate)

# COMMAND ----------

# DBTITLE 1,Cell 4
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
_inputday = spark.sql(f"""
    select date_format(to_timestamp('{_inputdate}',"yyyy-MM-dd'T'HH:mm:ss"),'E')
""").collect()

print(_inputday[0][0])

# COMMAND ----------

dbutils.jobs.taskValues.set("input_day", _inputday[0][0])
