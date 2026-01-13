# Databricks notebook source
dbutils.fs.head("dbfs:/databricks-datasets/online_retail/data-001/data.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## create text widget

# COMMAND ----------

dbutils.widgets.text("country", "")

# COMMAND ----------

ct_var = dbutils.widgets.get("country")


# COMMAND ----------

print(ct_var)

# COMMAND ----------

# DBTITLE 1,Cell 6
from pyspark.sql.functions import col, sum, count
emp_df = spark.read.csv("dbfs:/databricks-datasets/online_retail/data-001/data.csv", 
                        header=True)

##df_country = emp_df.groupBy('Country').agg({'Quantity': 'sum'})
##df_country.show()

# COMMAND ----------

df_filter = emp_df.where(f"upper(Country) = upper('{ct_var}')")

# COMMAND ----------

display(df_filter)

# COMMAND ----------

_cnt = df_filter.count()
if _cnt > 0:
  df_filter.write.mode("overwrite").saveAsTable(f"devenv.bronze.country_{ct_var}")
  print(f"Table 'devenv.browser.country_{ct_var}' created with {_cnt} records.")
else:
    print(f"no records save for_{ct_var}")

# COMMAND ----------

##%sql
##select * from devenv.bronze.country_united_kingdom limit 10;

# COMMAND ----------

dbutils.notebook.exit(_cnt)
