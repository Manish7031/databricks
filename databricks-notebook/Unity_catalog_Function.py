# Databricks notebook source
# MAGIC %sql
# MAGIC select * from devenv.bronze.emp;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scaler function

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function devenv.bronze.calc_tax(sal double)
# MAGIC returns double
# MAGIC language sql
# MAGIC return sal * 0.5;

# COMMAND ----------

# MAGIC %sql
# MAGIC select devenv.bronze.calc_tax(15000)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, devenv.bronze.calc_tax(salary) as tax_amount from devenv.bronze.emp;

# COMMAND ----------

from pyspark.sql.functions import expr, col, count
df = spark.read.table("devenv.bronze.emp").withColumn("tax_amount", expr("devenv.bronze.calc_tax(salary)")).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function devenv.bronze.nutrition_score(fruit string)
# MAGIC returns string
# MAGIC language python
# MAGIC as
# MAGIC $$
# MAGIC   import requests
# MAGIC   api_url = f"https://www.fruityvice.com/api/fruit/{fruit}"
# MAGIC   response_url = requests.get(api_url)
# MAGIC   data = response_url.json()
# MAGIC   return str(data.get('nutritions', 'NA'))
# MAGIC $$;

# COMMAND ----------

# MAGIC %sql
# MAGIC select devenv.bronze.nutrition_score("Apple")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table functions

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function devenv.bronze.get_empname(dept string)
# MAGIC returns table(emp_id string, emp_name string)
# MAGIC language SQL
# MAGIC return
# MAGIC (
# MAGIC   select emp_id, emp_name from devenv.bronze.emp where dept_code = dept
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.get_empname("D1")
# MAGIC
