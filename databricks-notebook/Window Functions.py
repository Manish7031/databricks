# Databricks notebook source
# MAGIC %md
# MAGIC ## spark datafame transformation with window function

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark window").getOrCreate()

# COMMAND ----------

emp_data = [
["121143118","Training","Jewel","1989","Female","68209","2/20/2025"],
["101918240","Accounting","Rubi","2008","Female","54149","1/11/2024"],
["053112055","Business Development","Chucho","1997","Male","35962","2/5/2023"],
["026010605","Legal","Frannie","2010","Male","13610","8/2/2023"],
["063105544","Support","Nevsa","1993","Female","90331","6/22/2023"],
["103109840","Accounting","Lonny","2005","Male","92483","10/13/2022"],
["064202844","Human Resources","Ninetta","1986","Female","70249","10/7/2022"],
["073922458","Business Development","Etan","1995","Male","30632","12/28/2023"],
["072406768","Services","Jim","1988","Male","13597","6/14/2021"],
["028000082","Marketing","Koralle","2005","Female","18770","11/7/2020"],
["274970076","Sales","Rogers","2008","Male","20084","6/20/2021"],
["114921949","Human Resources","Marty","2004","Male","31592","2/7/2025"],
["221572838","Services","Glennis","2008","Genderqueer","29385","11/5/2021"],
["053185079","Support","Zared","2006","Male","42956","5/9/2024"],
["116312873","Business Development","Cob","1996","Male","87096","8/9/2023"],
["271971735","Product Management","Morten","1991","Male","36379","7/16/2022"]
]
emp_schema = "employee_id string, department_id string ,name string ,dob string, gender string, salary string , hire_date string"

# COMMAND ----------

emp_df = spark.createDataFrame(data=emp_data,
                               schema=emp_schema)
                    
emp_df.printSchema()
emp_df.show()

# COMMAND ----------

# get Unique data
emp_unique = emp_df.distinct()
emp_unique.show()

# COMMAND ----------

emp_dept_id = emp_df.select("department_id").distinct()
emp_dept_id.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## window function

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, lit, row_number, desc,asc

window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())
max_func = max(col("salary")).over(window_spec)

emp_max_df = emp_df.withColumn("max_salary", max_func)
emp_max_df.show()


# COMMAND ----------

#2nd highest salary
from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, lit, row_number, desc,asc

window_spec = Window.partitionBy("department_id").orderBy(col("salary").desc())

df_sal = emp_df.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") == 2)
df_sal.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## use expr

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, expr, row_number, desc,asc

emp_sal2 = emp_df.withColumn("rn", expr("row_number() over(partition by department_id order by salary desc)")).where("rn == 2")
emp_sal2.show()
