# Databricks notebook source
# MAGIC %md
# MAGIC ## Sorting, Union & Aggregation

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark Aggregation").getOrCreate()

# COMMAND ----------

emp_data1 = [
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

emp_data2 = [
["123204835","Sales","Ebony","1993","Female","45277","10/31/2024"],
["121141819","Accounting","Sax","1999","Male","10907","11/27/2024"],
["081511754","Sales","Rogerio","2011","Male","26527","6/20/2025"],
["104102642","Accounting","Kylie","1996","Female","87524","7/16/2025"],
["071004161","Legal","Jorie","2004","Female","80220","12/7/2025"],
["104113217","Business Development","Edy","2011","Female","97031","6/25/2021"],
["111312739","Product Management","Ody","2002","Male","31667","12/9/2022"],
["322276949","Training","Nikolas","2000","Male","51683","8/13/2024"],
["065205484","Training","Donica","2000","Female","98140","3/9/2020"],
["072413832","Accounting","Eran","1993","Female","16113","6/11/2022"],
["322280485","Marketing","Carina","1999","Female","62958","6/17/2021"],
["053101396","Product Management","Rene","2001","Male","73665","2/21/2023"],
["071923323","Accounting","Rosabella","2012","Female","15035","12/23/2024"],
["231371663","Product Management","Dulcinea","2006","Female","53065","2/18/2025"],
["072413036","Sales","Yelena","2008","Agender","93099","11/29/2022"]
]

emp_schema = "employee_id string, department_id string ,name string ,dob string, gender string, salary string , hire_date string"


# COMMAND ----------

emp_df1 = spark.createDataFrame(data=emp_data1,schema=emp_schema)
emp_df2 = spark.createDataFrame(data=emp_data2,schema=emp_schema)
emp_df1.printSchema()
emp_df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union

# COMMAND ----------

emp_union_df = emp_df1.union(emp_df2)
emp_union_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sorting

# COMMAND ----------

from pyspark.sql.functions import desc, asc, col
df_sort = emp_union_df.orderBy(col("salary").desc())
df_sort.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation

# COMMAND ----------

from pyspark.sql.functions import count, col

df_count = df_sort.groupBy("department_id").agg(count(col("employee_id")).alias("total_dept_count"))
df_count.show()

# COMMAND ----------

from pyspark.sql.functions import count, sum, avg, desc, asc, col

df_sum = df_sort.groupBy("department_id").agg(sum(col("salary")).alias("total_dept_salary")).orderBy(col("total_dept_salary").desc())
df_sum.show()
