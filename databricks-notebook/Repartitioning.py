# Databricks notebook source
# MAGIC %md
# MAGIC ## dataframe Joins & repartition

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkDev").getOrCreate()

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

dept_data = [
["322286447","Product Management","Uppsala","Sweden","48598.11"],
["101101439","Human Resources","Lupane","Zimbabwe","84611.14"],
["051403630","Accounting","Maojia","China","43473.61"],
["083907722","Marketing","Buliok","Philippines","68697.02"],
["081000032","Marketing","Saint-Herblain","France","24408.1"],
["221278721","Support","Yanshou","China","96274.17"],
["114921622","Sales","Ribeira Quente","Portugal","28853.3"],
["061104877","Business Development","Nanhe","China","65697.36"],
["064104379","Services","Lâm Thao","Vietnam","39135.29"],
["073901877","Legal","Yamen","China","68741.33"],
["123103868","Training","Outlook","Canada","17188.19"],
["111302532","Sales","Memphis","United States","54135.55"],
["107006813","Engineering","Ilioúpoli","Greece","16269.45"],
["301171081","Training","Strasbourg","France","63321.01"],
["061103360","Business Development","Sijunjung","Indonesia","20205.15"]
]
dept_schema = "department_id string, department_name string, city string, country string, budget string"

# COMMAND ----------

df_emp = spark.createDataFrame(
    data=emp_data,
    schema=emp_schema
)
df_dept = spark.createDataFrame(
    data=dept_data,
    schema=dept_schema
)
df_emp.printSchema()
df_dept.printSchema()
df_emp.show()
df_dept.show()


# COMMAND ----------

from pyspark.sql.functions import spark_partition_id

emp_df = df_emp.withColumn("partition_num", spark_partition_id())
dept_df = df_dept.withColumn("partition_num", spark_partition_id())
emp_df.show()
dept_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join

# COMMAND ----------

df_join = emp_df.alias("e").join(dept_df.alias("d"),
                      how = "inner",
                      on=emp_df.department_id == dept_df.department_name)
df_join.select("e.name", "d.department_name", "e.salary", "d.city").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Left outer

# COMMAND ----------

df_join = emp_df.alias("e").join(dept_df.alias("d"),
                      how = "left_outer",
                      on=emp_df.department_id == dept_df.department_name)
            
df_join.select("e.name", "d.department_name", "e.salary", "d.city").show().limit(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## cascading Joins

# COMMAND ----------

df_cascade_join = emp_df.join(
    dept_df,
    on=[
        (emp_df.department_id == dept_df.department_name) &
        (
            (emp_df.department_id == "Sales") |
            (emp_df.department_id == "Business Development")
        )
    ],
    how="left_outer"
)
display(df_cascade_join)
