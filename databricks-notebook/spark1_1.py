# Databricks notebook source
# MAGIC %md
# MAGIC ## spark session & transformations

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark Intro").getOrCreate()

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
["271971735","Product Management","Morten","1991","Male","36379","7/16/2022"],
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
["072413036","Sales","Yelena","2008","Agender","93099","11/29/2022"],
["122240764","Sales","Marv","1987","Male","73424","3/23/2021"],
["042108122","Sales","Felicia","1993","Female","17640","7/9/2022"],
["107006101","Marketing","Ivan","1967","Male","67158","8/12/2020"],
["081213078","Legal","Lionello","1999","Male","26561","8/18/2023"],
["111908567","Accounting","Willi","1989","Male","13805","11/17/2022"],
["114926009","Accounting","Even","2004","Male","81500","4/29/2022"],
["011807043","Marketing","Arden","2003","Female","24953","11/17/2025"],
["243373471","Legal","Selia","1989","Female","81459","8/28/2022"],
["021110209","Product Management","Fremont","2003","Male","35589","11/29/2025"],
["122200759","Engineering","Dino","2012","Male","80065","1/2/2021"],
["112002080","Business Development","Nellie","2008","Female","90530","4/14/2023"],
["103002251","Training","Willie","1993","Female","30528","7/17/2021"],
["084306953","Business Development","Kelila","2010","Female","49083","8/23/2025"],
["086506955","Product Management","Lemar","2010","Male","97030","4/8/2024"],
["111000614","Sales","Cobb","2011","Male","36401","11/8/2023"],
["021206537","Engineering","Hilliary","2005","Female","76695","5/10/2024"],
["065202568","Engineering","Graeme","1993","Male","21119","10/25/2022"],
["111902000","Support","Hashim","2005","Genderfluid","34190","11/18/2025"],
["075902722","Sales","Neil","1999","Male","40384","3/21/2021"]
]

emp_schema = "employee_id string, department_id string ,name string ,dob string, gender string, salary string , hire_date string"

# COMMAND ----------

# MAGIC %md
# MAGIC ## create data frame

# COMMAND ----------

emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)
display(emp_df)

# COMMAND ----------

emp_df.show()

# COMMAND ----------

emp_query =  emp_df.where("salary > 30000")

# COMMAND ----------

emp_query.printSchema()


# COMMAND ----------

emp_query.schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## col & exp use with dataframe

# COMMAND ----------

from pyspark.sql.functions import col, expr

# COMMAND ----------

emp_filtered = emp_df.select(col("employee_id"), expr("name"), expr("dob"), col("hire_date"),emp_df["salary"]).where("salary > 30000").limit(10)



# COMMAND ----------

emp_filtered.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## selectExpr use with dataframe

# COMMAND ----------

emp_casted = emp_filtered.select(expr("employee_id as emp_id"), emp_filtered.name, emp_filtered.dob,emp_filtered.hire_date, expr("cast(salary as int) as salary"))

# COMMAND ----------

emp_cast_new = emp_filtered.selectExpr("employee_id as EMP_id", "name", "hire_date", "cast(dob as int) as dob", "cast(salary as int) as salary")

# COMMAND ----------

emp_cast_new.printSchema()

# COMMAND ----------

emp_cast_new.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## filter dataframe

# COMMAND ----------

emp_final = emp_cast_new.selectExpr("EMP_id as Employeeid", "name as Name", "hire_date as HireDate", "dob as dob", "salary as EmployeeSalary").where("dob >= 2000")

# COMMAND ----------

emp_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## convert string schema into spark native dataframe by importing _parse_datatype.string

# COMMAND ----------

schema_str = "name string, age int"
from pyspark.sql.types import _parse_datatype_string
schema = _parse_datatype_string(schema_str)
schema

# COMMAND ----------

emp_df.printSchema()



# COMMAND ----------

# MAGIC %md
# MAGIC ## casting using spark dataframe API

# COMMAND ----------

from pyspark.sql.functions import col, cast
emp_cast2 = emp_df.select("employee_id","department_id", "name",col("salary").cast("double"))
emp_cast2.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ## add new col with computation

# COMMAND ----------

emp_tax = emp_cast2.withColumn("Tax", col("salary") * 0.3)
emp_tax.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## add Literals

# COMMAND ----------

from pyspark.sql.functions import lit
emp_new_cols = emp_tax.withColumn("firstCol", lit("one")).withColumn("secondCol", lit("two"))
emp_new_cols.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## add multiple columns & row_count in dataframe

# COMMAND ----------

from pyspark.sql.functions import row_number, lit, col
from pyspark.sql.window import Window
cols = {
    "professional_tax" : col("salary") * 0.01,
    "healthcare_cess" : col("salary") * 0.02,
    "ThirdCol" : lit("three")
}

emp_tax_df = emp_tax.withColumns(cols).withColumn("row_count", row_number().over(Window.orderBy(lit('1'))))
emp_tax_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## monotonically_increasing_id

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
data = [("a",), ("b",), ("c",), ("d",)]
df = spark.createDataFrame(data, ["id"])
df_new_id = df.withColumn("unique_id", monotonically_increasing_id())
df_new_id.show()
