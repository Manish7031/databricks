# Databricks notebook source
import pytest
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ## create fixture

# COMMAND ----------

@pytest.fixture(scope="session")
def spark():
  spark = SparkSession \
    .builder \
    .appName("pyspark unit test") \
    .getOrCreate()
  return spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## create UDF functions

# COMMAND ----------

from pyspark.sql.functions import col, lit, regexp_replace

def trim_space(df, columnValue):
  df_new = df.withColumn(columnValue, regexp_replace(col(columnValue), "\\s+", " "))
  return df_new

def get_higher_salary(df, columnValue):
  df_filter= df.filter(col(columnValue) > 50000)
  return df_filter

def get_lower_salary(df, columnValue):
  df_filter= df.filter(col(columnValue) <= 50000)
  return df_filter

# COMMAND ----------

# MAGIC %md
# MAGIC ## test case: collect df data

# COMMAND ----------

def test_trim_space(spark):
    emp_data = [
        {"name": "Jack   1", "salary": 55000},
        {"name": "Jack   2", "salary": 60000},
        {"name": "Donald   1", "salary": 40000},
        {"name": "Donald   1", "salary": 50000}
     ]
    df_test_case = spark.createDataFrame(emp_data)
    df_transform = trim_space(df_test_case, "name")
    emp_fix_data = [
        {"name": "Jack 1", "salary": 55000},
        {"name": "Jack 2", "salary": 60000},
        {"name": "Donald 1", "salary": 40000},
        {"name": "Donald 1", "salary": 50000}
     ]
    df_expected = spark.createDataFrame(emp_fix_data)
    assert df_transform.collect() == df_expected.collect()



# COMMAND ----------

# MAGIC %md
# MAGIC ## test case : row count

# COMMAND ----------

def test_row_count(spark):
    emp_data = [
        {"name": "Jack   1", "salary": 55000},
        {"name": "Jack   2", "salary": 60000},
        {"name": "Donald   1", "salary": 40000},
        {"name": "Donald   1", "salary": 50000}
     ]
    df_test_case = spark.createDataFrame(emp_data)
    df_transform = trim_space(df_test_case, "name")
    emp_fix_data = [
        {"name": "Jack 1", "salary": 55000},
        {"name": "Jack 2", "salary": 60000},
        {"name": "Donald 1", "salary": 40000},
        {"name": "Donald 1", "salary": 50000}
     ]
    df_expected = spark.createDataFrame(emp_fix_data)
    assert df_transform.count() == df_expected.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Negative test case

# COMMAND ----------

def test_row_count(spark):
    emp_data = [
        {"name": "Jack   1", "salary": 55000},
        {"name": "Jack   2", "salary": 60000},
        {"name": "Donald   1", "salary": 40000},
        {"name": "Donald   1", "salary": 50000}
     ]
    df_test_case = spark.createDataFrame(emp_data)
    df_transform = trim_space(df_test_case, "name")
    emp_expected_data = [
        {"name": "Jack 1", "salary": 55000},
        {"name": "Jack 2", "salary": 60000}  
     ]
    df_expected = spark.createDataFrame(emp_expected_data)
    assert df_transform.count() == df_expected.count()
