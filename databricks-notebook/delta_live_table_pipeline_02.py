# Databricks notebook source
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC ### create streaming table

# COMMAND ----------

@dlt.table(
    table_properties = {"quality": "bronze"},
    comment = "bronze table"
)
def order_bronze():
    df = spark.readStream.table("devenv.bronze.orders_raw")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### create materialized view

# COMMAND ----------

@dlt.table(
    table_properties = {"quality": "bronze"},
    comment = "customer bronze table",
    name = "customer_bronze"
)
def cust_bronze():
    df = spark.read.table("devenv.bronze.customer_raw")
    return df

# COMMAND ----------

### create DLT views

@dlt.view(
    comment = "Joined view"
)
def joined_vw():
    df_c = spark.read.table("LIVE.customer_bronze")
    df_o = spark.read.table("LIVE.order_bronze")
    df_join = df_o.join(
        df_c,
        how ="left_outer",
        on = df_c.c_custkey == df_o.o_custkey
    )
    
    return df_join


# COMMAND ----------

# MAGIC %md
# MAGIC ### create silver layer table

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, count
@dlt.table(
    table_properties = {"quality": "silver"},
    comment = "silver table",
    name = "joined_silver"
)
def joined_silver():
    df = spark.readStream.table("LIVE.joined_vw").withcolumn("ingesttime", current_timestamp())
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### gold table

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, count
@dlt.table(
    table_properties = {"quality": "gold"},
    comment = "orders aggregated table"
)
def orders_agg_gold():
    df = spark.readStream.table("LIVE.joined_silver")
    
    final_df = df.groupBy("c_mktsegment")
                 .agg(count(("o_orderkey")
                            .alias("sum_orders")))
                      .withcolumn("ingesttime", current_timestamp())
    return final_df
