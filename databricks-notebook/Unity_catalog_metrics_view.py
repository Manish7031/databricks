# Databricks notebook source
# MAGIC %md
# MAGIC ### Measures in metrics view 

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC `Order Status readable`,
# MAGIC measure(`Average Total Price`)
# MAGIC from
# MAGIC devenv.bronze.orders_raw_metric_view
# MAGIC group by `Order Status readable`;

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %sql
# MAGIC select
# MAGIC `order_year`,
# MAGIC measure(`total_orders_current_year`),
# MAGIC measure(`total_orders_last_year`)
# MAGIC from
# MAGIC devenv.bronze.orders_raw_metric_view
# MAGIC group by `order_year`
# MAGIC order by 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC `order_year`,
# MAGIC measure(`total_orders_current_year`),
# MAGIC measure(`total_orders_last_year`),
# MAGIC measure(`Yearly growth`)
# MAGIC from
# MAGIC devenv.bronze.orders_raw_metric_view
# MAGIC group by `order_year`
# MAGIC order by 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joins metrics view

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %sql
# MAGIC select
# MAGIC `order_year`,
# MAGIC `customer_market_segment`,
# MAGIC measure(`yearly_growth`)
# MAGIC from
# MAGIC devenv.bronze.orders_raw_metric_view
# MAGIC group by `order_year`, `customer_market_segment`
# MAGIC order by 1;
