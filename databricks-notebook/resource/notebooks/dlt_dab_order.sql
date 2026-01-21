-- Databricks notebook source
-- DBTITLE 1,Cell 1
--create streaming table
create or refresh streaming table devenv.bronze.st_orders
as
select * from STREAM (devenv.bronze.orders_raw);

-- COMMAND ----------

--create MV
create or replace materialized view mv_orders
as
select count(o_orderkey) as cnt_orders,
o_orderstatus
from LIVE.st_orders
group by o_orderstatus
