-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS devenv.etl
COMMENT 'delta table schema intro';

-- COMMAND ----------

create table if not exists devenv.bronze.orders_raw deep clone samples.tpch.orders;
create table if not exists devenv.bronze.customer_raw deep clone samples.tpch.customer;
