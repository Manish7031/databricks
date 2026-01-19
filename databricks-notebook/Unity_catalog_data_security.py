# Databricks notebook source
# MAGIC %md
# MAGIC ## Row level filters in UC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.customer_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists devenv.metadata;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table devenv.metadata.mktsegments(
# MAGIC   user_email string,
# MAGIC   mktsegment string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into devenv.metadata.mktsegments values 
# MAGIC ("jackkerosy@ymail.com","BUILDING"),
# MAGIC ("KathieH@ymail.com","HOUSEHOLD");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.metadata.mktsegments;

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %sql
# MAGIC create or replace function devenv.metadata.allow_mktsegment(mktsegment string)
# MAGIC returns boolean
# MAGIC language sql
# MAGIC return
# MAGIC exists(
# MAGIC   select 1
# MAGIC   from devenv.metadata.mktsegments mk
# MAGIC   where mk.mktsegment = allow_mktsegment.mktsegment
# MAGIC   and mk.user_email = current_user()
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select devenv.metadata.allow_mktsegment("HOUSE")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.customer_raw
# MAGIC where devenv.metadata.allow_mktsegment(c_mktsegment);

# COMMAND ----------

# MAGIC %md
# MAGIC ### apply RLS in function

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table devenv.bronze.customer_raw 
# MAGIC set row filter devenv.metadata.allow_mktsegment 
# MAGIC on (c_mktsegment);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.customer_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Level masking

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.bronze.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended devenv.bronze.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table devenv.metadata.user_grp(
# MAGIC   user_email string,
# MAGIC   group string
# MAGIC );
# MAGIC insert into devenv.metadata.user_grp values 
# MAGIC ("jacob@ymail.com","admin"),
# MAGIC ("KathieH@ymail.com","user");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devenv.metadata.user_grp;

# COMMAND ----------

# DBTITLE 1,Cell 18
# MAGIC %sql
# MAGIC create or replace function devenv.metadata.mask_sal(salary int)
# MAGIC returns int
# MAGIC language sql
# MAGIC return (
# MAGIC select
# MAGIC case when group = 'user' then mask_sal.salary else NULL end as masked_salary
# MAGIC from 
# MAGIC (
# MAGIC select max(group) as group
# MAGIC from devenv.metadata.user_grp
# MAGIC where user_email = current_user()
# MAGIC )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select devenv.metadata.mask_sal(51000)

# COMMAND ----------

# MAGIC %md
# MAGIC ### apply column level masking

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table devenv.bronze.emp alter salary set mask devenv.metadata.mask_sal;
# MAGIC select * from devenv.bronze.emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table devenv.bronze.emp alter salary drop mask;

# COMMAND ----------

# MAGIC %sql
# MAGIC select is_member('admins')

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function devenv.metadata.mask_sal2(col int)
# MAGIC returns string
# MAGIC language sql
# MAGIC return (
# MAGIC   case when is_member('admins') then col else '***' end
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select devenv.metadata.mask_sal2(50000)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table devenv.bronze.emp alter salary set mask devenv.metadata.mask_sal2;
# MAGIC select * from devenv.bronze.emp;
