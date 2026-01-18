# Databricks notebook source
# MAGIC %md
# MAGIC ## Metastore Level

# COMMAND ----------

# MAGIC %sql
# MAGIC show grants on metastore;

# COMMAND ----------

# MAGIC %sql
# MAGIC grant create catalog on metastore to `control_grp`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog Level

# COMMAND ----------

# MAGIC %sql
# MAGIC --Grant user the ability to browse and use objects in a catalog
# MAGIC GRANT USE CATALOG ON CATALOG devenv TO `control_grp`;
# MAGIC GRANT BROWSE ON CATALOG devenv TO `control_grp`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant a group SELECT access to all tables in a catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON CATALOG devenv TO `control_grp`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant a user all privileges on a catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES ON CATALOG devenv TO `control_grp`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Level

# COMMAND ----------

# MAGIC %md
# MAGIC ### Allow a group to create tables in a specific schema

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT CREATE TABLE ON SCHEMA devenv.bronze TO `control_grp`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant user Modify access to all tables in a schema

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT MODIFY ON SCHEMA devenv.bronze TO `control_grp`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Level

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant user SELECT permission on a specific table

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON TABLE devenv.bronze.sales TO `control_grp`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant group MODIFY permission(update & delete data) on specific table

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT MODIFY ON TABLE devenv.bronze.sales TO `control_grp`;
# MAGIC
