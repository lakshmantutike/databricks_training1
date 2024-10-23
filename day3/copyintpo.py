# Databricks notebook source
# MAGIC %sql
# MAGIC create table if not exists users

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table users

# COMMAND ----------

# MAGIC %sql
# MAGIC copy into users
# MAGIC from 'dbfs:/mnt/adlssonydatabricks/raw/sample/'
# MAGIC fileformat = csv
# MAGIC FORMAT_OPTIONS('header'='true',
# MAGIC                 'mergeschma' = 'true')
# MAGIC copy_options('mergeschema'='true')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users;
