# Databricks notebook source
# MAGIC %run /Workspace/Users/lakshman.tutike@outlook.com/databricks_training1/day1/includes

# COMMAND ----------

# DBTITLE 1,read
df_sales=spark.read.csv("/Volumes/laks_databricks_training/default/raw/sales.csv", header=True,inferSchema=True) 
df_sales.write.mode("overwrite").saveAsTable("laks_databricks_training.default.sales")

