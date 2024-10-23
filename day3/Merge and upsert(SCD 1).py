# Databricks notebook source
data=[(1,'Sachin'),(2,'Virat')]
schema="id int, name string"
df=spark.createDataFrame(data, schema)

# COMMAND ----------

df.write.saveAsTable("laks_training_dev.bronze.cricket")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from laks_training_dev.bronze.cricket
