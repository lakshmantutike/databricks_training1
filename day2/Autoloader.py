# Databricks notebook source
print("Hello")

# COMMAND ----------

(spark
 .readStream
 .schema(schema)
 .csv("/Volumes/laks_training_dev/bronze/streaming/",header=True)
    .writeStream
    .option("checkpointLocation","/FileStore/tables/checkpoint")
    .table("laks_training_dev.bronze.stream"))

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation","/FileStore/tables/schemalocation")
 .load("/Volumes/laks_training_dev/bronze/streaming/")
    .writeStream
    .option("checkpointLocation","/FileStore/tables/checkpoint")
    .table("laks_training_dev.bronze.autoloader"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from laks_training_dev.bronze.autoloader
