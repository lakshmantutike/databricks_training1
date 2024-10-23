# Databricks notebook source
# MAGIC %sql
# MAGIC #spark.readStream.csv("path")
# MAGIC #spard.writestream.option("checkpointlocation", "path").table("tablename")
# MAGIC df=spark.read.csv()
# MAGIC /Volumes/laks_training_dev/bronze/streaming

# COMMAND ----------

schema="id int, name string, Gender string, Country string, date string"

# COMMAND ----------

(spark
 .readStream
 .schema(schema)
 .csv("/Volumes/laks_training_dev/bronze/streaming/",header=True)
    .writeStream
    .option("checkpointLocation","/FileStore/tables/checkpoint")
    .table("laks_training_dev.bronze.stream"))


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from laks_training_dev.bronze.stream
