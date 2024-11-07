# Databricks notebook source
# DBTITLE 1,common utilities
# MAGIC %run /Workspace/Users/lakshman.tutike@outlook.com/databricks_training1/project1/includes

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog project1;
# MAGIC use schema ecomm;

# COMMAND ----------

# DBTITLE 1,Load into Bronze Customer table
(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .option("cloudFiles.schemaLocation","/FileStore/tables/schemalocation")
 .load("dbfs:/mnt/adlssonydatabricks/raw/project1/customers/")
    .writeStream
    .option("checkpointLocation","/FileStore/tables/checkpoint_cust")
    .trigger(availableNow=True)
    .table("bronze_customer"))

# COMMAND ----------

# DBTITLE 1,Load into Bronze Sales table
(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation","/FileStore/tables/schemalocation_sales")
 .load("dbfs:/mnt/adlssonydatabricks/raw/project1/sales/")
    .writeStream
    .option("checkpointLocation","/FileStore/tables/checkpoint_sales")
    .trigger(availableNow=True)
    .table("bronze_sales"))

# COMMAND ----------

# DBTITLE 1,Load Bronze Order_dates table
(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format","csv")
 .option("cloudFiles.schemaLocation","/FileStore/tables/schemalocation_order_dates")
 .load("dbfs:/mnt/adlssonydatabricks/raw/project1/order_dates/")
    .writeStream
    .option("checkpointLocation","/FileStore/tables/checkpoint_order_dates")
    .trigger(availableNow=True)
    .table("bronze_order_dates"))

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .option("cloudFiles.schemaLocation","/FileStore/tables/schemalocation")
 .load("dbfs:/mnt/adlssonydatabricks/raw/project1/products/")
    .writeStream
    .option("checkpointLocation","/FileStore/tables/checkpoint_products")
    .trigger(availableNow=True)
    .table("bronze_products"))
