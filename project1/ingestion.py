# Databricks notebook source
# DBTITLE 1,common utilities
# MAGIC %run /Workspace/Users/lakshman.tutike@outlook.com/databricks_training1/project1/includes

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog project1;
# MAGIC use schema ecomm;

# COMMAND ----------

# DBTITLE 1,Load into Bronze Customer table
df_cust=spark.read.json("dbfs:/mnt/adlssonydatabricks/raw/project1/customers/customers.json")
df_cust.write.mode("append").saveAsTable("bronze_customers")

# COMMAND ----------

# DBTITLE 1,Load into Bronze Sales table
df_cust=spark.read.csv("dbfs:/mnt/adlssonydatabricks/raw/project1/sales/sales.csv", header=True, inferSchema=True)
df_cust.write.mode("append").saveAsTable("bronze_sales")

# COMMAND ----------

# DBTITLE 1,Load Bronze Order_dates table
df_cust=spark.read.csv("dbfs:/mnt/adlssonydatabricks/raw/project1/order_dates/order_dates.csv", header=True, inferSchema=True)
df_cust.write.mode("append").saveAsTable("bronze_order_dates")

# COMMAND ----------

df_cust=spark.read.json("dbfs:/mnt/adlssonydatabricks/raw/project1/products/products.json")
df_cust.write.mode("append").saveAsTable("bronze_products")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products;
