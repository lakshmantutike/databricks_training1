# Databricks notebook source
# MAGIC %run /Workspace/Users/lakshman.tutike@outlook.com/day1/includes

# COMMAND ----------

# DBTITLE 1,Read
df_sales=spark.read.csv("/Volumes/laks_databricks_training/default/raw/sales.csv", header=True,inferSchema=True) 

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_cust=spark.read.json("/Volumes/laks_databricks_training/default/raw/customers.json")

# COMMAND ----------

df_cust.display()

# COMMAND ----------

# DBTITLE 1,Write
df_cust.write.saveAsTable("customers")

# COMMAND ----------

# DBTITLE 1,Write
df_sales.write.saveAsTable("laks_databricks_training.default.salessales")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sales

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}sales.csv", header=True,inferSchema=True)

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df1=add_ingestion(df_sales)

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.write.saveAsTable("sales")

# COMMAND ----------

df_order_dates=spark.read.csv(f"{input_path}order_dates.csv", hearder=True, inferSchema=True)
