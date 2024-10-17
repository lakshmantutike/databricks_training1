# Databricks notebook source
# MAGIC %run /Workspace/Users/lakshman.tutike@outlook.com/day1/includes

# COMMAND ----------

# MAGIC %sql
# MAGIC create table customers1 as 
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/laks_databricks_training/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers1

# COMMAND ----------

# MAGIC %sql
# MAGIC create table order_dates as
# MAGIC select *, current_timestamp() as ingestion_date from csv.`/Volumes/laks_databricks_training/default/raw/order_dates.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table products as
# MAGIC select *, current_timestamp() as ingestion_date from json.`/Volumes/laks_databricks_training/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products

# COMMAND ----------

df_order_dates=spark.read.csv(f"{input_path}order_dates.csv", header=True, inferSchema=True)

# COMMAND ----------

df_order_dates.write.saveAsTable('order_dates1')

# COMMAND ----------

df_sale=spark.table("sales")

# COMMAND ----------

df_customers=spark.table("customers")

# COMMAND ----------

df_joined=df_sale.join(df_customers,df_sale["customer_id"]==df_customers["customer_id"],"inner")

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_products=spark.table("products")

# COMMAND ----------

df_sales_products=df_sale.join(df_products,df_products["product_id"]==df_sale["product_id"],"inner")

# COMMAND ----------

df_sales_products.display()

# COMMAND ----------

df_customers.filter("customer_id=2 and customer_state=='AS'").display()

# COMMAND ----------

df_customers.where("customer_id==3 and customer_state=='AS'").display()

# COMMAND ----------

df_customers.sort(col("customer_name").desc()).display()

# COMMAND ----------

df_customers.orderBy("customer_city").display()

# COMMAND ----------

df_customers.groupBy("customer_state").count().display()

# COMMAND ----------

df_sales_products.groupBy("product_name").count().orderBy("product_name").display()
