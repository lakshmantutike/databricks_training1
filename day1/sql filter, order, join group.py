# Databricks notebook source
# MAGIC %sql
# MAGIC select * from customers where customer_id=2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products order by product_name

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales, products where sales.product_id==products.product_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, count(product_id) from sales group by sales.product_id
