# Databricks notebook source
# DBTITLE 1,common utilities
# MAGIC %run /Workspace/Users/lakshman.tutike@outlook.com/databricks_training1/project1/includes

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh streaming live table customers_bronze as
# MAGIC select * from cloud_files("dbfs:/mnt/adlssonydatabricks/raw/project1/customers/","json",map("cloudFiles.inferColumnTypes","True"));

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh streaming live table sales_bronze as
# MAGIC select * from cloud_files("dbfs:/mnt/adlssonydatabricks/raw/project1/sales/","csv",map("cloudFiles.inferColumnTypes","True"));

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh streaming live table order_dates_bronze as
# MAGIC select * from cloud_files("dbfs:/mnt/adlssonydatabricks/raw/project1/order_dates/","csv",map("cloudFiles.inferColumnTypes","True"));

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh streaming live table products_bronze as
# MAGIC select * from cloud_files("dbfs:/mnt/adlssonydatabricks/raw/project1/products/","json",map("cloudFiles.inferColumnTypes","True"));

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh streaming live table sales_sliver
# MAGIC (constraint valid_order_id expect(order_id is not null) on violation drop row)
# MAGIC as 
# MAGIC select distinct(*) from stream(live.sales_bronze);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh streaming live table customers_silver
# MAGIC (constraint valid_cust_id expect(customer_id is not null) on violation drop row)
# MAGIC as 
# MAGIC select distinct(*) from stream(live.customers_bronze);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh streaming live table products_silver
# MAGIC (constraint valid_product_id expect(product_id is not null) on violation drop row)
# MAGIC as 
# MAGIC select distinct(*) from stream(live.products_bronze);

# COMMAND ----------

# MAGIC %sql
# MAGIC create streaming live table order_dates_silver
# MAGIC as 
# MAGIC select distinct(*) from stream(live.customers_silver);

# COMMAND ----------

# MAGIC %sql
# MAGIC create or refresh materialized view 
