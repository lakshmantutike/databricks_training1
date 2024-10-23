# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create or replace table laks_databricks_training.default.sales_agg 
# MAGIC as select customer_id, count(1) total_sales  from  laks_databricks_training.default.sales group by customer_id
