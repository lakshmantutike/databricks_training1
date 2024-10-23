-- Databricks notebook source
create or replace streaming live table sales_bronze as
select * from cloud_files("/Volumes/laks_training_dev/default/sales","csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

create or replace streaming live table customers_bronze as
select * from cloud_files("/Volumes/laks_training_dev/default/customers","csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

create or replace streaming live table products_bronze as
select * from cloud_files("/Volumes/laks_training_dev/default/products","csv",map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

create streaming live table sales_silver
(constraint valid_order_id expect(order_id is not null) on violation drop row)
as 
select distinct(*) from STREAM(live.sales_bronze) 

-- COMMAND ----------

create streaming live table customers_silver
(constraint valid_order_id expect(order_id is not null) on violation drop row)
as 
select distinct(*) from STREAM(live.customers_bronze) 
