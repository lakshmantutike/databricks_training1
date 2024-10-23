-- Databricks notebook source
select "functions"

-- COMMAND ----------

create or replace function slaes_announcement(item_name string, item_price int)
returns string
return concat("The", item_name," is on salr for $", round(item_price * 0.2,0))

-- COMMAND ----------

select *, slaes_announcement(product_id, discount_amount)  as discount from laks_databricks_training.default.sales
