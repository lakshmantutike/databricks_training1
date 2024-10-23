-- Databricks notebook source
use catalog laks_training_dev;

-- COMMAND ----------

create schema if not exists bronze;

-- COMMAND ----------

use schema bronze;

-- COMMAND ----------

create table emp(id int, name string, age int)

-- COMMAND ----------

describe detail emp

-- COMMAND ----------

describe extended laks_training_dev.bronze.emp

-- COMMAND ----------

insert into emp values(1,'a',30);
insert into emp values(2,'b',40);
insert into emp values(3,'c',50),(4,'d',60)

-- COMMAND ----------



-- COMMAND ----------

#views
#std view
#temp view
#global view

-- COMMAND ----------

create view v_emp as select * from emp where id>2; 

-- COMMAND ----------

select * from v_emp;

-- COMMAND ----------

create temp view temp_emp as select * from emp where id=2

-- COMMAND ----------

select * from temp_emp;

-- COMMAND ----------

show views

-- COMMAND ----------

create global temporary view gbl_temp_emp as select * from emp where id>1;

-- COMMAND ----------

show views in global_temp;

-- COMMAND ----------

select * from global_temp.gbl_temp_emp;

-- COMMAND ----------

select * from laks_databricks_training.default.customers;

-- COMMAND ----------

describe history laks_databricks_training.default.customers

-- COMMAND ----------

delete from laks_databricks_training.default.customers where customer_id=1

-- COMMAND ----------

update laks_databricks_training.default.customers set customer_state='SD' where customer_id=5

-- COMMAND ----------

select * from laks_databricks_training.default.customers version as of 2

-- COMMAND ----------

optimize laks_databricks_training.default.customers

-- COMMAND ----------

restore table laks_databricks_training.default.customers version as of 2

-- COMMAND ----------

set spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum laks_databricks_training.default.customers retain 0 hours
