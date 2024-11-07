# Databricks notebook source
# MAGIC %sql
# MAGIC create table silver_sales
# MAGIC as 
# MAGIC select distinct(*) from bronze_sales;
# MAGIC
# MAGIC create table silver_customers
# MAGIC (constraint valid_cust_id expect(customer_id is not null) on violation drop row)
# MAGIC as 
# MAGIC select distinct(*) from bronze_customers;
# MAGIC
# MAGIC create table silver_products
# MAGIC (constraint valid_product_id expect(product_id is not null) on violation drop row)
# MAGIC as 
# MAGIC select distinct(*) from bronze_products;
# MAGIC
# MAGIC create table silver_order_dates
# MAGIC as 
# MAGIC select distinct(*) from bronze_customers;
# MAGIC
# MAGIC create table if not exists silver_sales 
# MAGIC
# MAGIC (
# MAGIC   order_id int,
# MAGIC customer_id int,
# MAGIC transaction_id int,
# MAGIC product_id int,
# MAGIC quantity int,
# MAGIC discount_amount float,
# MAGIC total_amount float,
# MAGIC order_date timestamp
# MAGIC );
# MAGIC create table if not exists silver_customers (
# MAGIC   customer_city string,
# MAGIC customer_email string,
# MAGIC customer_id bigint,
# MAGIC customer_name string,
# MAGIC customer_state string
# MAGIC );
# MAGIC create table if not exists silver_products (
# MAGIC   product_category string,
# MAGIC product_id bigint,
# MAGIC product_name string,
# MAGIC product_price double
# MAGIC );
# MAGIC create table if not exists silver_order_dates (
# MAGIC   order_date timestamp,
# MAGIC day_of_week string,
# MAGIC week_of_year int,
# MAGIC month int,
# MAGIC quarter int,
# MAGIC year int
# MAGIC );
