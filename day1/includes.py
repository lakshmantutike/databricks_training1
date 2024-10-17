# Databricks notebook source
from pyspark.sql.functions import *
input_path="/Volumes/laks_databricks_training/default/raw/"


# COMMAND ----------

def add_ingestion(df):
    df1=df.withColumn("ingestion_date", current_timestamp())
    return df1

# COMMAND ----------


