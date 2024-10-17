# Databricks notebook source
print("First command")

# COMMAND ----------

# MAGIC %md
# MAGIC Spark Core.
# MAGIC
# MAGIC RDD-->DataFrame

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
schema=["id", "name", "age"]
#schema="id int", "name string", "age int"
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Functions.
# MAGIC
# MAGIC

# COMMAND ----------

df.select('*').display()

# COMMAND ----------

df.select("id","name").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.withColumnRenamed("id":"emp_id","name":"emp_name","aga":"emp_age").display()

# COMMAND ----------

df.withColumn("current_date",current_date()).display()

# COMMAND ----------


