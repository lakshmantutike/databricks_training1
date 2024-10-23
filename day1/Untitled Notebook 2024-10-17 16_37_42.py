# Databricks notebook source
simpleData = ((1,"James", "Sales", 3000), \
    (2,"Michael", "Sales", 4600),  \
    (3,"Robert", "Sales", 4100),   \
    (4,"Maria", "Finance", 3000),  \
    (5,"James", "Sales", 3000),    \
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100), \
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100),\
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100), \
    (None,None,None,None),\
    (None,None,None,None),\
    (None,None,None,None),\
    (None,"Robert",None,2000),\
     (11,"Jack", None, 4100), \
    (12,"Steve", "Sales", None)  
  )
 
columns= ["id","employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.display()

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df1=df.dropDuplicates(["id"])

# COMMAND ----------

df1.dropna("all",subset="department").display()

# COMMAND ----------

df2=df1.dropna("all")

# COMMAND ----------

df2.display()

# COMMAND ----------

help(df.dropna)

# COMMAND ----------

help(df.fillna)

# COMMAND ----------

df2.fillna("Finance",subset=["department"]).display()

# COMMAND ----------

df3=df2.fillna({"department":"Finance","salary":4660})

# COMMAND ----------

df3.display()

# COMMAND ----------

df.replace("Jen","Janet").display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window, types




# COMMAND ----------

w=Window.partitionBy("department").orderBy(col("salary").desc())

# COMMAND ----------

df3.withColumn("rank", dense_rank().over(Window.partitionBy("department").orderBy(col("salary").desc()))).display()
