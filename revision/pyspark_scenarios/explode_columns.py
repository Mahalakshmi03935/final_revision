# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode
spark = SparkSession.builder.appName("Explode_Columns").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC Explode - is used to explode or create array or map columns to rows. When an array is passed to this function, it creates a new default column “col1” and it contains all array elements. When a map is passed, it creates two new columns one for key and one for value and each element in map split into the rows.

# COMMAND ----------

data = [(1,["Praveen","Sagar"]),
        (2,["Udai","Kumar"]),
        (3,["Sanjay","Kumar"])]
columns = ["id","Name"]
df1 = spark.createDataFrame(data = data,schema = columns)
df1.show()
df_exploded_output = df1.select(col("id"),explode(col("Name")))
df_exploded_output.show()
